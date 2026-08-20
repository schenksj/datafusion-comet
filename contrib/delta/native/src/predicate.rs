// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Translates Catalyst-proto `Expr` to delta-kernel `Predicate` for
//! stats-based file pruning.
//!
//! Supported operators: =, !=, <, <=, >, >=, AND, OR, NOT, IS NULL,
//! IS NOT NULL, IN (including NOT IN). Cast wrappers are unwrapped
//! (kernel stats don't need type coercion). Anything else becomes
//! `Predicate::unknown()`, which disables data skipping for that
//! subtree but is never incorrect.

use datafusion_comet_proto::spark_expression::{self, expr::ExprStruct, literal, Expr};
use delta_kernel::expressions::{ArrayData, BinaryPredicateOp, Expression, Predicate, Scalar};
use delta_kernel::schema::{ArrayType, DataType};

/// Translate with column name resolution for BoundReferences.
pub fn catalyst_to_kernel_predicate_with_names(expr: &Expr, column_names: &[String]) -> Predicate {
    translate_predicate(expr, column_names)
}

/// Try to translate a Catalyst-proto `Expr` into a kernel `Predicate`
/// (without column name resolution — BoundReferences become Unknown).
pub fn catalyst_to_kernel_predicate(expr: &Expr) -> Predicate {
    translate_predicate(expr, &[])
}

fn translate_predicate(expr: &Expr, names: &[String]) -> Predicate {
    let to_expr = |e: &Expr| catalyst_to_kernel_expression_with_names(e, names);
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::IsNull(unary)) => match unary.child.as_deref() {
            Some(child) => Predicate::is_null(to_expr(child)),
            None => Predicate::unknown("missing_child"),
        },
        Some(ExprStruct::IsNotNull(unary)) => match unary.child.as_deref() {
            Some(child) => Predicate::is_not_null(to_expr(child)),
            None => Predicate::unknown("missing_child"),
        },
        Some(ExprStruct::Eq(binary)) => binary_pred_n(
            Predicate::eq,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Neq(binary)) => binary_pred_n(
            Predicate::ne,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Lt(binary)) => binary_pred_n(
            Predicate::lt,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::LtEq(binary)) => binary_pred_n(
            Predicate::le,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Gt(binary)) => binary_pred_n(
            Predicate::gt,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::GtEq(binary)) => binary_pred_n(
            Predicate::ge,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::And(binary)) => match (binary.left.as_deref(), binary.right.as_deref()) {
            (Some(l), Some(r)) => {
                Predicate::and(translate_predicate(l, names), translate_predicate(r, names))
            }
            _ => Predicate::unknown("and_missing_child"),
        },
        Some(ExprStruct::Or(binary)) => match (binary.left.as_deref(), binary.right.as_deref()) {
            (Some(l), Some(r)) => {
                Predicate::or(translate_predicate(l, names), translate_predicate(r, names))
            }
            _ => Predicate::unknown("or_missing_child"),
        },
        Some(ExprStruct::Not(unary)) => match unary.child.as_deref() {
            Some(child) => Predicate::not(translate_predicate(child, names)),
            None => Predicate::unknown("not_missing_child"),
        },
        Some(ExprStruct::In(in_expr)) => translate_in(in_expr, names),
        // Cast subtrees become unknown rather than unwrapping to the child: after
        // unwrapping, the literal on the other side carries the cast's TARGET type,
        // which by construction differs from the column's stats type, and kernel's
        // stats evaluator hard-errors on mismatched comparisons ("Invalid comparison:
        // Int64 <= Int32") -- aborting the scan and forcing the predicate-less retry,
        // which drops pruning for EVERY conjunct. Localizing the cast conjunct to
        // unknown keeps the other conjuncts pruning. (Spark's numeric-promotion casts
        // are always widening, so an unwrap essentially never type-checks anyway.)
        Some(ExprStruct::Cast(_)) => Predicate::unknown("cast_not_pushed"),
        _ => Predicate::unknown("unsupported_catalyst_expr"),
    }
}

fn translate_in(in_expr: &spark_expression::In, names: &[String]) -> Predicate {
    let value = match in_expr.in_value.as_deref() {
        Some(v) => catalyst_to_kernel_expression_with_names(v, names),
        None => return Predicate::unknown("in_missing_value"),
    };

    let scalars: Vec<Scalar> = in_expr
        .lists
        .iter()
        .filter_map(catalyst_literal_to_scalar)
        .collect();

    // Every element must translate, or the whole IN becomes unknown. Dropping an
    // untranslatable element (decimal, binary, NULL, non-literal) would produce a
    // STRICTLY STRONGER predicate -- `col IN (1, <decimal 2.5>)` must not prune a file
    // whose only matches carry 2.5. (Kernel 0.24 doesn't evaluate In for skipping yet,
    // so this is a latent bug, but its `eval_pred_in` is an explicit TODO -- the first
    // kernel upgrade that implements it would silently drop rows.)
    if scalars.len() != in_expr.lists.len() {
        return Predicate::unknown("in_unsupported_element");
    }
    if scalars.is_empty() {
        return Predicate::unknown("in_no_literal_values");
    }

    let kernel_type = scalar_to_kernel_type(&scalars[0]);
    let array_data = match ArrayData::try_new(ArrayType::new(kernel_type, true), scalars) {
        Ok(ad) => ad,
        Err(_) => return Predicate::unknown("in_array_type_mismatch"),
    };
    let array = Expression::literal(Scalar::Array(array_data));

    let pred = Predicate::binary(BinaryPredicateOp::In, value, array);
    if in_expr.negated {
        Predicate::not(pred)
    } else {
        pred
    }
}

fn scalar_to_kernel_type(s: &Scalar) -> DataType {
    match s {
        Scalar::Boolean(_) => DataType::BOOLEAN,
        Scalar::Byte(_) => DataType::BYTE,
        Scalar::Short(_) => DataType::SHORT,
        Scalar::Integer(_) => DataType::INTEGER,
        Scalar::Long(_) => DataType::LONG,
        Scalar::Float(_) => DataType::FLOAT,
        Scalar::Double(_) => DataType::DOUBLE,
        Scalar::String(_) => DataType::STRING,
        Scalar::Timestamp(_) => DataType::TIMESTAMP,
        Scalar::TimestampNtz(_) => DataType::TIMESTAMP_NTZ,
        Scalar::Date(_) => DataType::DATE,
        _ => DataType::STRING,
    }
}

fn catalyst_literal_to_scalar(expr: &Expr) -> Option<Scalar> {
    use spark_expression::data_type::DataTypeId;
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::Literal(lit)) => {
            // Spark stores TIMESTAMP / TIMESTAMP_NTZ as int64 micros and DATE as int32 days, all in
            // the int value slots. Honor the declared `datatype` so the kernel scalar carries the
            // right TYPE: a bare `Scalar::Long`/`Scalar::Integer` makes kernel reject the comparison
            // against a temporal column ("Invalid comparison: Timestamp < Int64") and abort the scan.
            let type_id = lit.datatype.as_ref().map(|d| d.type_id);
            match &lit.value {
                Some(literal::Value::BoolVal(b)) => Some(Scalar::Boolean(*b)),
                Some(literal::Value::ByteVal(v)) => Some(Scalar::Byte(*v as i8)),
                Some(literal::Value::ShortVal(v)) => Some(Scalar::Short(*v as i16)),
                Some(literal::Value::IntVal(v)) if type_id == Some(DataTypeId::Date as i32) => {
                    Some(Scalar::Date(*v))
                }
                Some(literal::Value::IntVal(v)) => Some(Scalar::Integer(*v)),
                Some(literal::Value::LongVal(v))
                    if type_id == Some(DataTypeId::Timestamp as i32) =>
                {
                    Some(Scalar::Timestamp(*v))
                }
                Some(literal::Value::LongVal(v))
                    if type_id == Some(DataTypeId::TimestampNtz as i32) =>
                {
                    Some(Scalar::TimestampNtz(*v))
                }
                Some(literal::Value::LongVal(v)) => Some(Scalar::Long(*v)),
                Some(literal::Value::FloatVal(v)) => Some(Scalar::Float(*v)),
                Some(literal::Value::DoubleVal(v)) => Some(Scalar::Double(*v)),
                Some(literal::Value::StringVal(s)) => Some(Scalar::String(s.clone())),
                _ => None,
            }
        }
        _ => None,
    }
}

fn binary_pred_n(
    builder: impl Fn(Expression, Expression) -> Predicate,
    left: Option<&Expr>,
    right: Option<&Expr>,
    names: &[String],
) -> Predicate {
    match (left, right) {
        (Some(l), Some(r)) => builder(
            catalyst_to_kernel_expression_with_names(l, names),
            catalyst_to_kernel_expression_with_names(r, names),
        ),
        _ => Predicate::unknown("binary_missing_child"),
    }
}

/// Translate a Catalyst-proto `Expr` into a kernel value `Expression`.
///
/// `column_names` maps BoundReference indices to column names. When
/// empty, BoundReferences become unknown expressions (disabling file
/// skipping for that sub-expression but never producing wrong results).
pub fn catalyst_to_kernel_expression_with_names(
    expr: &Expr,
    column_names: &[String],
) -> Expression {
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::Bound(bound)) => {
            let idx = bound.index as usize;
            if idx < column_names.len() {
                Expression::column([column_names[idx].as_str()])
            } else {
                Expression::unknown("bound_ref_out_of_range")
            }
        }
        Some(ExprStruct::Literal(lit)) => catalyst_literal_to_kernel(lit),
        // Cast operands become unknown, not the unwrapped child -- see the Cast arm in
        // `translate_predicate` for why (widening casts make the comparison mistype and
        // abort the scan instead of pruning).
        Some(ExprStruct::Cast(_)) => Expression::unknown("cast_not_pushed"),
        _ => Expression::unknown("unsupported_expr_operand"),
    }
}

fn catalyst_literal_to_kernel(lit: &spark_expression::Literal) -> Expression {
    use spark_expression::data_type::DataTypeId;
    // Spark stores TIMESTAMP / TIMESTAMP_NTZ as int64 micros and DATE as int32 days, in the int
    // value slots. Honor the declared `datatype` so the kernel literal carries the right TYPE -- a
    // bare LONG/INTEGER literal makes kernel reject the comparison against a temporal column
    // ("Invalid comparison: Timestamp(µs) < Int64") and abort the whole scan. This is the operand
    // path for binary comparisons (=, <, <=, >, >=, !=); IN-lists use `catalyst_literal_to_scalar`.
    let type_id = lit.datatype.as_ref().map(|d| d.type_id);
    match &lit.value {
        Some(literal::Value::BoolVal(b)) => Expression::literal(*b),
        // Byte/Short arrive in int32 slots (protobuf has no int8/int16); emit the NARROW
        // kernel scalar so the literal's type matches the column's stats type -- an
        // Integer literal against Byte/Short stats hard-errors kernel's evaluator
        // (same class as the Int64<=Int32 abort). Mirrors `catalyst_literal_to_scalar`.
        Some(literal::Value::ByteVal(v)) => Expression::literal(Scalar::Byte(*v as i8)),
        Some(literal::Value::ShortVal(v)) => Expression::literal(Scalar::Short(*v as i16)),
        Some(literal::Value::IntVal(v)) if type_id == Some(DataTypeId::Date as i32) => {
            Expression::literal(Scalar::Date(*v))
        }
        Some(literal::Value::IntVal(v)) => Expression::literal(*v),
        Some(literal::Value::LongVal(v)) if type_id == Some(DataTypeId::Timestamp as i32) => {
            Expression::literal(Scalar::Timestamp(*v))
        }
        Some(literal::Value::LongVal(v)) if type_id == Some(DataTypeId::TimestampNtz as i32) => {
            Expression::literal(Scalar::TimestampNtz(*v))
        }
        Some(literal::Value::LongVal(v)) => Expression::literal(*v),
        Some(literal::Value::FloatVal(v)) => Expression::literal(*v),
        Some(literal::Value::DoubleVal(v)) => Expression::literal(*v),
        Some(literal::Value::StringVal(s)) => Expression::literal(s.as_str()),
        // Any literal kind we can't faithfully type (decimal, binary, list, NULL, ...) becomes
        // UNKNOWN, which kernel treats as non-skippable -- always correct. The two tempting
        // fallbacks are both wrong: a typed-null literal makes `col = NULL` evaluate to NULL,
        // which data skipping treats as "never true" and prunes EVERY file (silently dropping
        // rows for e.g. a decimal-literal filter); and a wrongly-TYPED literal aborts the whole
        // scan, because kernel's stats evaluator hard-errors on comparisons like
        // `Int64 <= Int32` instead of degrading to "don't skip".
        _ => Expression::unknown("unsupported_literal_kind"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_comet_proto::spark_expression::{
        BinaryExpr, BoundReference, Cast, In, Literal, UnaryExpr,
    };

    // ---- builders for proto Exprs ----

    fn mk_expr(es: ExprStruct) -> Expr {
        Expr {
            expr_struct: Some(es),
            query_context: None,
            expr_id: None,
        }
    }

    fn lit_int(v: i32) -> Expr {
        mk_expr(ExprStruct::Literal(Literal {
            value: Some(literal::Value::IntVal(v)),
            ..Default::default()
        }))
    }

    fn lit_string(s: &str) -> Expr {
        mk_expr(ExprStruct::Literal(Literal {
            value: Some(literal::Value::StringVal(s.to_string())),
            ..Default::default()
        }))
    }

    fn bound_ref(idx: i32) -> Expr {
        mk_expr(ExprStruct::Bound(BoundReference {
            index: idx,
            ..Default::default()
        }))
    }

    fn binary(struct_fn: impl FnOnce(Box<BinaryExpr>) -> ExprStruct, l: Expr, r: Expr) -> Expr {
        let be = BinaryExpr {
            left: Some(Box::new(l)),
            right: Some(Box::new(r)),
        };
        mk_expr(struct_fn(Box::new(be)))
    }

    fn unary(struct_fn: impl FnOnce(Box<UnaryExpr>) -> ExprStruct, child: Expr) -> Expr {
        let ue = UnaryExpr {
            child: Some(Box::new(child)),
        };
        mk_expr(struct_fn(Box::new(ue)))
    }

    fn pred_str(p: &Predicate) -> String {
        format!("{p:?}")
    }

    // ---- literal coverage ----

    #[test]
    fn literal_kinds_all_translate() {
        let cases: Vec<(literal::Value, &str)> = vec![
            (literal::Value::BoolVal(true), "true"),
            (literal::Value::ByteVal(5), "5"),
            (literal::Value::ShortVal(6), "6"),
            (literal::Value::IntVal(7), "7"),
            (literal::Value::LongVal(8), "8"),
            (literal::Value::FloatVal(1.5), "1.5"),
            (literal::Value::DoubleVal(2.5), "2.5"),
            (literal::Value::StringVal("hi".to_string()), "hi"),
        ];
        for (val, needle) in cases {
            let lit = Literal {
                value: Some(val),
                ..Default::default()
            };
            let expr = catalyst_literal_to_kernel(&lit);
            assert!(
                format!("{expr:?}").contains(needle),
                "literal didn't translate: needle={needle}, got={expr:?}"
            );
        }
    }

    // Temporal literals must carry their kernel TYPE so a predicate against a TIMESTAMP/DATE column
    // type-checks in kernel. Before the fix a TIMESTAMP `LongVal` became a bare `Scalar::Long`,
    // making kernel reject "Timestamp(µs) < Int64" and abort the scan (gencol partition-filter
    // repro). This is the binary-operand path (`catalyst_literal_to_kernel`).
    #[test]
    fn temporal_literals_carry_kernel_type() {
        use spark_expression::data_type::DataTypeId;
        use spark_expression::DataType as ProtoDataType;
        let mk = |val: literal::Value, tid: DataTypeId| Literal {
            value: Some(val),
            datatype: Some(ProtoDataType {
                type_id: tid as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        // TIMESTAMP (int64 micros) -> Scalar::Timestamp, NOT Long.
        let ts = catalyst_literal_to_kernel(&mk(
            literal::Value::LongVal(1_600_000_000_000_000),
            DataTypeId::Timestamp,
        ));
        assert!(
            format!("{ts:?}").contains("Timestamp"),
            "timestamp literal must be a Timestamp scalar, got {ts:?}"
        );
        // TIMESTAMP_NTZ -> Scalar::TimestampNtz.
        let tsntz =
            catalyst_literal_to_kernel(&mk(literal::Value::LongVal(123), DataTypeId::TimestampNtz));
        assert!(
            format!("{tsntz:?}").contains("TimestampNtz"),
            "timestamp_ntz literal must be a TimestampNtz scalar, got {tsntz:?}"
        );
        // DATE (int32 days) -> Scalar::Date.
        let d = catalyst_literal_to_kernel(&mk(literal::Value::IntVal(18_000), DataTypeId::Date));
        assert!(
            format!("{d:?}").contains("Date"),
            "date literal must be a Date scalar, got {d:?}"
        );
        // A plain LONG (non-temporal) stays Long.
        let l = catalyst_literal_to_kernel(&mk(literal::Value::LongVal(8), DataTypeId::Int64));
        let ls = format!("{l:?}");
        assert!(
            ls.contains('8') && !ls.contains("Timestamp"),
            "plain long literal must stay Long, got {ls}"
        );
    }

    #[test]
    fn literal_unsupported_becomes_unknown_not_null() {
        // Literal kinds we can't type (decimal, binary, NULL, ...) must translate to
        // UNKNOWN. A typed-null fallback silently prunes every file (`col = NULL` is never
        // true under data skipping) and a mistyped literal aborts the scan on kernel's
        // strict comparison type-check -- both proven against a live kernel scan in
        // scan::tests.
        for lit in [
            Literal {
                value: None,
                ..Default::default()
            },
            Literal {
                value: Some(literal::Value::DecimalVal(vec![1, 2, 3])),
                ..Default::default()
            },
            Literal {
                value: Some(literal::Value::BytesVal(vec![1])),
                ..Default::default()
            },
        ] {
            let expr = catalyst_literal_to_kernel(&lit);
            let dbg = format!("{expr:?}");
            assert!(
                dbg.to_lowercase().contains("unknown"),
                "unsupported literal must become UNKNOWN, got: {dbg}"
            );
            assert!(
                !dbg.to_lowercase().contains("null"),
                "unsupported literal must NOT become a null literal, got: {dbg}"
            );
        }
    }

    #[test]
    fn literal_to_scalar_extracts_all_kinds() {
        let exprs = vec![
            (lit_int(7), Scalar::Integer(7)),
            (lit_string("foo"), Scalar::String("foo".into())),
        ];
        for (expr, want) in exprs {
            let got = catalyst_literal_to_scalar(&expr).unwrap();
            assert_eq!(format!("{got:?}"), format!("{want:?}"));
        }
    }

    #[test]
    fn literal_to_scalar_non_literal_returns_none() {
        assert!(catalyst_literal_to_scalar(&bound_ref(0)).is_none());
    }

    // ---- binary operators ----

    #[test]
    fn binary_eq_with_bound_ref_and_literal() {
        let expr = binary(ExprStruct::Eq, bound_ref(0), lit_int(7));
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["x".to_string()]);
        let s = pred_str(&p);
        // Should mention "Eq" or "=" plus column name x and value 7
        assert!(s.contains("Eq") || s.contains("Equal"), "got: {s}");
        assert!(s.contains("x"), "no column name: {s}");
    }

    #[test]
    fn binary_all_relops_translate() {
        let kinds: Vec<fn(Box<BinaryExpr>) -> ExprStruct> = vec![
            ExprStruct::Eq,
            ExprStruct::Neq,
            ExprStruct::Lt,
            ExprStruct::LtEq,
            ExprStruct::Gt,
            ExprStruct::GtEq,
        ];
        for k in kinds {
            let expr = binary(k, bound_ref(0), lit_int(1));
            let p = catalyst_to_kernel_predicate_with_names(&expr, &["c".to_string()]);
            let s = pred_str(&p);
            assert!(
                !s.contains("Unknown") && !s.contains("unknown"),
                "operator translated to unknown: {s}"
            );
        }
    }

    #[test]
    fn binary_missing_child_falls_back_to_unknown() {
        let be = BinaryExpr {
            left: Some(Box::new(bound_ref(0))),
            // right is None
            ..Default::default()
        };
        let expr = mk_expr(ExprStruct::Eq(Box::new(be)));
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("binary_missing_child"));
    }

    // ---- logical operators ----

    #[test]
    fn and_combines_two_children() {
        let l = binary(ExprStruct::Eq, bound_ref(0), lit_int(1));
        let r = binary(ExprStruct::Eq, bound_ref(1), lit_int(2));
        let expr = binary(ExprStruct::And, l, r);
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["a".to_string(), "b".to_string()]);
        let s = pred_str(&p);
        assert!(s.contains("a") && s.contains("b"), "{s}");
    }

    #[test]
    fn or_combines_two_children() {
        let l = binary(ExprStruct::Eq, bound_ref(0), lit_int(1));
        let r = binary(ExprStruct::Eq, bound_ref(0), lit_int(2));
        let expr = binary(ExprStruct::Or, l, r);
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["a".to_string()]);
        let _ = pred_str(&p); // just check it doesn't panic
    }

    #[test]
    fn and_missing_child_falls_back_to_unknown() {
        let be = BinaryExpr {
            left: Some(Box::new(bound_ref(0))),
            ..Default::default()
        };
        let expr = mk_expr(ExprStruct::And(Box::new(be)));
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("and_missing_child"));
    }

    #[test]
    fn or_missing_child_falls_back_to_unknown() {
        let be = BinaryExpr {
            right: Some(Box::new(bound_ref(0))),
            ..Default::default()
        };
        let expr = mk_expr(ExprStruct::Or(Box::new(be)));
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("or_missing_child"));
    }

    // ---- unary operators ----

    #[test]
    fn is_null_translates() {
        let expr = unary(ExprStruct::IsNull, bound_ref(0));
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["c".to_string()]);
        let s = pred_str(&p);
        assert!(s.contains("Null") || s.contains("null"), "{s}");
    }

    #[test]
    fn is_not_null_translates() {
        let expr = unary(ExprStruct::IsNotNull, bound_ref(0));
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["c".to_string()]);
        let s = pred_str(&p);
        assert!(s.contains("Null") || s.contains("null"), "{s}");
    }

    #[test]
    fn not_translates() {
        let inner = binary(ExprStruct::Eq, bound_ref(0), lit_int(1));
        let expr = unary(ExprStruct::Not, inner);
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["c".to_string()]);
        let s = pred_str(&p);
        assert!(s.contains("Not") || s.contains("not"), "{s}");
    }

    #[test]
    fn unary_missing_child_falls_back_to_unknown() {
        let expr = mk_expr(ExprStruct::IsNull(Box::default()));
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("missing_child"));
    }

    // ---- IN ----

    #[test]
    fn in_with_untranslatable_element_becomes_unknown() {
        // `col IN (1, <decimal>)` must NOT translate to `col IN (1)` -- a partial list
        // is a strictly stronger predicate (could prune files whose only matches carry
        // the dropped value).
        let in_expr = In {
            in_value: Some(Box::new(bound_ref(0))),
            lists: vec![
                lit_int(1),
                mk_expr(ExprStruct::Literal(Literal {
                    value: Some(literal::Value::DecimalVal(vec![1, 2])),
                    ..Default::default()
                })),
            ],
            negated: false,
        };
        let p = translate_in(&in_expr, &["c".to_string()]);
        assert!(
            pred_str(&p).contains("in_unsupported_element"),
            "got: {}",
            pred_str(&p)
        );
    }

    #[test]
    fn byte_short_literals_carry_narrow_kernel_types() {
        // Byte/Short literals must produce NARROW kernel scalars in the comparison
        // path -- an Integer literal against Byte/Short column stats hard-errors
        // kernel's evaluator (Int32-vs-Int8 class of abort).
        let byte_lit = mk_expr(ExprStruct::Literal(Literal {
            value: Some(literal::Value::ByteVal(7)),
            ..Default::default()
        }));
        let e = catalyst_to_kernel_expression_with_names(&byte_lit, &[]);
        assert!(format!("{e:?}").contains("Byte"), "got: {e:?}");
        let short_lit = mk_expr(ExprStruct::Literal(Literal {
            value: Some(literal::Value::ShortVal(300)),
            ..Default::default()
        }));
        let e = catalyst_to_kernel_expression_with_names(&short_lit, &[]);
        assert!(format!("{e:?}").contains("Short"), "got: {e:?}");
    }

    #[test]
    fn in_translates_with_literal_list() {
        let in_expr = In {
            in_value: Some(Box::new(bound_ref(0))),
            lists: vec![lit_int(1), lit_int(2), lit_int(3)],
            negated: false,
        };
        let expr = mk_expr(ExprStruct::In(Box::new(in_expr)));
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["c".to_string()]);
        let s = pred_str(&p);
        assert!(!s.contains("unknown"), "got: {s}");
    }

    #[test]
    fn in_negated_wraps_with_not() {
        let in_expr = In {
            in_value: Some(Box::new(bound_ref(0))),
            lists: vec![lit_int(1)],
            negated: true,
        };
        let expr = mk_expr(ExprStruct::In(Box::new(in_expr)));
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["c".to_string()]);
        let s = pred_str(&p);
        assert!(s.contains("Not") || s.contains("not"), "{s}");
    }

    #[test]
    fn in_empty_list_falls_back_to_unknown() {
        let in_expr = In {
            in_value: Some(Box::new(bound_ref(0))),
            lists: vec![],
            ..Default::default()
        };
        let expr = mk_expr(ExprStruct::In(Box::new(in_expr)));
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("in_no_literal_values"));
    }

    #[test]
    fn in_missing_value_falls_back_to_unknown() {
        let in_expr = In::default(); // in_value is None
        let expr = mk_expr(ExprStruct::In(Box::new(in_expr)));
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("in_missing_value"));
    }

    // ---- Cast unwrap ----

    #[test]
    fn cast_becomes_unknown_in_predicate_context() {
        // Casts are NOT unwrapped: post-unwrap the literal carries the cast's target
        // type, which mismatches the column's stats type and aborts kernel's stats
        // evaluator (whole-scan retry, all conjuncts lose pruning). Unknown localizes
        // the damage to this conjunct.
        let cast = Cast {
            child: Some(Box::new(binary(ExprStruct::Eq, bound_ref(0), lit_int(1)))),
            ..Default::default()
        };
        let expr = mk_expr(ExprStruct::Cast(Box::new(cast)));
        let p = catalyst_to_kernel_predicate_with_names(&expr, &["c".to_string()]);
        assert!(
            pred_str(&p).contains("cast_not_pushed"),
            "got: {}",
            pred_str(&p)
        );
    }

    #[test]
    fn cast_becomes_unknown_in_expression_context() {
        let cast = Cast {
            child: Some(Box::new(bound_ref(0))),
            ..Default::default()
        };
        let expr = mk_expr(ExprStruct::Cast(Box::new(cast)));
        let kernel_expr = catalyst_to_kernel_expression_with_names(&expr, &["x".to_string()]);
        assert!(
            format!("{kernel_expr:?}").contains("cast_not_pushed"),
            "got: {kernel_expr:?}"
        );
    }

    #[test]
    fn cast_missing_child_falls_back_to_unknown() {
        let expr = mk_expr(ExprStruct::Cast(Box::default()));
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("cast_not_pushed"));
    }

    // ---- BoundReference resolution ----

    #[test]
    fn bound_ref_resolves_to_column_name() {
        let expr = bound_ref(1);
        let kernel_expr = catalyst_to_kernel_expression_with_names(
            &expr,
            &["a".to_string(), "b".to_string(), "c".to_string()],
        );
        assert!(format!("{kernel_expr:?}").contains("b"));
    }

    #[test]
    fn bound_ref_out_of_range_yields_unknown() {
        let expr = bound_ref(5);
        let kernel_expr =
            catalyst_to_kernel_expression_with_names(&expr, &["only_one".to_string()]);
        assert!(format!("{kernel_expr:?}").contains("bound_ref_out_of_range"));
    }

    #[test]
    fn bound_ref_with_empty_names_yields_unknown() {
        let expr = bound_ref(0);
        let kernel_expr = catalyst_to_kernel_expression_with_names(&expr, &[]);
        assert!(format!("{kernel_expr:?}").contains("bound_ref_out_of_range"));
    }

    // ---- unsupported fallback ----

    #[test]
    fn unsupported_expr_kind_falls_back_to_unknown_predicate() {
        // An Expr with no expr_struct at all -> unsupported_catalyst_expr.
        let expr = Expr {
            expr_struct: None,
            query_context: None,
            expr_id: None,
        };
        let p = catalyst_to_kernel_predicate(&expr);
        assert!(pred_str(&p).contains("unsupported_catalyst_expr"));
    }

    // ---- scalar type inference ----

    #[test]
    fn scalar_to_kernel_type_round_trips() {
        let cases = vec![
            (Scalar::Boolean(true), "BOOLEAN"),
            (Scalar::Integer(0), "INTEGER"),
            (Scalar::Long(0), "LONG"),
            (Scalar::String("".into()), "STRING"),
        ];
        for (scalar, name) in cases {
            let dt = scalar_to_kernel_type(&scalar);
            // DataType::Display for these is "boolean"/"integer"/"long"/"string".
            // Just verify it's not panicking + roughly matches.
            assert!(
                format!("{dt:?}").to_uppercase().contains(name) || format!("{dt:?}").contains(name),
                "type mismatch for {scalar:?}: got {dt:?}"
            );
        }
    }
}

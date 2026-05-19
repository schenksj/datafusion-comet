/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

import org.apache.comet.serde.CometOperatorSerde

/**
 * Reflection-based bridge to the optional `contrib/delta/` integration.
 *
 * On default builds the contrib classes don't exist on the classpath, so the reflective class
 * lookups fail and every method here returns the "not handled" sentinel. On builds compiled with
 * `-Pcontrib-delta` (Maven) + `--features contrib-delta` (Cargo), the contrib classes are present
 * and the lookups resolve, dispatching the call into the contrib helpers.
 *
 * Keeping this bridge as one small file in core lets the Delta detection block in `CometScanRule`
 * and the serde dispatch in `CometExecRule` stay ~10 lines each -- exactly the shape Parth's
 * review on #4339 asked for.
 *
 * No `SPI`, no `ServiceLoader`, no registry: the contrib provides its own static helper objects
 * with stable names; this bridge just calls them.
 */
object DeltaIntegration {

  private val ScanRuleClass = "org.apache.comet.contrib.delta.DeltaScanRule"
  private val SerdeClass = "org.apache.comet.contrib.delta.CometDeltaNativeScan"

  /** scanImpl tag the contrib stamps on CometScanExec markers it produces. */
  val DeltaScanImpl: String = "native_delta_compat"

  // Lazy class lookups -- single reflection cost per JVM, cached either as the
  // class handle or as the empty option if the contrib wasn't bundled.
  @volatile private var scanRuleLookup: Option[Option[Class[AnyRef]]] = None
  @volatile private var serdeLookup: Option[Option[Class[AnyRef]]] = None

  private def scanRuleCls: Option[Class[AnyRef]] =
    scanRuleLookup.getOrElse {
      val cls =
        try {
          // scalastyle:off classforname
          Some(Class.forName(ScanRuleClass).asInstanceOf[Class[AnyRef]])
          // scalastyle:on classforname
        } catch { case _: ClassNotFoundException => None }
      scanRuleLookup = Some(cls)
      cls
    }

  private def serdeCls: Option[Class[AnyRef]] =
    serdeLookup.getOrElse {
      val cls =
        try {
          // scalastyle:off classforname
          Some(Class.forName(SerdeClass).asInstanceOf[Class[AnyRef]])
          // scalastyle:on classforname
        } catch { case _: ClassNotFoundException => None }
      serdeLookup = Some(cls)
      cls
    }

  /** True when the Delta contrib was bundled into this build. */
  def isAvailable: Boolean = scanRuleCls.isDefined

  /**
   * Delegate the V1 scan transform to the Delta contrib when both (a) the contrib is on the
   * classpath, AND (b) the relation's file format is `DeltaParquetFileFormat`.
   *
   * Returns `Some(plan)` if the contrib handled the scan (either with a transformed
   * `CometScanExec` marker or by explicitly declining via the `withInfo` path); `None` to
   * indicate "not a Delta scan, proceed with the vanilla CometScanRule path".
   */
  // Cached reflective binding: resolved once per JVM. The contrib's
  // `transformV1IfDelta` is invoked for every V1 scan in every plan, even
  // non-Delta ones; resolving the Method on each call would be a per-scan
  // reflection round-trip just to find we don't apply.
  @volatile private var transformV1IfDeltaBindingCache
      : Option[Option[(AnyRef, java.lang.reflect.Method)]] = None

  private def transformV1IfDeltaBinding: Option[(AnyRef, java.lang.reflect.Method)] =
    transformV1IfDeltaBindingCache.getOrElse {
      val binding = scanRuleCls.flatMap { cls =>
        try {
          val module = cls.getField("MODULE$").get(null)
          val m = cls.getMethod(
            "transformV1IfDelta",
            classOf[SparkPlan],
            classOf[SparkSession],
            classOf[FileSourceScanExec],
            classOf[HadoopFsRelation])
          Some((module, m))
        } catch {
          case _: Exception => None
        }
      }
      transformV1IfDeltaBindingCache = Some(binding)
      binding
    }

  def transformV1IfDelta(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = {
    transformV1IfDeltaBinding.flatMap { case (module, m) =>
      try {
        Option(m.invoke(module, plan, session, scanExec, relation))
          .map(_.asInstanceOf[Option[SparkPlan]])
          .flatten
      } catch {
        // scalastyle:off
        case _: Exception => None
        // scalastyle:on
      }
    }
  }

  /**
   * The Delta scan handler, resolved via reflection from the contrib's `CometDeltaNativeScan`
   * companion object. Returns `None` when the contrib isn't bundled into this build.
   * `CometExecRule` calls this and passes the result through the standard `convertToComet(scan,
   * handler)` path so the Delta scan flows through the same code as `CometNativeScan` etc.
   */
  def scanHandler: Option[CometOperatorSerde[CometScanExec]] = serdeCls.flatMap { cls =>
    try {
      val module = cls.getField("MODULE$").get(null)
      Some(module.asInstanceOf[CometOperatorSerde[CometScanExec]])
    } catch {
      case _: Exception => None
    }
  }
}

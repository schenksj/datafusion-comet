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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.comet.{CometDeltaNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.spi.CometOperatorSerdeExtension

/**
 * Discovered via `META-INF/services/org.apache.comet.spi.CometOperatorSerdeExtension`.
 * Three SPI surfaces are exercised:
 *
 *   - `serdes`: class-keyed serde for `CometDeltaNativeScanExec` (the post-conversion
 *     native exec class the contrib defines). Picked up after the native plan is built.
 *   - `matchOperator`: predicate-keyed serde for the `CometScanExec` marker the
 *     contrib's `DeltaScanRuleExtension.transformV1` returns. `CometScanExec` is shared
 *     with core's generic file-scan dispatch, so we can't route on class alone --
 *     `scanImpl == CometDeltaNativeScan.ScanImpl` is the disambiguator.
 *   - `nativeParquetScanImpls`: declares that scans tagged with our scanImpl go through
 *     Comet's tuned ParquetSource. `CometScanExec.supportedDataFilters` consults this
 *     set to decide whether to drop dynamic-pruning filters + IsNull/IsNotNull on
 *     ArrayType columns the same way it does for `SCAN_NATIVE_DATAFUSION`.
 */
class DeltaOperatorSerdeExtension extends CometOperatorSerdeExtension {
  override def name: String = "delta"

  override def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(classOf[CometDeltaNativeScanExec] -> CometDeltaNativeScan)

  override def matchOperator(op: SparkPlan): Option[CometOperatorSerde[_]] = op match {
    case s: CometScanExec if s.scanImpl == CometDeltaNativeScan.ScanImpl =>
      Some(CometDeltaNativeScan)
    case _ => None
  }

  override def nativeParquetScanImpls: Set[String] = Set(CometDeltaNativeScan.ScanImpl)
}

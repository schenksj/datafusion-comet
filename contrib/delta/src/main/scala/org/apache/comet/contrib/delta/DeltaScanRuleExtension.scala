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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

import org.apache.comet.spi.CometScanRuleExtension

/**
 * Discovered via `META-INF/services/org.apache.comet.spi.CometScanRuleExtension` to
 * route Delta file-format scans through Comet's Delta-native path.
 *
 * SCAFFOLD: the full method port from delta-kernel-phase-1's CometScanRule (~650 lines:
 * `stripDeltaDvWrappers`, `nativeDeltaScan`, `withDeltaColumnMappingMetadata`,
 * `applyRowTrackingRewrite`, and their helpers) is in progress as a follow-up commit.
 * `preTransform` and `transformV1` currently return identity / `None`, which means
 * activating `-Pcontrib-delta` produces a build that compiles cleanly but does NOT
 * accelerate Delta reads -- they fall back to Spark's Delta path. This shape lets the
 * SPI plumbing (ServiceLoader discovery, registry merge, dispatch wiring) be
 * end-to-end verified before the method bodies land.
 */
class DeltaScanRuleExtension extends CometScanRuleExtension {

  override def name: String = "delta"

  override def matchesV1(relation: HadoopFsRelation): Boolean =
    DeltaReflection.isDeltaFileFormat(relation.fileFormat)

  override def preTransform(plan: SparkPlan, session: SparkSession): SparkPlan = {
    // TODO(P4b): port stripDeltaDvWrappers + collectDeltaScanBelow +
    // scanBelowFallsBackForDvs + isDeltaDvFilterPattern + findAndStripDeltaScanBelow +
    // rebuildDeltaScanWithoutDvColumn from delta-kernel-phase-1's CometScanRule. Use a
    // Spark TreeNodeTag to mark `dvProtectedScans` for transformV1 to consult.
    plan
  }

  override def transformV1(
      plan: SparkPlan,
      scanExec: FileSourceScanExec,
      session: SparkSession): Option[SparkPlan] = {
    // TODO(P4b): port nativeDeltaScan + withDeltaColumnMappingMetadata +
    // applyRowTrackingRewrite from delta-kernel-phase-1's CometScanRule. Several
    // CometScanRule helpers (isSchemaSupported, encryptionEnabled,
    // isEncryptionConfigSupported, withInfo) are currently private; expose those via
    // a small additional SPI surface or copy them into this contrib in P4b.
    None
  }
}

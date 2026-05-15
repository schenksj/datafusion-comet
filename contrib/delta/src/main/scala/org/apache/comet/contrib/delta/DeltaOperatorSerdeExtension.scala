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

import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.spi.CometOperatorSerdeExtension

/**
 * Discovered via `META-INF/services/org.apache.comet.spi.CometOperatorSerdeExtension` to
 * register `CometDeltaNativeScanExec`'s serde with `CometExecRule`. Without this entry,
 * `CometExecRule` wouldn't know how to convert a `CometDeltaNativeScanExec` into the
 * `ContribOp` envelope for the native dispatch path.
 */
class DeltaOperatorSerdeExtension extends CometOperatorSerdeExtension {
  override def name: String = "delta"

  override def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(classOf[CometDeltaNativeScanExec] -> CometDeltaNativeScan)
}

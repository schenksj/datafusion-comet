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

import org.apache.comet.{ConfigBuilder, ConfigEntry}

/**
 * Contrib-local config entries for the Delta integration. Lives in the contrib's package
 * rather than in core's `CometConf` so PR1 stays format-agnostic. Side-effect of object
 * construction is registering the entries with `CometConf.allConfs` (via the
 * `ConfigBuilder` machinery), so they show up in the generated user-guide docs and
 * `SQLConf` resolution works the usual way.
 */
object DeltaConf {

  val COMET_DELTA_NATIVE_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.comet.scan.deltaNative.enabled")
      .doc(
        "Whether to enable native Delta table scans via delta-kernel-rs. When enabled, " +
          "Delta tables are read directly through Comet's tuned ParquetSource + " +
          "DV-filter wrapper, bypassing Spark's Delta reader for better performance.")
      .booleanConf
      .createWithDefault(true)

  val COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE: ConfigEntry[Boolean] =
    ConfigBuilder("spark.comet.scan.deltaNative.fallbackOnUnsupportedFeature")
      .doc(
        "When true (default), the Delta contrib falls back to Spark's Delta reader on " +
          "any Delta protocol feature it doesn't yet support. When false, the contrib " +
          "raises an error instead -- useful for tests that want to assert the native " +
          "path is reachable for a particular query.")
      .booleanConf
      .createWithDefault(true)

  /**
   * Relation-options key the contrib reads to know whether the surrounding plan references
   * `input_file_name()` / `input_file_block_*`. When set to `"true"`, the contrib emits
   * `oneTaskPerPartition = true` on the `CometDeltaNativeScanExec` so packTasks keeps each
   * task in its own partition and `CometExecRDD.setInputFileForDeltaScan` can set
   * `InputFileBlockHolder` to the correct path.
   */
  val NeedsInputFileNameOption: String = "comet.contrib.delta.needsInputFileName"
}

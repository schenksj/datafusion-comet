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

package org.apache.spark.sql.comet

import org.apache.spark.TaskContext
import org.apache.spark.rdd.InputFileBlockHolder

/**
 * Thin wrapper around Spark's `InputFileBlockHolder` so the Delta contrib can populate the
 * executor's input-file thread-local without trying to import a `private[spark]` symbol from
 * `org.apache.comet.contrib.delta` (which would fail at scalac access-check time even though
 * the underlying JVM class is public). Lives under `org.apache.spark.sql.comet` for the same
 * reason `CometDeltaNativeScanExec` does -- the contrib's source-injection model lets us put
 * helper classes anywhere on the classpath at build time.
 *
 * Public-API surface is intentionally minimal: set the file, register an unset on task
 * completion, no holding of state across tasks.
 */
object DeltaInputFileBlockHolder {

  /**
   * Set Spark's `InputFileBlockHolder` to the given file path and size for the duration of the
   * current task. Registers a `TaskCompletionListener` (when `context` is non-null) to clear
   * the thread-local on task end so the value doesn't leak into subsequent tasks on the same
   * executor thread.
   *
   * `startOffset` is fixed at 0 — Delta partitions reference whole files; range-splitting that
   * surfaces a non-zero offset would invalidate `_metadata.file_path` anyway.
   */
  def set(filePath: String, fileSize: Long, context: TaskContext): Unit = {
    InputFileBlockHolder.set(filePath, 0L, fileSize)
    Option(context).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit](_ => InputFileBlockHolder.unset())
    }
  }
}

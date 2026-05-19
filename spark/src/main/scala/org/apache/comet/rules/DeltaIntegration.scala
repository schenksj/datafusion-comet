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
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

/**
 * Reflection-based bridge to the optional `contrib/delta/` integration.
 *
 * On default builds the contrib classes don't exist on the classpath, so the
 * `Class.forName` lookups fail and every method here returns the "not handled"
 * sentinel. On builds compiled with `-Pcontrib-delta` (Maven) +
 * `--features contrib-delta` (Cargo), the contrib classes are present and the
 * lookups resolve, dispatching the call into the contrib helpers.
 *
 * Keeping this bridge as one small file in core lets the Delta detection block
 * in `CometScanRule` and the serde dispatch in `CometExecRule` stay ~10 lines
 * each -- exactly the shape Parth's review on #4339 asked for.
 *
 * No `SPI`, no `ServiceLoader`, no registry: the contrib provides its own
 * static helper objects with stable names; this bridge just calls them.
 */
object DeltaIntegration {

  private val ScanRuleClass = "org.apache.comet.contrib.delta.DeltaScanRule"
  private val SerdeClass = "org.apache.comet.contrib.delta.CometDeltaNativeScan"
  /** scanImpl tag the contrib stamps on CometScanExec markers it produces. */
  val DeltaScanImpl: String = "native_delta_compat"

  // Lazy class lookups -- single reflection cost per JVM, cached either as the
  // class handle or as `None` if the contrib was not bundled.
  @volatile private var scanRuleLookup: Option[Option[Class[_]]] = None
  @volatile private var serdeLookup: Option[Option[Class[_]]] = None

  private def lookupClass(name: String, slot: Option[Option[Class[_]]]): Option[Class[_]] = {
    slot match {
      case Some(cached) => cached
      case None =>
        val cls =
          try Some(Class.forName(name))
          catch { case _: ClassNotFoundException => None }
        cls
    }
  }

  private def scanRuleCls: Option[Class[_]] =
    scanRuleLookup.getOrElse {
      val cls =
        try Some(Class.forName(ScanRuleClass))
        catch { case _: ClassNotFoundException => None }
      scanRuleLookup = Some(cls)
      cls
    }

  private def serdeCls: Option[Class[_]] =
    serdeLookup.getOrElse {
      val cls =
        try Some(Class.forName(SerdeClass))
        catch { case _: ClassNotFoundException => None }
      serdeLookup = Some(cls)
      cls
    }

  /** True when the Delta contrib was bundled into this build. */
  def isAvailable: Boolean = scanRuleCls.isDefined

  /**
   * Delegate the V1 scan transform to the Delta contrib when both
   *   (a) the contrib is on the classpath, AND
   *   (b) the relation's file format is `DeltaParquetFileFormat`.
   *
   * Returns `Some(plan)` if the contrib handled the scan (either with a
   * transformed `CometScanExec` marker or by explicitly declining via the
   * `withInfo` path); `None` to indicate "not a Delta scan, proceed with the
   * vanilla CometScanRule path".
   */
  def transformV1IfDelta(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = {
    scanRuleCls.flatMap { cls =>
      try {
        val module = cls.getField("MODULE$").get(null)
        val m = cls.getMethod(
          "transformV1IfDelta",
          classOf[SparkPlan],
          classOf[SparkSession],
          classOf[FileSourceScanExec],
          classOf[HadoopFsRelation])
        Option(m.invoke(module, plan, session, scanExec, relation))
          .map(_.asInstanceOf[Option[SparkPlan]])
          .flatten
      } catch {
        case _: Exception => None
      }
    }
  }

  /**
   * Delta serde dispatch invoked from `CometExecRule` when a Delta-scan marker
   * (`CometScanExec` with `scanImpl == DeltaScanImpl`) needs converting to its
   * native operator proto.
   *
   * Mirrors `Iceberg`'s shape: a single reflective call resolves to
   * `CometDeltaNativeScan.convert(scan, builder, childOp*)` in the contrib.
   * Returns the populated `Operator` (with `OpStruct::DeltaScan` set), or
   * `None` if the contrib isn't bundled or declined the conversion.
   */
  def convertScan(scan: Any, builder: Any): Option[Any] = {
    serdeCls.flatMap { cls =>
      try {
        val module = cls.getField("MODULE$").get(null)
        val m = cls.getMethods.find { m =>
          m.getName == "convert" && m.getParameterCount == 3
        }
        m.flatMap { method =>
          Option(method.invoke(module, scan, builder, Array.empty[Any]))
        }
      } catch {
        case _: Exception => None
      }
    }
  }
}

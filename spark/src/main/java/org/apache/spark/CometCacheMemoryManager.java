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

package org.apache.spark;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.TestBlockId;

import org.apache.comet.Native;

/**
 * Executor-lifetime bridge that accounts the object-store data cache's memory tier as off-heap
 * <em>storage</em> memory in Spark's unified memory manager, instead of consuming {@code
 * spark.executor.memoryOverhead} headroom (OBJECT_STORE_CACHE_DESIGN.md section 2.9).
 *
 * <p>Memory is reserved in fixed 64 MiB quanta (the SSD region unit). The native cache never
 * exceeds what Spark has granted: {@link #initialize(long)} acquires up to the configured cap, and
 * {@link #releaseQuanta(long)} - driven from {@link CometTaskMemoryManager}'s execution shortfall -
 * shrinks the native cache (which evicts to the new budget) and returns the freed quanta so query
 * execution can borrow them. This keeps the reservation reclaimable even though it is not backed by
 * real {@code MemoryStore} blocks.
 *
 * <p>Requires {@code spark.memory.offHeap.enabled}. Off (the default) leaves the cache on the
 * fixed-budget path. <b>Experimental - not yet validated on a live cluster.</b>
 */
public final class CometCacheMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(CometCacheMemoryManager.class);

  /** Reservation quantum: 64 MiB, matching the SSD region unit. */
  private static final long QUANTUM_BYTES = 64L * 1024 * 1024;

  /** Synthetic block id used only to attribute the reservation in Spark's bookkeeping. */
  private static final BlockId BLOCK_ID = new TestBlockId("comet-data-cache");

  private static final Object LOCK = new Object();
  private static final AtomicLong granted = new AtomicLong(0L);

  private static volatile boolean active = false;

  private static MemoryManager memoryManager;
  private static Native nativeLib;
  private static long capBytes;

  private CometCacheMemoryManager() {}

  /** Whether unified-memory accounting is active on this executor. Cheap; hot-path safe. */
  public static boolean isActive() {
    return active;
  }

  /**
   * Initialize once on the executor from Spark configs (set at cluster level via {@code --conf}).
   * No-op unless the data cache and unified-memory accounting are both enabled and off-heap memory
   * is available.
   */
  public static void maybeInitialize() {
    if (active) {
      return;
    }
    SparkEnv env = SparkEnv.get();
    if (env == null) {
      return;
    }
    SparkConf conf = env.conf();
    if (!conf.getBoolean("spark.comet.scan.dataCache.enabled", false)
        || !conf.getBoolean("spark.comet.scan.dataCache.unifiedMemory.enabled", false)) {
      return;
    }
    long cap = conf.getSizeAsBytes("spark.comet.scan.dataCache.memoryLimit", "512m");
    initialize(cap);
  }

  /** Acquire the initial grant (up to {@code cap} bytes) and push it to the native cache. */
  static void initialize(long cap) {
    synchronized (LOCK) {
      if (active) {
        return;
      }
      SparkEnv env = SparkEnv.get();
      if (env == null) {
        return;
      }
      if (!env.conf().getBoolean("spark.memory.offHeap.enabled", false)) {
        logger.warn(
            "Comet data cache unified-memory mode requires spark.memory.offHeap.enabled; "
                + "falling back to the fixed-budget path.");
        return;
      }
      memoryManager = env.memoryManager();
      nativeLib = new Native();
      capBytes = cap;
      active = true;
      grow();
      logger.info(
          "Comet data cache unified-memory accounting active: cap {} MiB, initial grant {} MiB",
          cap >> 20,
          granted.get() >> 20);
    }
  }

  /**
   * Try to acquire storage-memory quanta up to the cap. {@code acquireStorageMemory} returns false
   * when Spark cannot grant more (e.g. the storage pool is exhausted), so growth stops naturally at
   * whatever the cluster allows.
   */
  private static void grow() {
    synchronized (LOCK) {
      if (!active) {
        return;
      }
      boolean changed = false;
      while (granted.get() + QUANTUM_BYTES <= capBytes
          && memoryManager.acquireStorageMemory(BLOCK_ID, QUANTUM_BYTES, MemoryMode.OFF_HEAP)) {
        granted.addAndGet(QUANTUM_BYTES);
        changed = true;
      }
      if (changed) {
        pushBudget();
      }
    }
  }

  /**
   * Release enough whole quanta to free at least {@code shortfall} bytes, shrinking the native
   * cache first (so it evicts to the new budget) and then returning the storage memory to Spark.
   * Returns the number of bytes released. Called from the execution-shortfall hook.
   */
  public static long releaseQuanta(long shortfall) {
    if (!active || shortfall <= 0) {
      return 0L;
    }
    synchronized (LOCK) {
      long available = granted.get() / QUANTUM_BYTES;
      long needed = (shortfall + QUANTUM_BYTES - 1) / QUANTUM_BYTES;
      long quanta = Math.min(available, needed);
      if (quanta <= 0) {
        return 0L;
      }
      long bytes = quanta * QUANTUM_BYTES;
      granted.addAndGet(-bytes);
      // Shrink the native cache to the reduced grant BEFORE returning the memory to Spark, so
      // the cache has actually evicted the bytes it is about to give up.
      pushBudget();
      memoryManager.releaseStorageMemory(bytes, MemoryMode.OFF_HEAP);
      return bytes;
    }
  }

  private static void pushBudget() {
    long g = granted.get();
    try {
      nativeLib.setDataCacheMemoryBudget(g);
    } catch (Throwable t) {
      logger.warn("Failed to push data cache memory budget {} bytes to native", g, t);
    }
  }
}

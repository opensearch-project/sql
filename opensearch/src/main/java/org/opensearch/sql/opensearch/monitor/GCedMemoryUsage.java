/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

import com.sun.management.GarbageCollectionNotificationInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Get memory usage from GC notification listener, which is used in Calcite engine. */
public class GCedMemoryUsage implements MemoryUsage {
  private static final Logger LOG = LogManager.getLogger();
  private static final List<String> OLD_GEN_GC_ACTION_KEYWORDS =
      List.of("major", "concurrent", "old", "full", "marksweep");

  private GCedMemoryUsage() {
    registerGCListener();
  }

  // Lazy initialize the instance to avoid register GCListener in v2.
  private static class Holder {
    static final MemoryUsage INSTANCE = new GCedMemoryUsage();
  }

  /**
   * Get the singleton instance of GCedMemoryUsage.
   *
   * @return GCedMemoryUsage instance
   */
  public static MemoryUsage getInstance() {
    return Holder.INSTANCE;
  }

  private final AtomicLong usage = new AtomicLong(-1);

  @Override
  public long usage() {
    return usage.get();
  }

  @Override
  public void setUsage(long value) {
    usage.set(value);
  }

  private void registerGCListener() {
    boolean registered = false;
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      if (gcBean instanceof NotificationEmitter && isOldGenGc(gcBean.getName())) {
        LOG.info("{} listener registered for memory usage monitor.", gcBean.getName());
        registered = true;
        NotificationEmitter emitter = (NotificationEmitter) gcBean;
        emitter.addNotificationListener(
            new OldGenGCListener(),
            notification -> {
              if (!notification
                  .getType()
                  .equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
                return false;
              }
              CompositeData cd = (CompositeData) notification.getUserData();
              GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
              return isOldGenGc(info.getGcAction());
            },
            null);
      }
    }
    if (!registered) {
      // fallback to RuntimeMemoryUsage
      LOG.info("No old gen GC listener registered, fallback to RuntimeMemoryUsage");
      throw new OpenSearchMemoryHealthy.MemoryUsageException();
    }
  }

  private boolean isOldGenGc(String gcKeyword) {
    String keyword = gcKeyword.toLowerCase(Locale.ROOT);
    return OLD_GEN_GC_ACTION_KEYWORDS.stream().anyMatch(keyword::contains);
  }

  private static class OldGenGCListener implements NotificationListener {
    @Override
    public void handleNotification(Notification notification, Object handback) {
      CompositeData cd = (CompositeData) notification.getUserData();
      GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
      Map<String, java.lang.management.MemoryUsage> memoryUsageAfterGc =
          info.getGcInfo().getMemoryUsageAfterGc();
      // Skip Metaspace and CodeHeap spaces which the GC scope is out of stack GC.
      long totalStackUsed =
          memoryUsageAfterGc.entrySet().stream()
              .filter(
                  entry ->
                      !entry.getKey().equals("Metaspace") && !entry.getKey().startsWith("CodeHeap"))
              .mapToLong(entry -> entry.getValue().getUsed())
              .sum();
      getInstance().setUsage(totalStackUsed);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Old Gen GC detected, memory usage after GC is {} bytes.", totalStackUsed);
      }
    }
  }
}

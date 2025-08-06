/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

import com.sun.management.GarbageCollectionNotificationInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      if (gcBean instanceof NotificationEmitter) {
        NotificationEmitter emitter = (NotificationEmitter) gcBean;
        emitter.addNotificationListener(new OldGenGCListener(), null, null);
      }
    }
  }

  private static class OldGenGCListener implements NotificationListener {
    @Override
    public void handleNotification(Notification notification, Object handback) {
      if (notification
          .getType()
          .equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
        CompositeData cd = (CompositeData) notification.getUserData();
        GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);

        String gcAction = info.getGcAction();
        if (gcAction.contains("major")
            || gcAction.contains("concurrent")
            || gcAction.contains("old")
            || gcAction.contains("full")) {
          Map<String, java.lang.management.MemoryUsage> memoryUsageAfterGc =
              info.getGcInfo().getMemoryUsageAfterGc();
          String possibleOldGenKey = findOldGenKey(memoryUsageAfterGc.keySet());
          if (possibleOldGenKey != null) {
            java.lang.management.MemoryUsage oldGenUsage =
                memoryUsageAfterGc.get(possibleOldGenKey);
            getInstance().setUsage(oldGenUsage.getUsed());
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Old Gen GC detected, memory usage after GC is {} bytes.", getInstance().usage());
            }
          }
        }
      }
    }

    private String findOldGenKey(Set<String> memoryPoolNames) {
      LOG.info("Memory pool names: {}", memoryPoolNames);
      List<String> possibleOldGenKeys =
          Arrays.asList("G1 Old Gen", "PS Old Gen", "CMS Old Gen", "Tenured Gen", "Old Gen");
      for (String key : possibleOldGenKeys) {
        if (memoryPoolNames.contains(key)) {
          return key;
        }
      }
      return null;
    }
  }
}

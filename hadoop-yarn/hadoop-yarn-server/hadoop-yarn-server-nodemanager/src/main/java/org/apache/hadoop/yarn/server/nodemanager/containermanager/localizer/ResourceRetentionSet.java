/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.yarn.server.nodemanager.DeletionService;

/**
 * 资源保留,定期删除已经没有引用的资源,避免资源大小过大
 * 当接收到LocalizationEventType.CACHE_CLEANUP事件时,调用ResourceLocalizationService的handleCacheCleanup方法
 */
public class ResourceRetentionSet {

  private long delSize;//当前删除多少磁盘
  private long currentSize;//当前还有多少磁盘
  private final long targetSize;//磁盘最多不允许超过该值
  private final DeletionService delService;
  private final SortedMap<LocalizedResource,LocalResourcesTracker> retain;//排序,按照资源的常用与否顺序

  ResourceRetentionSet(DeletionService delService, long targetSize) {
    this(delService, targetSize, new LRUComparator());
  }

  ResourceRetentionSet(DeletionService delService, long targetSize,
      Comparator<? super LocalizedResource> cmp) {
    this(delService, targetSize,
        new TreeMap<LocalizedResource,LocalResourcesTracker>(cmp));
  }

  ResourceRetentionSet(DeletionService delService, long targetSize,
      SortedMap<LocalizedResource,LocalResourcesTracker> retain) {
    this.retain = retain;
    this.delService = delService;
    this.targetSize = targetSize;
  }

  /**
   * 添加资源,如果超过了限制则删除,前提是目前有引用的资源不算超限的范围
   * @param newTracker
   */
  public void addResources(LocalResourcesTracker newTracker) {
    for (LocalizedResource resource : newTracker) {
      currentSize += resource.getSize();
      if (resource.getRefCount() > 0) {
        // always retain resources in use 还有引用,说明不允许删除,则永远保持保留
        continue;
      }
      retain.put(resource, newTracker);
    }
    /**
     * 如果当前的大小-删除的大小,还大于最大上限,则进行继续删除,直到所有的都循环完毕
     */
    for (Iterator<Map.Entry<LocalizedResource,LocalResourcesTracker>> i = retain.entrySet().iterator();currentSize - delSize > targetSize && i.hasNext();) {
      Map.Entry<LocalizedResource,LocalResourcesTracker> rsrc = i.next();
      LocalizedResource resource = rsrc.getKey();
      LocalResourcesTracker tracker = rsrc.getValue();
      if (tracker.remove(resource, delService)) {
        delSize += resource.getSize();
        i.remove();
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Cache: ").append(currentSize).append(", ");
    sb.append("Deleted: ").append(delSize);
    return sb.toString();
  }

  static class LRUComparator implements Comparator<LocalizedResource> {
    public int compare(LocalizedResource r1, LocalizedResource r2) {
      long ret = r1.getTimestamp() - r2.getTimestamp();
      if (0 == ret) {
        return System.identityHashCode(r1) - System.identityHashCode(r2);
      }
      return ret > 0 ? 1 : -1;
    }
    public boolean equals(Object other) {
      return this == other;
    }
  }
}

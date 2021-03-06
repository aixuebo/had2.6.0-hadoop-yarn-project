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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Node usage report.
 * 报告节点的信息,比如该节点已经使用磁盘大小、未使用大小、该节点有多少个contain
 */
@Private
@Stable
public class SchedulerNodeReport {
  private final Resource used;//该节点已经使用资源
  private final Resource avail;//该节点尚可使用资源
  private final int num;//该节点存在多少个容器
  
  public SchedulerNodeReport(SchedulerNode node) {
    this.used = node.getUsedResource();
    this.avail = node.getAvailableResource();
    this.num = node.getNumContainers();
  }
  
  /**
   * @return the amount of resources currently used by the node.
   */
  public Resource getUsedResource() {
    return used;
  }

  /**
   * @return the amount of resources currently available on the node
   */
  public Resource getAvailableResource() {
    return avail;
  }

  /**
   * @return the number of containers currently running on this node.
   */
  public int getNumContainers() {
    return num;
  }
}

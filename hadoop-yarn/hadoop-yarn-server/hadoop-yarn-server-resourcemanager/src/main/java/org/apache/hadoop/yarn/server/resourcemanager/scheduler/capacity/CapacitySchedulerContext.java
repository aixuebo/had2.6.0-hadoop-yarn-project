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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * Read-only interface to {@link CapacityScheduler} context.
 * 表示调度器的上下文
 */
public interface CapacitySchedulerContext {
	
  //公平队列的配置对象
  CapacitySchedulerConfiguration getConfiguration();
  
  //RM调度的每一个任务的最小资源,包含CPU和内存
  Resource getMinimumResourceCapability();

  //RM调度的每一个任务的最大资源,包含CPU和内存
  Resource getMaximumResourceCapability();

  RMContainerTokenSecretManager getContainerTokenSecretManager();
  
  //在调度器中记录集群中节点数量
  int getNumClusterNodes();

  RMContext getRMContext();
  
  //表示集群总资源
  Resource getClusterResource();

  /**
   * Get the yarn configuration.
   */
  Configuration getConf();

  //应用的比较器,比较应用的ID即可
  Comparator<FiCaSchedulerApp> getApplicationComparator();

  //资源计算框架
  ResourceCalculator getResourceCalculator();

  //队列比较器
  Comparator<CSQueue> getQueueComparator();
  
  //获取某一个Node节点
  FiCaSchedulerNode getNode(NodeId nodeId);
}

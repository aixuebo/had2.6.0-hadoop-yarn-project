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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

public enum RMNodeEventType {
  
  STARTED,//由new状态改成Runing状态,表示该节点在注册时候触发 
  
  // Source: AdminService
  DECOMMISSION,
  
  // Source: AdminService, ResourceTrackerService
  RESOURCE_UPDATE,//更新该NodeManager上节点的资源信息事件

  // ResourceTrackerService
  STATUS_UPDATE,//更新该NodeManager上节点的状态信息
  REBOOTING,
  RECONNECTED,

  // Source: Application
  CLEANUP_APP,

  // Source: Container
  CONTAINER_ALLOCATED,
  CLEANUP_CONTAINER,//清理容器

  // Source: RMAppAttempt
  FINISHED_CONTAINERS_PULLED_BY_AM,//RMAppAttempt尝试任务回复给Node,说该尝试任务哪些容器已经完成了

  // Source: NMLivelinessMonitor
  EXPIRE
}

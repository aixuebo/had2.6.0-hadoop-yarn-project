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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

public enum RMContainerEventType {
	//RMContainerEventType.ACQUIRED
  // Source: SchedulerApp
  START,
  ACQUIRED,//容器已经准备发送给对应的AM了,是SchedulerApplicationAttempt调度器产生的该事件,表示队列已经分配了该容器
  KILL, // Also from Node on NodeRemoval
  RESERVED,//抢占,预保留,表示在哪个node节点上预留了多少资源的容器

  LAUNCHED,//说明该容器已经在节点上启动了
  FINISHED,//该容器已经完成

  // Source: ApplicationMasterService->Scheduler
  RELEASED,//由调度器产生,使该容器释放掉

  // Source: ContainerAllocationExpirer  
  EXPIRE,//该容器没有心跳了

  RECOVER//该容器是恢复事件
}

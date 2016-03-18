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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

public enum SchedulerEventType {
	
  // Source: Node
  NODE_ADDED,//向资源调度器触发一个事件,一个节点被添加到调度器中
  NODE_REMOVED,//当某一个节点不健康了,则从调度器移除该节点
  NODE_UPDATE,//节点通过心跳,发送该节点的容器信息,更新该节点将要启动的容器以及完成的容器,通知调度器,一些节点的容器有变更.即远程节点有新容器被启动或者老容器被完成
  NODE_RESOURCE_UPDATE,//更新一个节点的资源信息,重新计算该节点的资源

  // Source: RMApp
  APP_ADDED,//一个app提交到调度器中
  APP_REMOVED,

  // Source: RMAppAttempt
  APP_ATTEMPT_ADDED,//创建一个app的尝试任务,提交到队列中来了
  APP_ATTEMPT_REMOVED,//app的尝试任务从队列中删除

  // Source: ContainerAllocationExpirer
  CONTAINER_EXPIRED
}

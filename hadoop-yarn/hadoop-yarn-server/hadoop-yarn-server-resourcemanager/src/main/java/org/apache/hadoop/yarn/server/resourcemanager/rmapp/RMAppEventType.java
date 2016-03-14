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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

public enum RMAppEventType {
  // Source: ClientRMService
  START,//表示一个应用从客户端已经提交到了RM
  RECOVER,
  KILL,//可以是客户端发来kill一个应用
  MOVE, // Move app to a new queue 该app从一个队列换成另外队列

  // Source: Scheduler and RMAppManager
  APP_REJECTED,//app拒绝,比如权限不对，不允许提交

  // Source: Scheduler 调度器发送事件,通知已经接受了该app
  APP_ACCEPTED,

  // Source: RMAppAttempt
  ATTEMPT_REGISTERED,//表示该尝试任务的一个AM已经在某个节点上产生了
  ATTEMPT_UNREGISTERED,
  ATTEMPT_FINISHED, // Will send the final state 一个尝试任务完成
  ATTEMPT_FAILED,//一个尝试任务失败了
  ATTEMPT_KILLED,//一个尝试任务被kill了
  NODE_UPDATE,//node节点更新,主要切换node节点是否可以使用
  
  // Source: Container and ResourceTracker
  APP_RUNNING_ON_NODE,//表示该app在某个node上执行了一个容器

  // Source: RMStateStore
  APP_NEW_SAVED,//说明已经记录日志完成,因此可以接下来将app提交到调度器中了
  APP_UPDATE_SAVED,
}

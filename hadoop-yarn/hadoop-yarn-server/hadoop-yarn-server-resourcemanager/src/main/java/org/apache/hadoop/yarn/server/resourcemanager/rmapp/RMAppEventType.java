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
  MOVE, // Move app to a new queue

  // Source: Scheduler and RMAppManager
  APP_REJECTED,//app拒绝,比如权限不对，不允许提交

  // Source: Scheduler 调度器发送事件,通知已经接受了该app
  APP_ACCEPTED,

  // Source: RMAppAttempt
  ATTEMPT_REGISTERED,
  ATTEMPT_UNREGISTERED,
  ATTEMPT_FINISHED, // Will send the final state
  ATTEMPT_FAILED,
  ATTEMPT_KILLED,
  NODE_UPDATE,//node节点更新,主要切换node节点是否可以使用
  
  // Source: Container and ResourceTracker
  APP_RUNNING_ON_NODE,

  // Source: RMStateStore
  APP_NEW_SAVED,//说明已经记录日志完成,因此可以接下来将app提交到调度器中了
  APP_UPDATE_SAVED,
}

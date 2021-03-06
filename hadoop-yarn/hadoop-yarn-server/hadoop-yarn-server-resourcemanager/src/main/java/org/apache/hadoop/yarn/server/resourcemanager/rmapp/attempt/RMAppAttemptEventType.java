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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

/**
 * 参见ResourceManager中ApplicationAttemptEventDispatcher内部类,做为处理该类型的事件接收器
 */
public enum RMAppAttemptEventType {
  // Source: RMApp
  START,//创建RMAppAttempt实例,因为调度器已经接受了该app,所以该需要创建RMAppAttempt实例了
  KILL,

  // Source: AMLauncher
  LAUNCHED,//表示AM启动成功了
  LAUNCH_FAILED,//表示AM启动失败了

  // Source: AMLivelinessMonitor  AppAttempt长期没有向resourceManager发送心跳,则resourceManager认为其过期
  EXPIRE,//通知ResourceManager这个应用已经过期了,即app的尝试任务ApplicationMaster长时间没有心跳反应了,AMLivelinessMonitor类触发该事件
  
  // Source: ApplicationMasterService
  REGISTERED,//表示AM完全注册到RM了,发送一个事件,尝试任务的状态从LAUNCHED变成RUNNING状态
  STATUS_UPDATE,//更新AM的执行进度
  UNREGISTERED,//由ApplicationMasterService触发的,当一个AM失效了,就会触发该函数
  
  // Source: Containers
  CONTAINER_ALLOCATED,//有容器分配给该尝试任务了
  CONTAINER_FINISHED,
  
  // Source: RMStateStore
  ATTEMPT_NEW_SAVED,//AM信息已经保存完成
  ATTEMPT_UPDATE_SAVED,

  // Source: Scheduler
  ATTEMPT_ADDED,//当调度器接受了该尝试任务后产生该事件
  
  // Source: RMAttemptImpl.recover
  RECOVER

}

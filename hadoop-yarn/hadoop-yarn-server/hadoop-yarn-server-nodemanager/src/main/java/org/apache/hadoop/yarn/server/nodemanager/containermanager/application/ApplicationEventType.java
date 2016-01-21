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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

public enum ApplicationEventType {

  INIT_APPLICATION,//初始化应用,即初始化一些日志系统,设置应用的权限信息
  // Source: ResourceLocalizationService
  APPLICATION_INITED,//应用全部初始化完成,开始到run状态
  APPLICATION_RESOURCES_CLEANEDUP,//资源清理
  //resourceManager发来的信息,表示该应用完成,该完成可能是正常的完成,也可以应用异常导致的完成
  FINISH_APPLICATION, // Source: LogAggregationService if init fails  
  
  // Source: ContainerManager
  INIT_CONTAINER,//初始化一个容器
  // Source: Container
  APPLICATION_CONTAINER_FINISHED,//应用完成了一个容器时触发的事件

  // Source: Log Handler
  APPLICATION_LOG_HANDLING_INITED,//应用的日志初始化
  APPLICATION_LOG_HANDLING_FINISHED,//应用的日志完成
  APPLICATION_LOG_HANDLING_FAILED//应用的日志失败
}

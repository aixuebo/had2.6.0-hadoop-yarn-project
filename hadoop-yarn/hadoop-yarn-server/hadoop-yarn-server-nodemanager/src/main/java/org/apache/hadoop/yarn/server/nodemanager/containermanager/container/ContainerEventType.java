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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

public enum ContainerEventType {

  // Producer: ContainerManager
  INIT_CONTAINER,//容器初始化
  KILL_CONTAINER,//容器被kill掉
  UPDATE_DIAGNOSTICS_MSG,//更新容器输出信息
  CONTAINER_DONE,//容器完成

  // DownloadManager
  CONTAINER_INITED,
  RESOURCE_LOCALIZED,//资源加载完成
  RESOURCE_FAILED,//资源加载失败
  CONTAINER_RESOURCES_CLEANEDUP,//容器资源清理

  // Producer: ContainersLauncher
  CONTAINER_LAUNCHED,//准备启动容器事件
  CONTAINER_EXITED_WITH_SUCCESS,
  CONTAINER_EXITED_WITH_FAILURE,
  CONTAINER_KILLED_ON_REQUEST,//在请求阶段容器就被kill掉
}

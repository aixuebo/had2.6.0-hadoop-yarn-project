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

public enum ContainerState {
  
  NEW,//新容器
  
  LOCALIZING,//容器加载资源 
  LOCALIZATION_FAILED, //容器加载资源失败
  LOCALIZED, //容器加载资源完成
  
  RUNNING, //正在运行
  
  EXITED_WITH_SUCCESS,//容器成功后结束
  EXITED_WITH_FAILURE, //容器失败结束
  
  KILLING, //正在杀死该容器
  
  CONTAINER_CLEANEDUP_AFTER_KILL,//容器kill之后清理
  CONTAINER_RESOURCES_CLEANINGUP,//容器资源清理
  
  DONE//容器全部完成
}

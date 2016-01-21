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

/**
 * 应用的状态
 */
public enum ApplicationState {
  NEW, //初始状态
  INITING, //正在初始化中,尚未初始化完成
  RUNNING, //该应用已经在运行中了
  
  FINISHING_CONTAINERS_WAIT, //应用完成了,但是容器还存在,则发送kill杀死掉活着的容器
  APPLICATION_RESOURCES_CLEANINGUP, //清理应用程序的资源
  
  FINISHED //该应用完成,至于完成的原因是正常的完成,还是异常导致的完成
}

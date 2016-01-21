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

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 真正去初始化一个容器,此时该容器所在的应用已经处于运行中了,即所需要的资源已经加载完毕
 * ApplicationImpl.InitContainerTransition调用该方法
 */
public class ContainerInitEvent extends ContainerEvent {

  public ContainerInitEvent(ContainerId c) {
    super(c, ContainerEventType.INIT_CONTAINER);
  }

}

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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 对每一个容器发送该事件,表示该容器的一个资源已经下载完成
 * 传入参数是资源下载到哪个路径下,以及下载的资源大小
 */
public class ContainerResourceLocalizedEvent extends ContainerResourceEvent {

  private final Path loc;//容器加载到本地资源的存储路径

  public ContainerResourceLocalizedEvent(ContainerId container, LocalResourceRequest rsrc, Path loc) {
    super(container, ContainerEventType.RESOURCE_LOCALIZED, rsrc);
    this.loc = loc;
  }

  public Path getLocation() {
    return loc;
  }

}

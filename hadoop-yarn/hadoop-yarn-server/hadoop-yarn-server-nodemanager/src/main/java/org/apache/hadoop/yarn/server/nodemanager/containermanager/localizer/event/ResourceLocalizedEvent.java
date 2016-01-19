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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 已经资源被下载到本地后,发送该事件
 */
public class ResourceLocalizedEvent extends ResourceEvent {

  private final long size;//本地文件所占用的文件大小
  private final Path location;//本地资源所在路径

  /**
   * @param rsrc 等待下载的资源
   * @param location 下载完成后的本地路径
   * @param size 本地所占用磁盘大小
   */
  public ResourceLocalizedEvent(LocalResourceRequest rsrc, Path location,long size) {
    super(rsrc, ResourceEventType.LOCALIZED);
    this.size = size;
    this.location = location;
  }

  public Path getLocation() {
    return location;
  }

  public long getSize() {
    return size;
  }

}

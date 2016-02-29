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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;

/**
 * Component tracking resources all of the same {@link LocalResourceVisibility}
 * 表示管理本地资源,不同可见性,持有一个该对象
 * 题外话:
 * 根据可见性,每个可见性拥有一个该对象
 * 1.public的单独拥有一个该对象
 * 2.PRIVATE的每一个user对应一个该对象
 * 3.APPLICATION的每一个应用对应一个该对象
 * 
 * 该对象是一个容器,因此要保持若干个LocalizedResource对象
 */
interface LocalResourcesTracker extends EventHandler<ResourceEvent>, Iterable<LocalizedResource> {

  //该容器删除一个资源LocalizedResource
  boolean remove(LocalizedResource req, DeletionService delService);

  Path getPathForLocalization(LocalResourceRequest req, Path localDirPath);

  String getUser();

  //一个HDFS的资源请求,即参数,对应的返回值就是该资源下载到本地后的对象
  LocalizedResource getLocalizedResource(LocalResourceRequest request);
}

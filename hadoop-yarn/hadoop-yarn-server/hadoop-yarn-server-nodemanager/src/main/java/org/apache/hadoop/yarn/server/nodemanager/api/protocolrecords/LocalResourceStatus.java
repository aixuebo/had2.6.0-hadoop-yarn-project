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
package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.URL;

/**
 * 表示在下载容器内,每一个正在下载的对象状态信息
 * 参见LocalizerStatus
 */
public interface LocalResourceStatus {
  public LocalResource getResource();//该资源信息
  public ResourceStatusType getStatus();//资源加载的完成状态
  public URL getLocalPath();//该资源的本地路径
  public long getLocalSize();//该资源的大小
  public SerializedException getException();//该加载有异常的话,异常信息

  public void setResource(LocalResource resource);
  public void setStatus(ResourceStatusType status);
  public void setLocalPath(URL localPath);
  public void setLocalSize(long size);
  public void setException(SerializedException exception);
}

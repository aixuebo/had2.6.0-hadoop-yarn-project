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
package org.apache.hadoop.yarn.server.nodemanager.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;

import com.google.common.annotations.VisibleForTesting;

/**
 * 私有资源下载到本地的对象信息
 */
@Private
@VisibleForTesting
public interface ResourceLocalizationSpec {

  //待下载的私有资源
  public void setResource(LocalResource rsrc);
  public LocalResource getResource();

  //等待下载私有资源到本地的路径
  public void setDestinationDirectory(URL destinationDirectory);
  public URL getDestinationDirectory();
}
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

import java.util.List;

/**
 * 主要保存两个信息
 * 1.localizerId
 * 2.LocalResourceStatus集合
 */
public interface LocalizerStatus {

  String getLocalizerId();
  void setLocalizerId(String id);

  List<LocalResourceStatus> getResources();
  
  void addAllResources(List<LocalResourceStatus> resources);
  void addResourceStatus(LocalResourceStatus resource);
  
  LocalResourceStatus getResourceStatus(int index);
  void removeResource(int index);
  void clearResources();
}
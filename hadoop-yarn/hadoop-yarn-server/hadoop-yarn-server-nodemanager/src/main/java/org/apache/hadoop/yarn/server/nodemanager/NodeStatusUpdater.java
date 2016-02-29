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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ContainerId;

public interface NodeStatusUpdater extends Service {

  /**
   * Schedule a heartbeat to the ResourceManager outside of the normal,
   * periodic heartbeating process. This is typically called when the state
   * of containers on the node has changed to notify the RM sooner.
   */
  void sendOutofBandHeartBeat();

  /**
   * Get the ResourceManager identifier received during registration
   * @return the ResourceManager ID
   * ResourceManager的唯一标示
   */
  long getRMIdentifier();
  
  /**
   * Query if a container has recently completed
   * @param containerId the container ID
   * @return true if the container has recently completed
   * true标示一个容器最近已经完成了
   */
  public boolean isContainerRecentlyStopped(ContainerId containerId);
  
  /**
   * Add a container to the list of containers that have recently completed
   * @param containerId the ID of the completed container
   * 添加一个容器,说明该容器已经完成了
   */
  public void addCompletedContainer(ContainerId containerId);

  /**
   * Clear the list of recently completed containers
   * 清理最近完成的容器列表
   */
  public void clearFinishedContainersFromCache();
}

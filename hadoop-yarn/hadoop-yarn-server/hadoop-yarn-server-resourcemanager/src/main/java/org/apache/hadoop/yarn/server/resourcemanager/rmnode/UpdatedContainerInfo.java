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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerStatus;

/**
 * 更新容器的集合信息
 * 当节点状态更新的时候,
 * 1.会收到以前节点没有收到过的容器,即新容器
 * 2.会计算到远程节点已经完成的容器,即完成容器
 */
public class UpdatedContainerInfo {
  private List<ContainerStatus> newlyLaunchedContainers;//会收到以前节点没有收到过的容器,即新容器
  private List<ContainerStatus> completedContainers;//会计算到远程节点已经完成的容器,即完成容器
  
  public UpdatedContainerInfo() {
  }

  public UpdatedContainerInfo(List<ContainerStatus> newlyLaunchedContainers
      , List<ContainerStatus> completedContainers) {
    this.newlyLaunchedContainers = newlyLaunchedContainers;
    this.completedContainers = completedContainers;
  } 

  public List<ContainerStatus> getNewlyLaunchedContainers() {
    return this.newlyLaunchedContainers;
  }

  public List<ContainerStatus> getCompletedContainers() {
    return this.completedContainers;
  }
}

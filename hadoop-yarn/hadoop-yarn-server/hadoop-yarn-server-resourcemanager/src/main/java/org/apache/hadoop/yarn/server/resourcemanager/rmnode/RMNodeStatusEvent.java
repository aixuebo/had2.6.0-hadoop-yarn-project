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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;

/**
 * 更新节点的状态信息事件
 */
public class RMNodeStatusEvent extends RMNodeEvent {

   private final NodeHealthStatus nodeHealthStatus;//记录上一次该远程节点的健康检查统计信息
   private final List<ContainerStatus> containersCollection;//目前正在运行的容器状态
   private final NodeHeartbeatResponse latestResponse;//针对该节点的心跳回复统计信息
   private final List<ApplicationId> keepAliveAppIds;//应该被关闭的应用.但是此时还存在,这样的一个集合

  public RMNodeStatusEvent(NodeId nodeId, NodeHealthStatus nodeHealthStatus,
      List<ContainerStatus> collection, List<ApplicationId> keepAliveAppIds,
      NodeHeartbeatResponse latestResponse) {
    super(nodeId, RMNodeEventType.STATUS_UPDATE);
    this.nodeHealthStatus = nodeHealthStatus;
    this.containersCollection = collection;
    this.keepAliveAppIds = keepAliveAppIds;
    this.latestResponse = latestResponse;
  }

  public NodeHealthStatus getNodeHealthStatus() {
    return this.nodeHealthStatus;
  }

  public List<ContainerStatus> getContainers() {
    return this.containersCollection;
  }

  public NodeHeartbeatResponse getLatestResponse() {
    return this.latestResponse;
  }
  
  public List<ApplicationId> getKeepAliveAppIds() {
    return this.keepAliveAppIds;
  }
}
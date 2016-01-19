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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

/**
 * 节点心跳后,返回给节点的对象
 * 参见ResourceTrackerService类
 */
public interface NodeHeartbeatResponse {
  //新的responseId,上一个responseId+1的结果
  int getResponseId();
  void setResponseId(int responseId);
  

  //去清理的容器
  List<ContainerId> getContainersToCleanup();
  //去移除的容器
  List<ContainerId> getContainersToBeRemovedFromNM();
  //去清理的应用
  List<ApplicationId> getApplicationsToCleanup();

  //动作
  NodeAction getNodeAction();
  void setNodeAction(NodeAction action);

  //密钥
  MasterKey getContainerTokenMasterKey();
  void setContainerTokenMasterKey(MasterKey secretKey);
  //密钥  
  MasterKey getNMTokenMasterKey();
  void setNMTokenMasterKey(MasterKey secretKey);

  //去清理的容器
  void addAllContainersToCleanup(List<ContainerId> containers);

  // This tells NM to remove finished containers from its context. Currently, NM
  // will remove finished containers from its context only after AM has actually
  // received the finished containers in a previous allocate response
  void addContainersToBeRemovedFromNM(List<ContainerId> containers);
  
  //去清理的应用
  void addAllApplicationsToCleanup(List<ApplicationId> applications);

  //下一次请求时间
  long getNextHeartBeatInterval();
  void setNextHeartBeatInterval(long nextHeartBeatInterval);
  
  //发生异常的时候,发送特别的信息
  String getDiagnosticsMessage();
  void setDiagnosticsMessage(String diagnosticsMessage);

  // Credentials (i.e. hdfs tokens) needed by NodeManagers for application
  // localizations and logAggreations.
  Map<ApplicationId, ByteBuffer> getSystemCredentialsForApps();
  void setSystemCredentialsForApps(Map<ApplicationId, ByteBuffer> systemCredentials);
}

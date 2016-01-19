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

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

/**
 * Context interface for sharing information across components in the
 * NodeManager.
 * NodeManager的上下问
 */
public interface Context {

  /**
   * Return the nodeId. Usable only when the ContainerManager is started.
   * 
   * @return the NodeId
   * 该节点的NodeId
   */
  NodeId getNodeId();

  /**
   * Return the node http-address. Usable only after the Webserver is started.
   * 
   * @return the http-port
   * http的端口,仅仅在webserver被开启的时候是可用的
   */
  int getHttpPort();

  /**
   * 该节点上运行的应用集合
   */
  ConcurrentMap<ApplicationId, Application> getApplications();

  /**
   * 该节点上运行的应用与对应的加密校验信息
   */
  Map<ApplicationId, Credentials> getSystemCredentialsForApps();

  /**
   * 该节点上运行的容器集合
   */
  ConcurrentMap<ContainerId, Container> getContainers();

  NMContainerTokenSecretManager getContainerTokenSecretManager();
  
  NMTokenSecretManagerInNM getNMTokenSecretManager();

  /**
   * 该节点的健康情况
   */
  NodeHealthStatus getNodeHealthStatus();

  ContainerManagementProtocol getContainerManager();

  LocalDirsHandlerService getLocalDirsHandler();

  ApplicationACLsManager getApplicationACLsManager();

  //存储日志行为,便于恢复
  NMStateStoreService getNMStateStore();

  boolean getDecommissioned();

  //设置该节点是否已经准备shutdown了,true表示是关闭
  void setDecommissioned(boolean isDecommissioned);
}

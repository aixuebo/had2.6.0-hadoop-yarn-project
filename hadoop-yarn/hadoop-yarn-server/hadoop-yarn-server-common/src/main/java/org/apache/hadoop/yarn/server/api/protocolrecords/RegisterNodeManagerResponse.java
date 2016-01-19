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

import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

/**
 * 节点向ResourceManager注册后,返回的信息
 * 这些信息都是由ResourceManager生成并且返回的
 * 参见ResourceTrackerService类
 */
public interface RegisterNodeManagerResponse {
  
  //加密信息
  MasterKey getContainerTokenMasterKey();
  void setContainerTokenMasterKey(MasterKey secretKey);

  //加密信息
  MasterKey getNMTokenMasterKey();
  void setNMTokenMasterKey(MasterKey secretKey);

  /**
   * ResourceManager返回给节点的动作,
   * 例如:
   * 如果返回SHUTDOWN,则说明注册失败,有问题
   */
  NodeAction getNodeAction();
  void setNodeAction(NodeAction nodeAction);

  /**
   * ResourceManager的唯一标示,即ResourceManager启动时候的时间戳
   */
  long getRMIdentifier();
  void setRMIdentifier(long rmIdentifier);

  //ResourceManager返回的信息,当出现异常的时候,提示给节点用的
  String getDiagnosticsMessage();
  void setDiagnosticsMessage(String diagnosticsMessage);

  //ResourceManager的版本号YarnVersionInfo.getVersion()
  void setRMVersion(String version);
  String getRMVersion();
}

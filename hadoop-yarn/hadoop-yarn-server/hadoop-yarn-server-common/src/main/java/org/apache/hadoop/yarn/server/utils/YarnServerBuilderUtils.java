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

package org.apache.hadoop.yarn.server.utils;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

/**
 * Server Builder utilities to construct various objects.
 * 对节点向ResourceManager发送的心跳请求进行回复
 */
public class YarnServerBuilderUtils {

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public static NodeHeartbeatResponse newNodeHeartbeatResponse(int responseId,
      NodeAction action, List<ContainerId> containersToCleanUp,
      List<ApplicationId> applicationsToCleanUp,
      MasterKey containerTokenMasterKey, MasterKey nmTokenMasterKey,
      long nextHeartbeatInterval) {
    NodeHeartbeatResponse response = recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    response.setResponseId(responseId);//新的responseId,上一个responseId+1的结果
    response.setNodeAction(action);
    response.setContainerTokenMasterKey(containerTokenMasterKey);
    response.setNMTokenMasterKey(nmTokenMasterKey);
    response.setNextHeartBeatInterval(nextHeartbeatInterval);//下一次请求时间
    if(containersToCleanUp != null) {
      response.addAllContainersToCleanup(containersToCleanUp);//去清理的容器
    }
    if(applicationsToCleanUp != null) {
      response.addAllApplicationsToCleanup(applicationsToCleanUp);//去清理的应用
    }
    return response;
  }
}

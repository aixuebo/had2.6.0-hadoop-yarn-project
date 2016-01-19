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

package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;

@Private
@Stable
public interface ResourceManagerAdministrationProtocol extends GetUserMappingsProtocol {

  /**
   * 刷新队列
   */
  @Public
  @Stable
  @Idempotent
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request) 
  throws StandbyException, YarnException, IOException;

  /**
   * 刷新节点
   */
  @Public
  @Stable
  @Idempotent
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
  throws StandbyException, YarnException, IOException;

  @Public
  @Stable
  @Idempotent
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(RefreshSuperUserGroupsConfigurationRequest request) 
  throws StandbyException, YarnException, IOException;

  /**
   * 刷新用户和组的关系
   */
  @Public
  @Stable
  @Idempotent
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest request)
  throws StandbyException, YarnException, IOException;

  /**
   * 刷新管理员权限
   */
  @Public
  @Stable
  @Idempotent
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request)
  throws YarnException, IOException;

  
  /**
   * 刷新权限
   */
  @Public
  @Stable
  @Idempotent
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request)
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by admin to update nodes' resources to the
   * <code>ResourceManager</code> </p>.
   * 
   * <p>The admin client is required to provide details such as a map from 
   * {@link NodeId} to {@link ResourceOption} required to update resources on 
   * a list of <code>RMNode</code> in <code>ResourceManager</code> etc.
   * via the {@link UpdateNodeResourceRequest}.</p>
   * 
   * @param request request to update resource for a node in cluster.
   * @return (empty) response on accepting update.
   * @throws YarnException
   * @throws IOException
   * 更新集群中某些节点的资源
   */
  @Public
  @Evolving
  @Idempotent
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) 
  throws YarnException, IOException;
   
  /**
   * 向集群中添加标签集合
   */
  @Public
  @Evolving
  @Idempotent
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(AddToClusterNodeLabelsRequest request)
      throws YarnException, IOException;
   
  /**
   * 从集群中移除标签集合
   */
  @Public
  @Evolving
  @Idempotent
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request) throws YarnException, IOException;
  
  /**
   * 替换集群中某些节点的标签信息
   */
  @Public
  @Evolving
  @Idempotent
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException;
}

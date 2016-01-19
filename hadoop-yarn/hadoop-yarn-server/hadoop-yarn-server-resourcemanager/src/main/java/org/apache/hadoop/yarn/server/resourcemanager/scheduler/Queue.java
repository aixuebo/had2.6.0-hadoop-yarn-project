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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

@Evolving
@LimitedPrivate("yarn")
public interface Queue {
  /**
   * Get the queue name
   * @return queue name
   */
  String getQueueName();

  /**
   * Get the queue metrics 返回该队列的统计信息对象
   * @return the queue metrics
   */
  QueueMetrics getMetrics();

  /**
   * Get queue information 返回当前队列的基础信息
   * @param includeChildQueues include child queues? 返回的信息是否包含子队列
   * @param recursive recursively get child queue information? 是否递归循环子队列信息
   * @return queue information
   */
  QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive);
  
  /**
   * Get queue ACLs for given <code>user</code>.
   * @param user username
   * @return queue ACLs for user
   * 根据用户信息,返回该用户使用队列的权限集合,即该用户是否有提交任务权限，或者有管理权限
   */
  List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user);

  /**
   * 判断user是否有参数权限,即判断该用户是否有管理权限或者提交权限，true表示有权限
   * @param acl
   * @param user
   * @return
   */
  boolean hasAccess(QueueACL acl, UserGroupInformation user);
  
  public ActiveUsersManager getActiveUsersManager();

  /**
   * Recover the state of the queue for a given container.
   * @param clusterResource the resource of the cluster
   * @param schedulerAttempt the application for which the container was allocated
   * @param rmContainer the container that was recovered.
   */
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer);
  
  /**
   * Get labels can be accessed of this queue 表示该队列能访问的标签
   * labels={*}, means this queue can access any label 表示能访问任何标签
   * labels={ }, means this queue cannot access any label except node without label 表示不能访问任何标签,但是可以访问没有标签的
   * labels={a, b, c} means this queue can access a or b or c  只能访问a、b、c三个标签
   * @return labels
   * 该队列可以访问的标签
   */
  public Set<String> getAccessibleNodeLabels();
  
  /**
   * Get default label expression of this queue. If label expression of
   * ApplicationSubmissionContext and label expression of Resource Request not
   * set, this will be used.
   * 
   * @return default label expression
   * 返回默认标签
   */
  public String getDefaultNodeLabelExpression();
}

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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

/**
 * <code>CSQueue</code> represents a node in the tree of 
 * hierarchical queues in the {@link CapacityScheduler}.
 * CSQueue该类,代表CapacityScheduler公平调度器中队列树中的一个队列节点
 */
@Stable
@Private
public interface CSQueue 
extends org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue {
  /**
   * Get the parent <code>Queue</code>.
   * @return the parent queue
   * 父节点对象
   */
  public CSQueue getParent();

  /**
   * Set the parent <code>Queue</code>.
   * @param newParentQueue new parent queue
   * 设置父节点对象
   */
  public void setParent(CSQueue newParentQueue);

  /**
   * Get the queue name.
   * @return the queue name
   * 节点名称
   */
  public String getQueueName();

  /**
   * Get the full name of the queue, including the heirarchy.
   * @return the full name of the queue
   * 节点全路径
   */
  public String getQueuePath();
  
  /**
   * Get the configured <em>capacity</em> of the queue.
   * @return configured queue capacity
   * 配置文件中配置的该队列的资源
   * 参见capacity-scheduler-myself.xml
   */
  public float getCapacity();
  
  /**
   * Get actual <em>capacity</em> of the queue, this may be different from
   * configured capacity when mis-config take place, like add labels to the
   * cluster
   * 
   * @return actual queue capacity
   */
  public float getAbsActualCapacity();

  /**
   * Get capacity of the parent of the queue as a function of the 
   * cumulative capacity in the cluster.
   * @return capacity of the parent of the queue as a function of the 
   *         cumulative capacity in the cluster
   * 即该队列占用总资源的百分比.根据该队列的配置资源*父队列的资源百分比
   * 参见capacity-scheduler-myself.xml
   */
  public float getAbsoluteCapacity();
  
  /**
   * Get the configured maximum-capacity of the queue. 
   * @return the configured maximum-capacity of the queue
   * 配置文件中配置的最大占比值,参见capacity-scheduler-myself.xml
   */
  public float getMaximumCapacity();
  
  /**
   * Get maximum-capacity of the queue as a funciton of the cumulative capacity
   * of the cluster.
   * @return maximum-capacity of the queue as a funciton of the cumulative capacity
   *         of the cluster
   *  该队列占用总资源的最大百分比       
   *  参见capacity-scheduler-myself.xml
   */
  public float getAbsoluteMaximumCapacity();
  
  /**
   * Get the current absolute used capacity of the queue relative to the entire cluster.
   * @return queue absolute used capacity
   * 公式:usedResources/clusterResource,即该队列已经使用的资源占总资源的比例
   */
  public float getAbsoluteUsedCapacity();
  
  /**
   * Set absolute used capacity of the queue.
   * @param absUsedCapacity
   *          absolute used capacity of the queue
   * 公式:usedResources/clusterResource,即该队列已经使用的资源占总资源的比例
   */
  public void setAbsoluteUsedCapacity(float absUsedCapacity);

  /**
   * Get the current used capacity of nodes without label(s) of the queue
   * and it's children (if any).
   * @return queue used capacity
   * 公式:usedResources/(clusterResource*childQueue.getAbsoluteCapacity()
   * 翻译 已经使用的资源量/该队列分配的capacity量,即就是该队列的使用百分比
   */
  public float getUsedCapacity();
  
  /**
   * Set used capacity of the queue.
   * @param usedCapacity
   *          used capacity of the queue
   * 公式:usedResources/(clusterResource*childQueue.getAbsoluteCapacity()
   * 翻译 已经使用的资源量/该队列分配的capacity量,即就是该队列的使用百分比
   */
  public void setUsedCapacity(float usedCapacity);

  /**
   * Get the currently utilized resources which allocated at nodes without any
   * labels in the cluster by the queue and children (if any).
   * 
   * @return used resources by the queue and it's children
   * 该资源已经使用的资源大小
   */
  public Resource getUsedResources();
  
  /**
   * Get the current run-state of the queue
   * @return current run-state
   * 该队列的状态
   */
  public QueueState getState();
  
  /**
   * Get child queues
   * @return child queues
   * 返回子队列信息
   */
  public List<CSQueue> getChildQueues();
  
  /**
   * Check if the <code>user</code> has permission to perform the operation
   * @param acl ACL
   * @param user user
   * @return <code>true</code> if the user has the permission, 
   *         <code>false</code> otherwise
   *  判断该用户是否有参数权限       
   */
  public boolean hasAccess(QueueACL acl, UserGroupInformation user);
  
  /**
   * Submit a new application to the queue.
   * @param applicationId the applicationId of the application being submitted 应用ID
   * @param user user who submitted the application 应用的user
   * @param queue queue to which the application is submitted 应用所属队列name
   * 提交一个应用
   */
  public void submitApplication(ApplicationId applicationId, String user,
      String queue) throws AccessControlException;

  /**
   * Submit an application attempt to the queue.
   * 提交一个应用的实例
   */
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName);

  /**
   * An application submitted to this queue has finished.
   * @param applicationId
   * @param user user who submitted the application
   * 完成一个应用
   */
  public void finishApplication(ApplicationId applicationId, String user);

  /**
   * An application attempt submitted to this queue has finished.
   * 完成一个应用的实例
   */
  public void finishApplicationAttempt(FiCaSchedulerApp application,String queue);

  /**
   * Assign containers to applications in the queue or it's children (if any).
   * @param clusterResource the resource of the cluster.
   * @param node node on which resources are available
   * @param needToUnreserve assign container only if it can unreserve one first
   * @return the assignment
   * 分配一个容器在该节点
   */
  public CSAssignment assignContainers(Resource clusterResource, FiCaSchedulerNode node, boolean needToUnreserve);
  
  /**
   * A container assigned to the queue has completed.
   * @param clusterResource the resource of the cluster
   * @param application application to which the container was assigned
   * @param node node on which the container completed
   * @param container completed container, 
   *                  <code>null</code> if it was just a reservation
   * @param containerStatus <code>ContainerStatus</code> for the completed 
   *                        container
   * @param childQueue <code>CSQueue</code> to reinsert in childQueues 
   * @param event event to be sent to the container
   * @param sortQueues indicates whether it should re-sort the queues
   * 完成一个容器
   */
  public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      RMContainer container, ContainerStatus containerStatus, 
      RMContainerEventType event, CSQueue childQueue,
      boolean sortQueues);

  /**
   * Get the number of applications in the queue.
   * @return number of applications
   * 该队列拥有多少个应用
   */
  public int getNumApplications();

  
  /**
   * Reinitialize the queue.
   * @param newlyParsedQueue new queue to re-initalize from
   * @param clusterResource resources in the cluster
   * 重新初始化队列
   */
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource) 
  throws IOException;

   /**
   * Update the cluster resource for queues as we add/remove nodes
   * @param clusterResource the current cluster resource
   * 当新增和删除节点后,要更新集群总资源
   */
  public void updateClusterResource(Resource clusterResource);
  
  /**
   * Get the {@link ActiveUsersManager} for the queue.
   * @return the <code>ActiveUsersManager</code> for the queue
   */
  public ActiveUsersManager getActiveUsersManager();
  
  /**
   * Adds all applications in the queue and its subqueues to the given collection.
   * @param apps the collection to add the applications to
   * 向队列添加应用集合
   */
  public void collectSchedulerApplications(Collection<ApplicationAttemptId> apps);

  /**
  * Detach a container from this queue
  * @param clusterResource the current cluster resource
  * @param application application to which the container was assigned
  * @param container the container to detach
  * 减少一个容器
  */
  public void detachContainer(Resource clusterResource,
               FiCaSchedulerApp application, RMContainer container);

  /**
   * Attach a container to this queue
   * @param clusterResource the current cluster resource
   * @param application application to which the container was assigned
   * @param container the container to attach
   * 增加一个容器
   */
  public void attachContainer(Resource clusterResource,
               FiCaSchedulerApp application, RMContainer container);
  
  /**
   * Get absolute capacity by label of this queue can use 
   * @param nodeLabel
   * @return absolute capacity by label of this queue can use
   * 获取该标签能使用的绝对资源
   * 参见capacity-scheduler-myself.xml
   */
  public float getAbsoluteCapacityByNodeLabel(String nodeLabel);
  
  /**
   * Get absolute max capacity by label of this queue can use 
   * @param nodeLabel
   * @return absolute capacity by label of this queue can use
   * 获取该标签能使用的最大资源
   * 参见capacity-scheduler-myself.xml
   */
  public float getAbsoluteMaximumCapacityByNodeLabel(String nodeLabel);

  /**
   * Get capacity by node label
   * @param nodeLabel
   * @return capacity by node label
   * 获取该标签的资源
   */
  public float getCapacityByNodeLabel(String nodeLabel);
}

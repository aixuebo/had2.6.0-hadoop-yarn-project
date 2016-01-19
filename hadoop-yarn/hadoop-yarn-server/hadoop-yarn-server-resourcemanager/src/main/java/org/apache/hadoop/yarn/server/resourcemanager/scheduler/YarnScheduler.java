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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

/**
 * This interface is used by the components to talk to the
 * scheduler for allocating of resources, cleaning up resources.
 *
 */
public interface YarnScheduler extends EventHandler<SchedulerEvent> {

  /**
   * Get queue information
   * @param queueName queue name
   * @param includeChildQueues include child queues?是否获取子队列名称集合
   * @param recursive get children queues? 是否递归查询子队列的详细信息
   * @return queue information
   * @throws IOException
   * 获取给定队列名称的队列详细信息
   */
  @Public
  @Stable
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
      boolean recursive) throws IOException;

  /**
   * Get acls for queues for current user.
   * @return acls for queues for current user
   * 获取该队列的所有用户以及用户权限信息集合
   */
  @Public
  @Stable
  public List<QueueUserACLInfo> getQueueUserAclInfo();

  /**
   * Get the whole resource capacity of the cluster.
   * @return the whole resource capacity of the cluster.
   * 获取集群总资源
   */
  @LimitedPrivate("yarn")
  @Unstable
  public Resource getClusterResource();

  /**
   * Get minimum allocatable {@link Resource}.
   * @return minimum allocatable resource
   */
  @Public
  @Stable
  public Resource getMinimumResourceCapability();
  
  /**
   * Get maximum allocatable {@link Resource}.
   * @return maximum allocatable resource
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability();

  /**
   * Get the number of nodes available in the cluster.
   * @return the number of available nodes.
   * 获取集群中节点数
   */
  @Public
  @Stable
  public int getNumClusterNodes();
  
  /**
   * The main api between the ApplicationMaster and the Scheduler.
   * The ApplicationMaster is updating his future resource requirements
   * and may release containers he doens't need.
   * 
   * @param appAttemptId
   * @param ask 要申请的资源信息集合
   * @param release 要释放的容器集合,即这些容器已经完成了任务,可以释放了
   * @param blacklistAdditions 要添加的黑名单节点名称集合
   * @param blacklistRemovals  要移除的黑名单节点名称集合
   * @return the {@link Allocation} for the application
   * 为一个任务分配资源以及释放该任务已经完成的容器资源
   */
  @Public
  @Stable
  Allocation 
  allocate(ApplicationAttemptId appAttemptId, 
      List<ResourceRequest> ask,
      List<ContainerId> release, 
      List<String> blacklistAdditions, 
      List<String> blacklistRemovals);

  /**
   * Get node resource usage report.
   * @param nodeId
   * @return the {@link SchedulerNodeReport} for the node or null
   * if nodeId does not point to a defined node.
   * 报道某一个节点信息
   */
  @LimitedPrivate("yarn")
  @Stable
  public SchedulerNodeReport getNodeReport(NodeId nodeId);
  
  /**
   * Get the Scheduler app for a given app attempt Id.
   * @param appAttemptId the id of the application attempt
   * @return SchedulerApp for this given attempt.
   * 报告某一个任务的信息
   */
  @LimitedPrivate("yarn")
  @Stable
  SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId);

  /**
   * Get a resource usage report from a given app attempt ID.
   * @param appAttemptId the id of the application attempt
   * @return resource usage report for this given attempt
   * 返回一个任务的资源使用详细信息
   */
  @LimitedPrivate("yarn")
  @Evolving
  ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId);
  
  /**
   * Get the root queue for the scheduler.
   * @return the root queue for the scheduler.
   * 获取该队列的统计信息
   */
  @LimitedPrivate("yarn")
  @Evolving
  QueueMetrics getRootQueueMetrics();

  /**
   * Check if the user has permission to perform the operation.
   * If the user has {@link QueueACL#ADMINISTER_QUEUE} permission,
   * this user can view/modify the applications in this queue
   * @param callerUGI
   * @param acl
   * @param queueName
   * @return <code>true</code> if the user has the permission,
   *         <code>false</code> otherwise
   *         判断该用户是否有访问该队列的QueueACL权限
   */
  boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName);
  
  /**
   * Gets the apps under a given queue
   * @param queueName the name of the queue.
   * @return a collection of app attempt ids in the given queue.
   * 返回某一个队列下正在执行的任务集合
   */
  @LimitedPrivate("yarn")
  @Stable
  public List<ApplicationAttemptId> getAppsInQueue(String queueName);

  /**
   * Get the container for the given containerId.
   * @param containerId
   * @return the container for the given containerId.
   * 给定某一个容器id,获取该容器运行的详细信息,例如运行状态等
   */
  @LimitedPrivate("yarn")
  @Unstable
  public RMContainer getRMContainer(ContainerId containerId);

  /**
   * Moves the given application to the given queue
   * @param appId
   * @param newQueue
   * @return the name of the queue the application was placed into
   * @throws YarnException if the move cannot be carried out
   * 从一个队列中移除一个任务
   */
  @LimitedPrivate("yarn")
  @Evolving
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException;

  /**
   * Completely drain sourceQueue of applications, by moving all of them to
   * destQueue.
   *
   * @param sourceQueue
   * @param destQueue
   * @throws YarnException
   * 将一个队列中任务移动到另一个队列中
   */
  void moveAllApps(String sourceQueue, String destQueue) throws YarnException;

  /**
   * Terminate all applications in the specified queue.
   *
   * @param queueName the name of queue to be drained
   * @throws YarnException
   * 终止一个队列中所有的任务
   */
  void killAllAppsInQueue(String queueName) throws YarnException;

  /**
   * Remove an existing queue. Implementations might limit when a queue could be
   * removed (e.g., must have zero entitlement, and no applications running, or
   * must be a leaf, etc..).
   *
   * @param queueName name of the queue to remove
   * @throws YarnException
   * 删除目前存在的一个队列
   */
  void removeQueue(String queueName) throws YarnException;

  /**
   * Add to the scheduler a new Queue. Implementations might limit what type of
   * queues can be dynamically added (e.g., Queue must be a leaf, must be
   * attached to existing parent, must have zero entitlement).
   *
   * @param newQueue the queue being added.
   * @throws YarnException
   * 添加一个队列
   */
  void addQueue(Queue newQueue) throws YarnException;

  /**
   * This method increase the entitlement for current queue (must respect
   * invariants, e.g., no overcommit of parents, non negative, etc.).
   * Entitlement is a general term for weights in FairScheduler, capacity for
   * the CapacityScheduler, etc.
   *
   * @param queue the queue for which we change entitlement
   * @param entitlement the new entitlement for the queue (capacity,
   *              maxCapacity, etc..)
   * @throws YarnException
   * 为某一个队列设置容量限制
   */
  void setEntitlement(String queue, QueueEntitlement entitlement)
      throws YarnException;

  /**
   * Gets the list of names for queues managed by the Reservation System
   * @return the list of queues which support reservations
   */
  public Set<String> getPlanQueues() throws YarnException;  

  /**
   * Return a collection of the resource types that are considered when
   * scheduling
   *
   * @return an EnumSet containing the resource types
   * 返回资源管理的总类型,目前只有cpu和内存两种枚举类型
   */
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes();
}

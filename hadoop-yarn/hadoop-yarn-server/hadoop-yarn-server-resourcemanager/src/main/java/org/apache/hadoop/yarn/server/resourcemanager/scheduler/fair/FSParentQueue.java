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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

@Private
@Unstable
public class FSParentQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(
      FSParentQueue.class.getName());

  private final List<FSQueue> childQueues = 
      new ArrayList<FSQueue>();
  private Resource demand = Resources.createResource(0);
  private int runnableApps;//该队列已经运行的app数量,属于该队列所有的叶子节点已经运行的app数量
  
  public FSParentQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
  }
  
  public void addChildQueue(FSQueue child) {
    childQueues.add(child);
  }

  @Override
  public void recomputeShares() {
    policy.computeShares(childQueues, getFairShare());
    for (FSQueue childQueue : childQueues) {
      childQueue.getMetrics().setFairShare(childQueue.getFairShare());
      childQueue.recomputeShares();
    }
  }

  //重新计算资源
  public void recomputeSteadyShares() {
    policy.computeSteadyShares(childQueues, getSteadyFairShare());
    for (FSQueue childQueue : childQueues) {
      childQueue.getMetrics().setSteadyFairShare(childQueue.getSteadyFairShare());
      if (childQueue instanceof FSParentQueue) {
        ((FSParentQueue) childQueue).recomputeSteadyShares();
      }
    }
  }

  @Override
  public void updatePreemptionVariables() {
    super.updatePreemptionVariables();
    // For child queues
    for (FSQueue childQueue : childQueues) {
      childQueue.updatePreemptionVariables();
    }
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  /**
   * 获取该队列已经使用的资源
   */
  @Override
  public Resource getResourceUsage() {
    Resource usage = Resources.createResource(0);
    for (FSQueue child : childQueues) {
      Resources.addTo(usage, child.getResourceUsage());
    }
    return usage;
  }

  //更新队列需要的需求资源量
  @Override
  public void updateDemand() {
	  //计算该队列下所有的app需要的资源,限制资源到最大的资源
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Resource maxRes = scheduler.getAllocationConfiguration()
        .getMaxResources(getName());//获取该队列最大的资源
    demand = Resources.createResource(0);
    for (FSQueue childQueue : childQueues) {
      childQueue.updateDemand();
      Resource toAdd = childQueue.getDemand();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Counting resource from " + childQueue.getName() + " " + 
            toAdd + "; Total resource consumption for " + getName() +
            " now " + demand);
      }
      demand = Resources.add(demand, toAdd);
      demand = Resources.componentwiseMin(demand, maxRes);
      if (Resources.equals(demand, maxRes)) {//达到上限了,则停止循环
        break;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand +
          "; the max is " + maxRes);
    }    
  }
  
  private synchronized QueueUserACLInfo getUserAclInfo(
      UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = 
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      } 
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return userAclInfo;
  }
  
  @Override
  public synchronized List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<QueueUserACLInfo>();
    
    // Add queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    for (FSQueue child : childQueues) {
      userAcls.addAll(child.getQueueUserAclInfo(user));
    }
 
    return userAcls;
  }

  //为该node分配一个容器
  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();

    // If this queue is over its limit, reject
    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    //按照队列的策略排序,然后依次分配应用到该node上
    Collections.sort(childQueues, policy.getComparator());
    for (FSQueue child : childQueues) {
      assigned = child.assignContainer(node);
      if (!Resources.equals(assigned, Resources.none())) {//说明已经分配到资源了,则返回
        break;
      }
    }
    return assigned;
  }

  //竞选一个容器
  @Override
  public RMContainer preemptContainer() {
    RMContainer toBePreempted = null;

    // Find the childQueue which is most over fair share
    //找到一个最有优势的容器
    FSQueue candidateQueue = null;
    Comparator<Schedulable> comparator = policy.getComparator();
    for (FSQueue queue : childQueues) {
      if (candidateQueue == null ||
          comparator.compare(queue, candidateQueue) > 0) {
        candidateQueue = queue;
      }
    }

    // Let the selected queue choose which of its container to preempt
    if (candidateQueue != null) {
      toBePreempted = candidateQueue.preemptContainer();
    }
    return toBePreempted;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    return childQueues;
  }

  @Override
  public void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    boolean allowed =
        SchedulingPolicy.isApplicableTo(policy, (parent == null)
            ? SchedulingPolicy.DEPTH_ROOT
            : SchedulingPolicy.DEPTH_INTERMEDIATE);
    if (!allowed) {//不允许该调度策略
      throwPolicyDoesnotApplyException(policy);//抛异常
    }
    super.policy = policy;
  }
  
  //增加该队列已经运行的app数量,属于该队列所有的叶子节点已经运行的app数量
  public void incrementRunnableApps() {
    runnableApps++;
  }
  
  //减少该队列已经运行的app数量,属于该队列所有的叶子节点已经运行的app数量
  public void decrementRunnableApps() {
    runnableApps--;
  }

  //该队列已经运行的app数量,属于该队列所有的叶子节点已经运行的app数量
  @Override
  public int getNumRunnableApps() {
    return runnableApps;
  }

  /**
   * 获取该父节点下所有的叶子节点上运行的app集合
   * 包含运行的和非运行的所有app集合,最终都存储到参数集合中
   */
  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (FSQueue childQueue : childQueues) {
      childQueue.collectSchedulerApplications(apps);
    }
  }
  
  @Override
  public ActiveUsersManager getActiveUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
    
  }
}

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

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class FSLeafQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(
      FSLeafQueue.class.getName());

  //运行的应用集合
  private final List<FSAppAttempt> runnableApps = // apps that are runnable
      new ArrayList<FSAppAttempt>();
  
  //尚未运行的应用集合
  private final List<FSAppAttempt> nonRunnableApps =
      new ArrayList<FSAppAttempt>();
  
  private Resource demand = Resources.createResource(0);
  
  // Variables used for preemption
  private long lastTimeAtMinShare;
  private long lastTimeAtFairShareThreshold;
  
  // Track the AM resource usage for this queue
  private Resource amResourceUsage;//该队列中AM使用的资源量

  private final ActiveUsersManager activeUsersManager;
  
  /**
   * 
   * @param name 该队列的名称,可能是root.aa.bb这种形式的name
   * @param scheduler 总的调度器
   * @param parent 该队列的父队列
   */
  public FSLeafQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
    this.lastTimeAtMinShare = scheduler.getClock().getTime();
    this.lastTimeAtFairShareThreshold = scheduler.getClock().getTime();
    activeUsersManager = new ActiveUsersManager(getMetrics());
    amResourceUsage = Resource.newInstance(0, 0);
  }
  
  /**
   * 一个app加入该队列
   * @param app
   * @param runnable true表示该app已经运行了
   */
  public void addApp(FSAppAttempt app, boolean runnable) {
    if (runnable) {
      runnableApps.add(app);
    } else {
      nonRunnableApps.add(app);
    }
  }
  
  // for testing
  void addAppSchedulable(FSAppAttempt appSched) {
    runnableApps.add(appSched);
  }
  
  /**
   * Removes the given app from this queue.
   * @return whether or not the app was runnable 返回该移除的app是否是运行中的
   */
  public boolean removeApp(FSAppAttempt app) {
    if (runnableApps.remove(app)) {//说明该app是运行中的,因此返回true
      // Update AM resource usage
      if (app.isAmRunning() && app.getAMResource() != null) {//减少该队列中AM的资源
        Resources.subtractFrom(amResourceUsage, app.getAMResource());
      }
      return true;
    } else if (nonRunnableApps.remove(app)) {//说明该app不是运行中的,因此返回false
      return false;
    } else {//说明不存在这个app
      throw new IllegalStateException("Given app to remove " + app +
          " does not exist in queue " + this);
    }
  }
  
  public Collection<FSAppAttempt> getRunnableAppSchedulables() {
    return runnableApps;
  }
  
  public List<FSAppAttempt> getNonRunnableAppSchedulables() {
    return nonRunnableApps;
  }
  
  /**
   * 返回该队列上所有运行的app尝试ID集合
   */
  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (FSAppAttempt appSched : runnableApps) {
      apps.add(appSched.getApplicationAttemptId());
    }
    for (FSAppAttempt appSched : nonRunnableApps) {
      apps.add(appSched.getApplicationAttemptId());
    }
  }

  @Override
  public void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    if (!SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF)) {
      throwPolicyDoesnotApplyException(policy);
    }
    super.policy = policy;
  }
  
  @Override
  public void recomputeShares() {
    policy.computeShares(getRunnableAppSchedulables(), getFairShare());
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  /**
   * 计算该队列所有使用的资源
   */
  @Override
  public Resource getResourceUsage() {
    Resource usage = Resources.createResource(0);
    for (FSAppAttempt app : runnableApps) {
      Resources.addTo(usage, app.getResourceUsage());
    }
    for (FSAppAttempt app : nonRunnableApps) {
      Resources.addTo(usage, app.getResourceUsage());
    }
    return usage;
  }

  public Resource getAmResourceUsage() {
    return amResourceUsage;
  }

  /**
   * 更新所有的app使用的资源量
   */
  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Resource maxRes = scheduler.getAllocationConfiguration()
        .getMaxResources(getName());
    demand = Resources.createResource(0);
    for (FSAppAttempt sched : runnableApps) {
      if (Resources.equals(demand, maxRes)) {
        break;
      }
      updateDemandForApp(sched, maxRes);
    }
    for (FSAppAttempt sched : nonRunnableApps) {
      if (Resources.equals(demand, maxRes)) {
        break;
      }
      updateDemandForApp(sched, maxRes);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand
          + "; the max is " + maxRes);
    }
  }
  
  /**
   * 更新app使用的资源量
   */
  private void updateDemandForApp(FSAppAttempt sched, Resource maxRes) {
    sched.updateDemand();
    Resource toAdd = sched.getDemand();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Counting resource from " + sched.getName() + " " + toAdd
          + "; Total resource consumption for " + getName() + " now "
          + demand);
    }
    demand = Resources.add(demand, toAdd);
    demand = Resources.componentwiseMin(demand, maxRes);
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node " + node.getNodeName() + " offered to queue: " + getName());
    }

    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    Comparator<Schedulable> comparator = policy.getComparator();
    Collections.sort(runnableApps, comparator);
    for (FSAppAttempt sched : runnableApps) {
      if (SchedulerAppUtils.isBlacklisted(sched, node, LOG)) {
        continue;
      }

      assigned = sched.assignContainer(node);
      if (!assigned.equals(Resources.none())) {
        break;
      }
    }
    return assigned;
  }

  @Override
  public RMContainer preemptContainer() {
    RMContainer toBePreempted = null;

    // If this queue is not over its fair share, reject
    if (!preemptContainerPreCheck()) {
      return toBePreempted;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Queue " + getName() + " is going to preempt a container " +
          "from its applications.");
    }

    // Choose the app that is most over fair share
    Comparator<Schedulable> comparator = policy.getComparator();
    FSAppAttempt candidateSched = null;
    for (FSAppAttempt sched : runnableApps) {
      if (candidateSched == null ||
          comparator.compare(sched, candidateSched) > 0) {
        candidateSched = sched;
      }
    }

    // Preempt from the selected app
    if (candidateSched != null) {
      toBePreempted = candidateSched.preemptContainer();
    }
    return toBePreempted;
  }

  //返回空的子队列
  @Override
  public List<FSQueue> getChildQueues() {
    return new ArrayList<FSQueue>(1);
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
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
    return Collections.singletonList(userAclInfo);
  }
  
  public long getLastTimeAtMinShare() {
    return lastTimeAtMinShare;
  }

  private void setLastTimeAtMinShare(long lastTimeAtMinShare) {
    this.lastTimeAtMinShare = lastTimeAtMinShare;
  }

  public long getLastTimeAtFairShareThreshold() {
    return lastTimeAtFairShareThreshold;
  }

  private void setLastTimeAtFairShareThreshold(
      long lastTimeAtFairShareThreshold) {
    this.lastTimeAtFairShareThreshold = lastTimeAtFairShareThreshold;
  }

  //返回该队列上正在运行的app数量
  @Override
  public int getNumRunnableApps() {
    return runnableApps.size();
  }
  
  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  /**
   * Check whether this queue can run this application master under the
   * maxAMShare limit
   *
   * @param amResource
   * @return true if this queue can run
   */
  public boolean canRunAppAM(Resource amResource) {
    float maxAMShare =
        scheduler.getAllocationConfiguration().getQueueMaxAMShare(getName());
    if (Math.abs(maxAMShare - -1.0f) < 0.0001) {
      return true;
    }
    Resource maxAMResource = Resources.multiply(getFairShare(), maxAMShare);
    Resource ifRunAMResource = Resources.add(amResourceUsage, amResource);
    return !policy
        .checkIfAMResourceUsageOverLimit(ifRunAMResource, maxAMResource);
  }

  //对该队列添加AM的资源
  public void addAMResourceUsage(Resource amResource) {
    if (amResource != null) {
      Resources.addTo(amResourceUsage, amResource);
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
  }

  /**
   * Update the preemption fields for the queue, i.e. the times since last was
   * at its guaranteed share and over its fair share threshold.
   */
  public void updateStarvationStats() {
    long now = scheduler.getClock().getTime();
    if (!isStarvedForMinShare()) {
      setLastTimeAtMinShare(now);
    }
    if (!isStarvedForFairShare()) {
      setLastTimeAtFairShareThreshold(now);
    }
  }

  /**
   * Helper method to check if the queue should preempt containers
   *
   * @return true if check passes (can preempt) or false otherwise
   */
  private boolean preemptContainerPreCheck() {
    return parent.getPolicy().checkIfUsageOverFairShare(getResourceUsage(),
        getFairShare());
  }

  /**
   * Is a queue being starved for its min share.
   */
  @VisibleForTesting
  boolean isStarvedForMinShare() {
    return isStarved(getMinShare());
  }

  /**
   * Is a queue being starved for its fair share threshold.
   */
  @VisibleForTesting
  boolean isStarvedForFairShare() {
    return isStarved(
        Resources.multiply(getFairShare(), getFairSharePreemptionThreshold()));
  }

  /**
   * true表示饥饿
   * @param share 表示当前队列分配的最小的资源使用量
   */
  private boolean isStarved(Resource share) {
	  //渴望的分配,min(队列最小的使用量,队列已经分配的量),如果队列最小的使用量 > 队列已经分配的量,则返回队列已经分配的量,反之,返回队列最小的使用量
    Resource desiredShare = Resources.min(FairScheduler.getResourceCalculator(),
        scheduler.getClusterResource(), share, getDemand());
    return Resources.lessThan(FairScheduler.getResourceCalculator(),
        scheduler.getClusterResource(), getResourceUsage(), desiredShare);
  }
}

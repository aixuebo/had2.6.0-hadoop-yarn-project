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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.server.utils.Lock.NoLock;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

@Private
@Unstable
public class LeafQueue extends AbstractCSQueue {
  private static final Log LOG = LogFactory.getLog(LeafQueue.class);

  private float absoluteUsedCapacity = 0.0f;
  
  private int userLimit;//userLimit和userLimitFactor用于运算maxApplicationsPerUser
  private float userLimitFactor;//userLimit和userLimitFactor用于运算maxApplicationsPerUser
  
  protected int maxApplications;//该队列最多允许的应用数量
  
  /**
   * 该队列为每个人最多允许的应用数量
   * 公式:(maxApplications * (userLimit / 100.0f) * userLimitFactor)
   * 由此可见,userLimit表示百分比,大于100是不现实的，因为比如userLimit=200,则200/100 =2,表示该一个用户可以沾满整个队列的2倍,因此应该是100以内,表示百分比,比如50,表示一个用户可以使用最多50%的应用都由一个用户占满,不过由于userLimitFactor因此也可以随意调节,正负都可以,则该userLimit值也可以是很大的值,使用的时候根据公式自己算吧
   */
  protected int maxApplicationsPerUser;
  
  
  private float maxAMResourcePerQueuePercent;//解析yarn.scheduler.capacity.$queue.maximum-am-resource-percent,默认是1.0f
  private int maxActiveApplications; // Based on absolute max capacity 该队列最多允许多少个活跃的应用,基于absolute max capacity
  private int maxActiveAppsUsingAbsCap; // Based on absolute capacity 该队列最多允许多少个活跃的应用,基于absolute capacity
  private int maxActiveApplicationsPerUser;//该队列为每一个user最多分配多少个活跃的队列
  
  //读取配置文件yarn.scheduler.capacity.node-locality-delay,默认是-1,整数
  private int nodeLocalityDelay;

  /**
   * 每一个应用实例,对应一个应用,里面包含等待的，也包含正在执行的应用
   */
  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap = new HashMap<ApplicationAttemptId, FiCaSchedulerApp>(); 
  
  Set<FiCaSchedulerApp> pendingApplications;//等候的应用集合,该队列是有顺序的,按照app的顺序排列
  Set<FiCaSchedulerApp> activeApplications;//已经执行的应用集合,该队列是有顺序的,按照app的顺序排列
  
  private final float minimumAllocationFactor;//最小分配因子,公式(maximumAllocation - minimumAllocation)/maximumAllocation

  /**
   * user的name和对应的user对象映射关系
   */
  private Map<String, User> users = new HashMap<String, User>();

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null); 

  private CapacitySchedulerContext scheduler;
  
  private final ActiveUsersManager activeUsersManager;

  // cache last cluster resource to compute actual capacity
  private Resource lastClusterResource = Resources.none();
  
  private final QueueHeadroomInfo queueHeadroomInfo = new QueueHeadroomInfo();
  
  public LeafQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;

    this.activeUsersManager = new ActiveUsersManager(metrics);
    
    //(maximumAllocation - minimumAllocation)/maximumAllocation
    this.minimumAllocationFactor = 
        Resources.ratio(resourceCalculator, 
            Resources.subtract(maximumAllocation, minimumAllocation), 
            maximumAllocation);

    //获取该队列在配置文件中配置的百分比,即配置的值/100,例如配置50,则该值为0.5
    float capacity = getCapacityFromConf();
    //真实所占用的比例
    float absoluteCapacity = parent.getAbsoluteCapacity() * capacity;

    //配置的最大值,以及最大值真实所占用的比例
    float maximumCapacity = 
        (float)cs.getConfiguration().getMaximumCapacity(getQueuePath()) / 100;
    float absoluteMaxCapacity = 
        CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);

    //整数,默认是100
    int userLimit = cs.getConfiguration().getUserLimit(getQueuePath());
    //浮点数,默认是1.0
    float userLimitFactor = 
      cs.getConfiguration().getUserLimitFactor(getQueuePath());

    //默认是-1,整数,表示最多该队列上可以存储多少个app
    int maxApplications =
        cs.getConfiguration().getMaximumApplicationsPerQueue(getQueuePath());
    if (maxApplications < 0) {//如果设置的是-1,默认值
      int maxSystemApps = cs.getConfiguration().getMaximumSystemApplications();//获取全局的最多允许多少个app
      maxApplications = (int)(maxSystemApps * absoluteCapacity);//根据absoluteCapacity的真实比例 * maxSystemApps 运算获取该队列上存储最多多少个app
    }
    
    //该队列上每一个用户最多允许分配多少个应用
    /**
     * 公式:(maxApplications * (userLimit / 100.0f) * userLimitFactor)
     * 由此可见,userLimit表示百分比,大于100是不现实的，因为比如userLimit=200,则200/100 =2,表示该一个用户可以沾满整个队列的2倍,因此应该是100以内,表示百分比,比如50,表示一个用户可以使用最多50%的应用都由一个用户占满,不过由于userLimitFactor因此也可以随意调节,正负都可以,则该userLimit值也可以是很大的值,使用的时候根据公式自己算吧
     */
    maxApplicationsPerUser = 
      (int)(maxApplications * (userLimit / 100.0f) * userLimitFactor);

    
    //解析yarn.scheduler.capacity.$queue.maximum-am-resource-percent,默认是1.0f
    float maxAMResourcePerQueuePercent = cs.getConfiguration()
        .getMaximumApplicationMasterResourcePerQueuePercent(getQueuePath());
    
    /**
     * 公式 (getClusterResource/minimumAllocation) * absoluteMaxCapacity * maxAMResourcePerQueuePercent
     * 其中(getClusterResource/minimumAllocation) * absoluteMaxCapacity 表示当前队列可以允许多少个最小资源
     * 获取该队列最多允许多少个活跃的应用
     */
    int maxActiveApplications = 
        CSQueueUtils.computeMaxActiveApplications(
            resourceCalculator,
            cs.getClusterResource(), this.minimumAllocation,
            maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    
    /**
     * 公式 (getClusterResource/minimumAllocation) * absoluteCapacity * maxAMResourcePerQueuePercent
     * 其中(getClusterResource/minimumAllocation) * absoluteMaxCapacity 表示当前队列可以允许多少个最小资源
     * 获取该队列最多允许多少个活跃的应用
     */
    this.maxActiveAppsUsingAbsCap = 
            CSQueueUtils.computeMaxActiveApplications(
                resourceCalculator,
                cs.getClusterResource(), this.minimumAllocation,
                maxAMResourcePerQueuePercent, absoluteCapacity);
    
    //计算在该队列上每一个用户最多可以有多少个活跃的app
    int maxActiveApplicationsPerUser =
        CSQueueUtils.computeMaxActiveApplicationsPerUser(
            maxActiveAppsUsingAbsCap, userLimit, userLimitFactor);

    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());

    QueueState state = cs.getConfiguration().getState(getQueuePath());

    Map<QueueACL, AccessControlList> acls = 
      cs.getConfiguration().getAcls(getQueuePath());

    setupQueueConfigs(cs.getClusterResource(), capacity, absoluteCapacity,
        maximumCapacity, absoluteMaxCapacity, userLimit, userLimitFactor,
        maxApplications, maxAMResourcePerQueuePercent, maxApplicationsPerUser,
        maxActiveApplications, maxActiveApplicationsPerUser, state, acls, cs
            .getConfiguration().getNodeLocalityDelay(), accessibleLabels,
        defaultLabelExpression, this.capacitiyByNodeLabels,
        this.maxCapacityByNodeLabels,
        cs.getConfiguration().getReservationContinueLook());

    if(LOG.isDebugEnabled()) {
      LOG.debug("LeafQueue:" + " name=" + queueName
        + ", fullname=" + getQueuePath());
    }

    //设置app的队列顺序
    Comparator<FiCaSchedulerApp> applicationComparator =
        cs.getApplicationComparator();
    this.pendingApplications = 
        new TreeSet<FiCaSchedulerApp>(applicationComparator);
    
    
    this.activeApplications = new TreeSet<FiCaSchedulerApp>(applicationComparator);
  }
  
  // externalizing in method, to allow overriding
  //获取该队列在配置文件中配置的百分比,即配置的值/100,例如配置50,则该值为0.5
  protected float getCapacityFromConf() {
    return (float)scheduler.getConfiguration().getCapacity(getQueuePath()) / 100;
  }

  protected synchronized void setupQueueConfigs(
      Resource clusterResource,
      float capacity, float absoluteCapacity, 
      float maximumCapacity, float absoluteMaxCapacity,
      int userLimit, float userLimitFactor,
      int maxApplications, float maxAMResourcePerQueuePercent,
      int maxApplicationsPerUser, int maxActiveApplications,
      int maxActiveApplicationsPerUser, QueueState state,
      Map<QueueACL, AccessControlList> acls, int nodeLocalityDelay,
      Set<String> labels, String defaultLabelExpression,
      Map<String, Float> capacitieByLabel,
      Map<String, Float> maximumCapacitiesByLabel, 
      boolean revervationContinueLooking) throws IOException {
    super.setupQueueConfigs(clusterResource, capacity, absoluteCapacity,
        maximumCapacity, absoluteMaxCapacity, state, acls, labels,
        defaultLabelExpression, capacitieByLabel, maximumCapacitiesByLabel,
        revervationContinueLooking);
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absCapacity = getParent().getAbsoluteCapacity() * capacity;
    CSQueueUtils.checkAbsoluteCapacity(getQueueName(), absCapacity,
        absoluteMaxCapacity);

    this.absoluteCapacity = absCapacity;

    this.userLimit = userLimit;
    this.userLimitFactor = userLimitFactor;

    this.maxApplications = maxApplications;
    this.maxAMResourcePerQueuePercent = maxAMResourcePerQueuePercent;
    this.maxApplicationsPerUser = maxApplicationsPerUser;

    this.maxActiveApplications = maxActiveApplications;
    this.maxActiveApplicationsPerUser = maxActiveApplicationsPerUser;

    if (!SchedulerUtils.checkQueueLabelExpression(this.accessibleLabels,
        this.defaultLabelExpression)) {
      throw new IOException("Invalid default label expression of "
          + " queue="
          + queueInfo.getQueueName()
          + " doesn't have permission to access all labels "
          + "in default label expression. labelExpression of resource request="
          + (this.defaultLabelExpression == null ? ""
              : this.defaultLabelExpression)
          + ". Queue labels="
          + (queueInfo.getAccessibleNodeLabels() == null ? "" : StringUtils.join(queueInfo
              .getAccessibleNodeLabels().iterator(), ',')));
    }
    
    this.nodeLocalityDelay = nodeLocalityDelay;

    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }

    StringBuilder labelStrBuilder = new StringBuilder(); 
    if (labels != null) {
      for (String s : labels) {
        labelStrBuilder.append(s);
        labelStrBuilder.append(",");
      }
    }

    LOG.info("Initializing " + queueName + "\n" +
        "capacity = " + capacity +
        " [= (float) configuredCapacity / 100 ]" + "\n" + 
        "asboluteCapacity = " + absoluteCapacity +
        " [= parentAbsoluteCapacity * capacity ]" + "\n" +
        "maxCapacity = " + maximumCapacity +
        " [= configuredMaxCapacity ]" + "\n" +
        "absoluteMaxCapacity = " + absoluteMaxCapacity +
        " [= 1.0 maximumCapacity undefined, " +
        "(parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ]" + 
        "\n" +
        "userLimit = " + userLimit +
        " [= configuredUserLimit ]" + "\n" +
        "userLimitFactor = " + userLimitFactor +
        " [= configuredUserLimitFactor ]" + "\n" +
        "maxApplications = " + maxApplications +
        " [= configuredMaximumSystemApplicationsPerQueue or" + 
        " (int)(configuredMaximumSystemApplications * absoluteCapacity)]" + 
        "\n" +
        "maxApplicationsPerUser = " + maxApplicationsPerUser +
        " [= (int)(maxApplications * (userLimit / 100.0f) * " +
        "userLimitFactor) ]" + "\n" +
        "maxActiveApplications = " + maxActiveApplications +
        " [= max(" + 
        "(int)ceil((clusterResourceMemory / minimumAllocation) * " + 
        "maxAMResourcePerQueuePercent * absoluteMaxCapacity)," + 
        "1) ]" + "\n" +
        "maxActiveAppsUsingAbsCap = " + maxActiveAppsUsingAbsCap +
        " [= max(" + 
        "(int)ceil((clusterResourceMemory / minimumAllocation) *" + 
        "maxAMResourcePercent * absoluteCapacity)," + 
        "1) ]" + "\n" +
        "maxActiveApplicationsPerUser = " + maxActiveApplicationsPerUser +
        " [= max(" +
        "(int)(maxActiveApplications * (userLimit / 100.0f) * " +
        "userLimitFactor)," +
        "1) ]" + "\n" +
        "usedCapacity = " + usedCapacity +
        " [= usedResourcesMemory / " +
        "(clusterResourceMemory * absoluteCapacity)]" + "\n" +
        "absoluteUsedCapacity = " + absoluteUsedCapacity +
        " [= usedResourcesMemory / clusterResourceMemory]" + "\n" +
        "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent +
        " [= configuredMaximumAMResourcePercent ]" + "\n" +
        "minimumAllocationFactor = " + minimumAllocationFactor +
        " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / " +
        "maximumAllocationMemory ]" + "\n" +
        "numContainers = " + numContainers +
        " [= currentNumContainers ]" + "\n" +
        "state = " + state +
        " [= configuredState ]" + "\n" +
        "acls = " + aclsString +
        " [= configuredAcls ]" + "\n" + 
        "nodeLocalityDelay = " + nodeLocalityDelay + "\n" +
        "labels=" + labelStrBuilder.toString() + "\n" +
        "nodeLocalityDelay = " +  nodeLocalityDelay + "\n" +
        "reservationsContinueLooking = " +
        reservationsContinueLooking + "\n");
  }

  @Override
  public String getQueuePath() {
    return getParent().getQueuePath() + "." + getQueueName();
  }

  /**
   * Used only by tests.
   */
  @Private
  public float getMinimumAllocationFactor() {
    return minimumAllocationFactor;
  }
  
  /**
   * Used only by tests.
   */
  @Private
  public float getMaxAMResourcePerQueuePercent() {
    return maxAMResourcePerQueuePercent;
  }

  public int getMaxApplications() {
    return maxApplications;
  }

  public synchronized int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  public synchronized int getMaximumActiveApplications() {
    return maxActiveApplications;
  }

  public synchronized int getMaximumActiveApplicationsPerUser() {
    return maxActiveApplicationsPerUser;
  }

  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  @Override
  public List<CSQueue> getChildQueues() {
    return null;
  }
  
  /**
   * Set user limit - used only for testing.
   * @param userLimit new user limit
   */
  synchronized void setUserLimit(int userLimit) {
    this.userLimit = userLimit;
  }

  /**
   * Set user limit factor - used only for testing.
   * @param userLimitFactor new user limit factor
   */
  synchronized void setUserLimitFactor(float userLimitFactor) {
    this.userLimitFactor = userLimitFactor;
  }

  @Override
  public synchronized int getNumApplications() {
    return getNumPendingApplications() + getNumActiveApplications();
  }

  public synchronized int getNumPendingApplications() {
    return pendingApplications.size();
  }

  public synchronized int getNumActiveApplications() {
    return activeApplications.size();
  }

  @Private
  public synchronized int getNumApplications(String user) {
    return getUser(user).getTotalApplications();
  }

  @Private
  public synchronized int getNumPendingApplications(String user) {
    return getUser(user).getPendingApplications();
  }

  @Private
  public synchronized int getNumActiveApplications(String user) {
    return getUser(user).getActiveApplications();
  }
  
  public synchronized int getNumContainers() {
    return numContainers;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }

  @Private
  public synchronized int getUserLimit() {
    return userLimit;
  }

  @Private
  public synchronized float getUserLimitFactor() {
    return userLimitFactor;
  }

  @Override
  public synchronized QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    queueInfo.setCurrentCapacity(usedCapacity);
    return queueInfo;
  }

  @Override
  public synchronized List<QueueUserACLInfo> 
  getQueueUserAclInfo(UserGroupInformation user) {
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

  @Private
  public int getNodeLocalityDelay() {
    return nodeLocalityDelay;
  }
  
  public String toString() {
    return queueName + ": " + 
        "capacity=" + capacity + ", " + 
        "absoluteCapacity=" + absoluteCapacity + ", " + 
        "usedResources=" + usedResources +  ", " +
        "usedCapacity=" + getUsedCapacity() + ", " + 
        "absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + ", " +
        "numApps=" + getNumApplications() + ", " + 
        "numContainers=" + getNumContainers();  
  }
  
  @VisibleForTesting
  public synchronized void setNodeLabelManager(RMNodeLabelsManager mgr) {
    this.labelManager = mgr;
  }

  @VisibleForTesting
  public synchronized User getUser(String userName) {
    User user = users.get(userName);
    if (user == null) {
      user = new User();
      users.put(userName, user);
    }
    return user;
  }

  /**
   * @return an ArrayList of UserInfo objects who are active in this queue
   */
  public synchronized ArrayList<UserInfo> getUsers() {
    ArrayList<UserInfo> usersToReturn = new ArrayList<UserInfo>();
    for (Map.Entry<String, User> entry: users.entrySet()) {
      usersToReturn.add(new UserInfo(entry.getKey(), Resources.clone(
        entry.getValue().consumed), entry.getValue().getActiveApplications(),
        entry.getValue().getPendingApplications()));
    }
    return usersToReturn;
  }

  @Override
  public synchronized void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
  throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof LeafQueue) || 
        !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() + 
          " from " + newlyParsedQueue.getQueuePath());
    }

    LeafQueue newlyParsedLeafQueue = (LeafQueue)newlyParsedQueue;
    setupQueueConfigs(
        clusterResource,
        newlyParsedLeafQueue.capacity, newlyParsedLeafQueue.absoluteCapacity, 
        newlyParsedLeafQueue.maximumCapacity, 
        newlyParsedLeafQueue.absoluteMaxCapacity, 
        newlyParsedLeafQueue.userLimit, newlyParsedLeafQueue.userLimitFactor, 
        newlyParsedLeafQueue.maxApplications,
        newlyParsedLeafQueue.maxAMResourcePerQueuePercent,
        newlyParsedLeafQueue.getMaxApplicationsPerUser(),
        newlyParsedLeafQueue.getMaximumActiveApplications(), 
        newlyParsedLeafQueue.getMaximumActiveApplicationsPerUser(),
        newlyParsedLeafQueue.state, newlyParsedLeafQueue.acls,
        newlyParsedLeafQueue.getNodeLocalityDelay(),
        newlyParsedLeafQueue.accessibleLabels,
        newlyParsedLeafQueue.defaultLabelExpression,
        newlyParsedLeafQueue.capacitiyByNodeLabels,
        newlyParsedLeafQueue.maxCapacityByNodeLabels,
        newlyParsedLeafQueue.reservationsContinueLooking);

    // queue metrics are updated, more resource may be available
    // activate the pending applications if possible
    activateApplications();
  }

  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,String userName) {
    // Careful! Locking order is important!
    synchronized (this) {
      User user = getUser(userName);
      // Add the attempt to our data-structures
      addApplicationAttempt(application, user);
    }

    // We don't want to update metrics for move app
    if (application.isPending()) {
      metrics.submitAppAttempt(userName);
    }
    getParent().submitApplicationAttempt(application, userName);
  }

  //就是一顿校验
  @Override
  public void submitApplication(ApplicationId applicationId, String userName,String queue)  throws AccessControlException {
    // Careful! Locking order is important!

    // Check queue ACLs 权限必须通过
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(userName);
    if (!hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)
        && !hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
      throw new AccessControlException("User " + userName + " cannot submit" +
          " applications to queue " + getQueuePath());
    }

    User user = null;
    synchronized (this) {

      //该队列状态必须是运行中的
      // Check if the queue is accepting jobs
      if (getState() != QueueState.RUNNING) {
        String msg = "Queue " + getQueuePath() +
        " is STOPPED. Cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for queues 该队列运行的app数量必须充足
      if (getNumApplications() >= getMaxApplications()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + getNumApplications() + " applications," +
        " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for the user on this queue 确保该用户运行的app数量必须充足
      user = getUser(userName);
      if (user.getTotalApplications() >= getMaxApplicationsPerUser()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + user.getTotalApplications() + 
        " applications from user " + userName + 
        " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }
    }

    // Inform the parent queue
    try {
      getParent().submitApplication(applicationId, userName, queue);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application to parent-queue: " + 
          getParent().getQueuePath(), ace);
      throw ace;
    }

  }

  /**
   * 从等待队列中执行新的应用
   */
  private synchronized void activateApplications() {
	  //按照顺序,从等待队列中获取一个任务去激活
    for (Iterator<FiCaSchedulerApp> i=pendingApplications.iterator(); i.hasNext(); ) {
      FiCaSchedulerApp application = i.next();
      
      // Check queue limit 只要队列空间还足够,就激活一个等待的任务
      if (getNumActiveApplications() >= getMaximumActiveApplications()) {
        break;
      }
      
      // Check user limit
      User user = getUser(application.getUser());
      if (user.getActiveApplications() < getMaximumActiveApplicationsPerUser()) {//用户有足够的资源可以允许该任务的话
        user.activateApplication();
        activeApplications.add(application);//将任务添加到激活任务中
        i.remove();//从等待任务中移除
        LOG.info("Application " + application.getApplicationId() +
            " from user: " + application.getUser() + 
            " activated in queue: " + getQueueName());
      }
    }
  }
  
  /**
   * 为该user添加一个尝试任务
   */
  private synchronized void addApplicationAttempt(FiCaSchedulerApp application,User user) {
    // Accept 
    user.submitApplication();//添加一个等待任务
    pendingApplications.add(application);
    applicationAttemptMap.put(application.getApplicationAttemptId(), application);

    // Activate applications
    activateApplications();
    
    LOG.info("Application added -" +
        " appId: " + application.getApplicationId() +
        " user: " + user + "," + " leaf-queue: " + getQueueName() +
        " #user-pending-applications: " + user.getPendingApplications() +
        " #user-active-applications: " + user.getActiveApplications() +
        " #queue-pending-applications: " + getNumPendingApplications() +
        " #queue-active-applications: " + getNumActiveApplications()
        );
  }

  @Override
  public void finishApplication(ApplicationId application, String user) {
    // Inform the activeUsersManager
    activeUsersManager.deactivateApplication(user, application);
    // Inform the parent queue
    getParent().finishApplication(application, user);
  }

  @Override
  public void finishApplicationAttempt(FiCaSchedulerApp application, String queue) {
    // Careful! Locking order is important!
    synchronized (this) {
      removeApplicationAttempt(application, getUser(application.getUser()));
    }
    getParent().finishApplicationAttempt(application, queue);
  }

  public synchronized void removeApplicationAttempt(FiCaSchedulerApp application, User user) {
    boolean wasActive = activeApplications.remove(application);
    if (!wasActive) {
      pendingApplications.remove(application);
    }
    applicationAttemptMap.remove(application.getApplicationAttemptId());

    user.finishApplication(wasActive);
    if (user.getTotalApplications() == 0) {
      users.remove(application.getUser());
    }

    // Check if we can activate more applications
    activateApplications();

    LOG.info("Application removed -" +
        " appId: " + application.getApplicationId() + 
        " user: " + application.getUser() + 
        " queue: " + getQueueName() +
        " #user-pending-applications: " + user.getPendingApplications() +
        " #user-active-applications: " + user.getActiveApplications() +
        " #queue-pending-applications: " + getNumPendingApplications() +
        " #queue-active-applications: " + getNumActiveApplications()
        );
  }

  private synchronized FiCaSchedulerApp getApplication(ApplicationAttemptId applicationAttemptId) {
    return applicationAttemptMap.get(applicationAttemptId);
  }

  //叶子队列分配一个空的资源信息,即没有在该队列上找到匹配的容器
  private static final CSAssignment NULL_ASSIGNMENT = new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
  
  private static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);
  
  /**
   * labelExpression按照&&分割拆分,获得标签集合
   */
  private static Set<String> getRequestLabelSetByExpression(String labelExpression) {
    Set<String> labels = new HashSet<String>();
    if (null == labelExpression) {
      return labels;
    }
    for (String l : labelExpression.split("&&")) {
      if (l.trim().isEmpty()) {
        continue;
      }
      labels.add(l.trim());
    }
    return labels;
  }
  
  /**
   * 为该node分配一个容器
   */
  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,FiCaSchedulerNode node, boolean needToUnreserve) {

    if(LOG.isDebugEnabled()) {
    	//打印日志,要在node上分配一个容器,该容器时从activeApplications这些个激活的应用中选择,为某一个应用提供一个容器
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " #applications=" + activeApplications.size());
    }
    
    // if our queue cannot access this node, just return检查是否可以访问该队列
    if (!SchedulerUtils.checkQueueAccessToNode(accessibleLabels,
        labelManager.getLabelsOnNode(node.getNodeID()))) {//如果该node节点的标签与该队列标签不符合,则返回一个空的分配结果
      return NULL_ASSIGNMENT;
    }
    
    // Check for reserved resources
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp application = getApplication(reservedContainer.getApplicationAttemptId()); 
      synchronized (application) {
        return assignReservedContainer(application, node, reservedContainer,clusterResource);
      }
    }
    
    // Try to assign containers to applications in order
    for (FiCaSchedulerApp application : activeApplications) {//按照顺序循环每一个应用
      
      if(LOG.isDebugEnabled()) {
    	 //打印日志,预先要为该应用分配一个容器
        LOG.debug("pre-assignContainers for application "
        + application.getApplicationId());
        application.showRequests();
      }

      synchronized (application) {
        // Check if this resource is on the blacklist查看是否是黑名单节点,如果是则continue
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {//该应用是否在该node上是黑名单,因此继续到下一个应用
          continue;
        }
        
        // Schedule in priority order
        for (Priority priority : application.getPriorities()) {//按照优先级,为该应用分配容器
          ResourceRequest anyRequest = application.getResourceRequest(priority, ResourceRequest.ANY);
          if (null == anyRequest) {
            continue;
          }
          
          // Required resource 请求的资源
          Resource required = anyRequest.getCapability();

          // Do we need containers at this 'priority'?
          if (application.getTotalRequiredResources(priority) <= 0) {//说明该优先级上已经没有容器需要了,则继续寻找下一个优先级
            continue;
          }
          if (!this.reservationsContinueLooking) {
            if (!needContainers(application, priority, required)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("doesn't need containers based on reservation algo!");
              }
              continue;
            }
          }
          
          //获取请求的标签集合
          Set<String> requestedNodeLabels =
              getRequestLabelSetByExpression(anyRequest
                  .getNodeLabelExpression());

          // Compute user-limit & set headroom
          // Note: We compute both user-limit & headroom with the highest 
          //       priority request as the target. 
          //       This works since we never assign lower priority requests
          //       before all higher priority ones are serviced.
          //计算user-limit和headroom的目的是为了更高级别的优先级请求
          //所有的比更高级别的请求服务完前,是绝对不会服务低级别的请求的
          //计算该app所属的user,他的允许使用资源的上限
          Resource userLimit = 
              computeUserLimitAndSetHeadroom(application, clusterResource, 
                  required, requestedNodeLabels);          
          
          // Check queue max-capacity limit
          //仅仅循环每一个标签,查看标签的使用资源占比是否满足条件,如果有任意一个标签使用占比超量,则返回false
          if (!canAssignToThisQueue(clusterResource, required,
              labelManager.getLabelsOnNode(node.getNodeID()), application, true)) {//因为该node节点上任意一个label已经达到上限了,因此不会进行分配,也不会继续循环下一个app应用了,直接返回空对象即可
            return NULL_ASSIGNMENT;
          }

          // Check user limit 比较该label在该user上消费的资源,如果该资源比limit大,则返回false
          if (!assignToUser(clusterResource, application.getUser(), userLimit,
              application, true, requestedNodeLabels)) {
            break;//仅仅是跳出这个应用,继续获取下一个应用
          }

          // Inform the application it is about to get a scheduling opportunity
          //通知这个应用,他有一个调度的机会
          application.addSchedulingOpportunity(priority);
          
          // Try to schedule 去调度,让该应用的priority优先级上的请求在该node上去调度,返回在该node上分配的容器资源详细情况
          CSAssignment assignment =  
            assignContainersOnNode(clusterResource, node, application, priority, 
                null, needToUnreserve);

          // Did the application skip this node?让这个应用跳过该node,则继续循环下一个优先级
          if (assignment.getSkipped()) {
            // Don't count 'skipped nodes' as a scheduling opportunity!通知应用,减少他的一个调度机会
            application.subtractSchedulingOpportunity(priority);
            continue;
          }
          
          // Did we schedule or reserve a container?
          Resource assigned = assignment.getResource();//获取准备调度的资源情况
          if (Resources.greaterThan(
              resourceCalculator, clusterResource, assigned, Resources.none())) {//发现资源大于none,则说明有资源被分配了

            // Book-keeping 
            // Note: Update headroom to account for current allocation too...更新资源使用情况
            allocateResource(clusterResource, application, assigned,
                labelManager.getLabelsOnNode(node.getNodeID()));
            
            // Don't reset scheduling opportunities for non-local assignments
            // otherwise the app will be delayed for each non-local assignment.
            // This helps apps with many off-cluster requests schedule faster.
            //重置调度机会,会请求更快的被调度,但是没看懂逻辑,以后要再详细看一下
            if (assignment.getType() != NodeType.OFF_SWITCH) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Resetting scheduling opportunities");
              }
              application.resetSchedulingOpportunities(priority);
            }
            
            // Done
            return assignment;//返回分配的容器信息
          } else {//换另外一个app继续查找,该app是不能再满足需求了
            // Do not assign out of order w.r.t priorities
            break;
          }
        }
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("post-assignContainers for application "
          + application.getApplicationId());
      }
      application.showRequests();
    }
  
    return NULL_ASSIGNMENT;

  }

  private synchronized CSAssignment assignReservedContainer(FiCaSchedulerApp application,FiCaSchedulerNode node, RMContainer rmContainer, Resource clusterResource) {
    // Do we still need this reservation?
    Priority priority = rmContainer.getReservedPriority();
    if (application.getTotalRequiredResources(priority) == 0) {
      // Release
      return new CSAssignment(application, rmContainer);
    }

    // Try to assign if we have sufficient resources
    assignContainersOnNode(clusterResource, node, application, priority, 
        rmContainer, false);
    
    // Doesn't matter... since it's already charged for at time of reservation
    // "re-reservation" is *free*
    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }
  
  protected Resource getHeadroom(User user, Resource queueMaxCap,
      Resource clusterResource, FiCaSchedulerApp application, Resource required) {
    return getHeadroom(user, queueMaxCap, clusterResource,
	  computeUserLimit(application, clusterResource, required, user, null));
  }
  
  /**
   * @param user 用户占用资源的详细信息对象
   * @param queueMaxCap 该队列的最大资源使用量
   * @param clusterResource 集群资源
   * @param userLimit 计算在该队列上该user最多允许多少资源,如果label有值,则要加上label做限制条件
   * @return 返回还能使用多少资源
   */
  private Resource getHeadroom(User user, Resource queueMaxCap,
      Resource clusterResource, Resource userLimit) {
    /** 
     * Headroom is:
     *    min(
     *        min(userLimit, queueMaxCap) - userConsumed,
     *        queueMaxCap - queueUsedResources
     *       )
     * 
     * ( which can be expressed as, 
     *  min (userLimit - userConsumed, queuMaxCap - userConsumed, 
     *    queueMaxCap - queueUsedResources)
     *  )
     *
     * given that queueUsedResources >= userConsumed, this simplifies to
     *
     * >> min (userlimit - userConsumed,   queueMaxCap - queueUsedResources) << 
     *
     */
    Resource headroom = 
      Resources.min(resourceCalculator, clusterResource,
        Resources.subtract(userLimit, user.getTotalConsumedResources()),//user资源最大限制-user已经使用量 = user剩余资源量
        Resources.subtract(queueMaxCap, usedResources)//该队列最大使用量-已经使用量 = 还剩余能用多少使用量
        );
    return headroom;
  }

  /**
   * @param clusterResource 集群资源
   * @param required 该容器准备请求的资源
   * @param nodeLabels 该node节点能满足的标签
   * @param application 该请求的资源属于什么app应用的
   * 仅仅循环每一个标签,查看标签的使用资源占比是否满足条件,如果有任意一个标签使用占比超量,则返回false
   */
  synchronized boolean canAssignToThisQueue(Resource clusterResource,
      Resource required, Set<String> nodeLabels, FiCaSchedulerApp application, 
      boolean checkReservations) {
    // Get label of this queue can access, it's (nodeLabel AND queueLabel)
    //获取该队列和该node的标签交集,app应用必须满足这个标签交集
    Set<String> labelCanAccess;
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      labelCanAccess = new HashSet<String>();
      // Any queue can always access any node without label
      labelCanAccess.add(RMNodeLabelsManager.NO_LABEL);
    } else {
      labelCanAccess = new HashSet<String>(Sets.intersection(accessibleLabels, nodeLabels));
    }
    
    boolean canAssign = true;
    for (String label : labelCanAccess) {
      if (!usedResourcesByNodeLabels.containsKey(label)) {
        usedResourcesByNodeLabels.put(label, Resources.createResource(0));
      }
      
      //该标签可能在该队列上存在的资源
      Resource potentialTotalCapacity = Resources.add(usedResourcesByNodeLabels.get(label), required);
      
      /**
       * 该标签可能获取资源的占比
       */
      float potentialNewCapacity =
          Resources.divide(resourceCalculator, clusterResource,
              potentialTotalCapacity,
              labelManager.getResourceByLabel(label, clusterResource));
      // if enabled, check to see if could we potentially use this node instead
      // of a reserved node if the application has reserved containers
      // TODO, now only consider reservation cases when the node has no label
      if (this.reservationsContinueLooking && checkReservations
          && label.equals(RMNodeLabelsManager.NO_LABEL)) {
        float potentialNewWithoutReservedCapacity = Resources.divide(
            resourceCalculator,
            clusterResource,
            Resources.subtract(potentialTotalCapacity,
               application.getCurrentReservation()),
            labelManager.getResourceByLabel(label, clusterResource));

        if (potentialNewWithoutReservedCapacity <= absoluteMaxCapacity) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("try to use reserved: "
                + getQueueName()
                + " usedResources: "
                + usedResources
                + " clusterResources: "
                + clusterResource
                + " reservedResources: "
                + application.getCurrentReservation()
                + " currentCapacity "
                + Resources.divide(resourceCalculator, clusterResource,
                    usedResources, clusterResource) + " required " + required
                + " potentialNewWithoutReservedCapacity: "
                + potentialNewWithoutReservedCapacity + " ( " + " max-capacity: "
                + absoluteMaxCapacity + ")");
          }
          // we could potentially use this node instead of reserved node
          return true;
        }
      }
      
      // Otherwise, if any of the label of this node beyond queue limit, we
      // cannot allocate on this node. Consider a small epsilon here.
      //如果超过了该标签最大允许的资源,则返回false
      if (potentialNewCapacity > getAbsoluteMaximumCapacityByNodeLabel(label) + 1e-4) {
        canAssign = false;
        break;
      }

      //打印日志,校验分配到队列的label标签,
      if (LOG.isDebugEnabled()) {
        LOG.debug(getQueueName()
            + "Check assign to queue, label=" + label //标签名称
            + " usedResources: " + usedResourcesByNodeLabels.get(label)//该标签已经使用的资源
            + " clusterResources: " + clusterResource//集群资源
            + " currentCapacity "
            + Resources.divide(resourceCalculator, clusterResource,
                usedResourcesByNodeLabels.get(label),
                labelManager.getResourceByLabel(label, clusterResource))//该标签的使用占比
            + " potentialNewCapacity: " + potentialNewCapacity + " ( "
            + " max-capacity: " + absoluteMaxCapacity + ")");//最大占比
      }
    }
    
    return canAssign;
  }

  /**
   * 
   * @param application app应用
   * @param clusterResource 集群资源
   * @param required 该app应用的每一个容器所需要的资源情况
   * @param requestedLabels 该app应用的每一个容器请求需要的标签集合
   * @return
   * 计算该app所属的user,返回他的允许使用资源的上限,并且设置app的Headroom属性
   */
  @Lock({LeafQueue.class, FiCaSchedulerApp.class})
  Resource computeUserLimitAndSetHeadroom(FiCaSchedulerApp application,
      Resource clusterResource, Resource required, Set<String> requestedLabels) {
	  
	//找到对应的user对象
    String user = application.getUser();
    User queueUser = getUser(user);

    // Compute user limit respect requested labels,
    //计算每一个用户的限制,并且尊重有请求中标签的情况,即有标签要特殊处理
    // TODO, need consider headroom respect labels also headroom也需要特殊尊重有标签的请求
    //计算在该队列上该user最多允许多少资源,如果label有值,则要加上label做限制条件
    Resource userLimit =
        computeUserLimit(application, clusterResource, required,
            queueUser, requestedLabels);

    //Max avail capacity needs to take into account usage by ancestor-siblings
    //which are greater than their base capacity, so we are interested in "max avail"
    //capacity
    //返回该队列可用的资源使用量绝对占比
    float absoluteMaxAvailCapacity = CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, this);

    //集群资源 * 该队列可用的资源使用量绝对占比 = 该队列可以使用的资源最大量
    Resource queueMaxCap =                        // Queue Max-Capacity
        Resources.multiplyAndNormalizeDown(
            resourceCalculator, 
            clusterResource, 
            absoluteMaxAvailCapacity,
            minimumAllocation);
	
    //设置队列的全局信息
    synchronized (queueHeadroomInfo) {
      queueHeadroomInfo.setQueueMaxCap(queueMaxCap);
      queueHeadroomInfo.setClusterResource(clusterResource);
    }
    
    Resource headroom =
        getHeadroom(queueUser, queueMaxCap, clusterResource, userLimit);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for user " + user + ": " + 
          " userLimit=" + userLimit + //该用户在该队列使用的资源最高上限
          " queueMaxCap=" + queueMaxCap + 
          " consumed=" + queueUser.getTotalConsumedResources() + //该用户使用的总资源 
          " headroom=" + headroom);
    }
    
    CapacityHeadroomProvider headroomProvider = new CapacityHeadroomProvider(
      queueUser, this, application, required, queueHeadroomInfo);
    
    application.setHeadroomProvider(headroomProvider);

    metrics.setAvailableResourcesToUser(user, headroom);
    
    return userLimit;
  }
  
  /**
   * @param application app应用
   * @param clusterResource 集群资源
   * @param required 该app应用的每一个容器所需要的资源情况
   * @param user 该app应用的user对应的User对象
   * @param requestedLabels 该app应用的每一个容器请求需要的标签集合
   * @return 计算在该队列上该user最多允许多少资源,如果label有值,则要加上label做限制条件
   */
  @Lock(NoLock.class)
  private Resource computeUserLimit(FiCaSchedulerApp application,
      Resource clusterResource, Resource required, User user,
      Set<String> requestedLabels) {
    // What is our current capacity? 什么是当前的容量
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)
    Resource queueCapacity = Resource.newInstance(0, 0);//表示该label在该队列上的资源使用量上限
    if (requestedLabels != null && !requestedLabels.isEmpty()) {
      // if we have multiple labels to request, we will choose to use the first
      // label 如果有多个label请求,则我们会选择第一个
      String firstLabel = requestedLabels.iterator().next();
      /**
       * 
       * labelManager.getResourceByLabel(firstLabel,clusterResource) 获取该label对应的集群存储的资源
       * getAbsoluteCapacityByNodeLabel(firstLabel) 返回配置文件中配置的该队列对该标签对应的绝对容量信息
       * minimumAllocation最小的资源配置
       * 
       * 因此:
       * Resources.multiplyAndNormalizeUp,返回的是label在总资源*该队列的label占比 = label在该队列上的资源最多使用量
       */
      queueCapacity =
          Resources
              .max(resourceCalculator, clusterResource, queueCapacity,
                  Resources.multiplyAndNormalizeUp(resourceCalculator,
                      labelManager.getResourceByLabel(firstLabel,
                          clusterResource),
                      getAbsoluteCapacityByNodeLabel(firstLabel),
                      minimumAllocation));
    } else {//没有label请求,则我们使用绝对容量运算
      // else there's no label on request, just to use absolute capacity as
      // capacity for nodes without label
      //返回的是总资源的*该队列的占比 = 在该队列上的资源最多使用量
      queueCapacity =
          Resources.multiplyAndNormalizeUp(resourceCalculator, labelManager
                .getResourceByLabel(CommonNodeLabelsManager.NO_LABEL, clusterResource),
              absoluteCapacity, minimumAllocation);
    }

    // Allow progress for queues with miniscule capacity
    queueCapacity =
        Resources.max(
            resourceCalculator, clusterResource, 
            queueCapacity, 
            required);

    /**
     * 返回添加该资源后的总资源容量
     * queueCapacity>usedResources,返回queueCapacity,因为总容量够,则返回总容量即可
     * queueCapacity<=usedResources,返回Resources.add(usedResources, required),因为总容量不够,则要添加容量
     */
    Resource currentCapacity =
        Resources.lessThan(resourceCalculator, clusterResource, 
            usedResources, queueCapacity) ?
            queueCapacity : Resources.add(usedResources, required);
    
    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    //当前活跃用户数
    final int activeUsers = activeUsersManager.getNumActiveUsers();  
    		
    /**
     * 简单的公式算一下:
     * 1.currentCapacity/activeUsers,表示当前活跃用户数平均每个用户占用容量
     * 2.currentCapacity*userLimit,表示根据配置文件运算的话,每一个用户最多占用多少容量
     * max(1,2)可以获取最多允许为每一个用户分配多少容量
     */
    Resource limit =
        Resources.roundUp(
            resourceCalculator, 
            Resources.min(
                resourceCalculator, clusterResource,   
                Resources.max(
                    resourceCalculator, clusterResource, 
                    Resources.divideAndCeil(
                        resourceCalculator, currentCapacity, activeUsers),//表示当前活跃用户数平均每个用户占用容量
                    Resources.divideAndCeil(
                        resourceCalculator, 
                        Resources.multiplyAndRoundDown(
                            currentCapacity, userLimit), 
                        100)//计算每一个用户限制最大量
                    ), 
                Resources.multiplyAndRoundDown(queueCapacity, userLimitFactor)//userLimitFactor如果大于100,则没意义,一定不会进入到这里,min阶段就过滤掉了,只会取第一部分
                ), 
            minimumAllocation);

    if (LOG.isDebugEnabled()) {
      String userName = application.getUser();
      LOG.debug("User limit computation for " + userName + 
          " in queue " + getQueueName() +
          " userLimit=" + userLimit +
          " userLimitFactor=" + userLimitFactor +
          " required: " + required + 
          " consumed: " + user.getTotalConsumedResources() + 
          " limit: " + limit +
          " queueCapacity: " + queueCapacity + 
          " qconsumed: " + usedResources +
          " currentCapacity: " + currentCapacity +
          " activeUsers: " + activeUsers +
          " clusterCapacity: " + clusterResource
      );
    }

    return limit;
  }
  
  /**
   * 
   * @param clusterResource 集群资源
   * @param userName 该app所属的user对象
   * @param limit 所使用的资源上限,不允许比他大,如果比他大,则失败,返回false
   * @param application 等待分配的app
   * @param checkReservations
   * @param requestLabels 该app所需要的请求的资源 需要的标签集合
   * @return
   * 目的是将app分配给该user,因此需要校验该队列能否容纳该user执行一个app了
   * 比较该label在该user上消费的资源,如果该资源比limit大,则返回false
   */
  @Private
  protected synchronized boolean assignToUser(Resource clusterResource,
      String userName, Resource limit, FiCaSchedulerApp application,
      boolean checkReservations, Set<String> requestLabels) {
    User user = getUser(userName);//获取user对象
    
    String label = CommonNodeLabelsManager.NO_LABEL;//设置默认标签
    if (requestLabels != null && !requestLabels.isEmpty()) {
      label = requestLabels.iterator().next();//获取第一个标签
    }

    // Note: We aren't considering the current request since there is a fixed
    // overhead of the AM, but it's a > check, not a >= check, so...
    //比较该label在该user上消费的资源,如果该资源比limit大,则返回false
    if (Resources
        .greaterThan(resourceCalculator, clusterResource,
            user.getConsumedResourceByLabel(label),
            limit)) {
      // if enabled, check to see if could we potentially use this node instead
      // of a reserved node if the application has reserved containers
      if (this.reservationsContinueLooking && checkReservations) {
        if (Resources.lessThanOrEqual(
            resourceCalculator,
            clusterResource,
            Resources.subtract(user.getTotalConsumedResources(),
                application.getCurrentReservation()), limit)) {

          if (LOG.isDebugEnabled()) {
            LOG.debug("User " + userName + " in queue " + getQueueName()
                + " will exceed limit based on reservations - " + " consumed: "
                + user.getTotalConsumedResources() + " reserved: "
                + application.getCurrentReservation() + " limit: " + limit);
          }
          return true;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + userName + " in queue " + getQueueName()
            + " will exceed limit - " + " consumed: "
            + user.getTotalConsumedResources() + " limit: " + limit);
      }
      return false;
    }
    return true;
  }

  boolean needContainers(FiCaSchedulerApp application, Priority priority,
      Resource required) {
    int requiredContainers = application.getTotalRequiredResources(priority);
    int reservedContainers = application.getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor = 
          Resources.ratio(
              resourceCalculator, required, getMaximumAllocation()
              );
      
      // Use percentage of node required to bias against large containers...
      // Protect against corner case where you need the whole node with
      // Math.min(nodeFactor, minimumAllocationFactor)
      starvation = 
          (int)((application.getReReservations(priority) / (float)reservedContainers) * 
                (1.0f - (Math.min(nodeFactor, getMinimumAllocationFactor())))
               );
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" +
            " app.#re-reserve=" + application.getReReservations(priority) + 
            " reserved=" + reservedContainers + 
            " nodeFactor=" + nodeFactor + 
            " minAllocFactor=" + getMinimumAllocationFactor() +
            " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }

  /**
   * 让该应用的priority优先级上的请求在该node上去调度
   * @param clusterResource 集群资源
   * @param node 要在哪个node上去调度容器
   * @param application 容器所执行的是哪个app
   * @param priority 容器所执行的是哪个app的什么优先级的请求
   * @param reservedContainer
   * @param needToUnreserve
   * @return 返回在该node上分配的容器资源详细情况
   */
  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, boolean needToUnreserve) {
    Resource assigned = Resources.none();

    // Data-local
    ResourceRequest nodeLocalResourceRequest =
        application.getResourceRequest(priority, node.getNodeName());//获取本地请求
    if (nodeLocalResourceRequest != null) {
      assigned = 
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest, 
              node, application, priority, reservedContainer, needToUnreserve); 
      if (Resources.greaterThan(resourceCalculator, clusterResource, 
          assigned, Resources.none())) {//说明已经获取到了资源,则返回
        return new CSAssignment(assigned, NodeType.NODE_LOCAL);
      }
    }

    // Rack-local
    ResourceRequest rackLocalResourceRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }
      
      assigned = 
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest, 
              node, application, priority, reservedContainer, needToUnreserve);
      if (Resources.greaterThan(resourceCalculator, clusterResource, 
          assigned, Resources.none())) {
        return new CSAssignment(assigned, NodeType.RACK_LOCAL);
      }
    }
    
    // Off-switch
    ResourceRequest offSwitchResourceRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      return new CSAssignment(
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
              node, application, priority, reservedContainer, needToUnreserve), 
              NodeType.OFF_SWITCH);
    }
    
    return SKIP_ASSIGNMENT;
  }

  @Private
  protected boolean findNodeToUnreserve(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      Resource capability) {
    // need to unreserve some other container first
    NodeId idToUnreserve = application.getNodeIdToUnreserve(priority, capability);
    if (idToUnreserve == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("checked to see if could unreserve for app but nothing "
            + "reserved that matches for this app");
      }
      return false;
    }
    FiCaSchedulerNode nodeToUnreserve = scheduler.getNode(idToUnreserve);
    if (nodeToUnreserve == null) {
      LOG.error("node to unreserve doesn't exist, nodeid: " + idToUnreserve);
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("unreserving for app: " + application.getApplicationId()
        + " on nodeId: " + idToUnreserve
        + " in order to replace reserved application and place it on node: "
        + node.getNodeID() + " needing: " + capability);
    }

    // headroom
    Resources.addTo(application.getHeadroom(), nodeToUnreserve
        .getReservedContainer().getReservedResource());

    // Make sure to not have completedContainers sort the queues here since
    // we are already inside an iterator loop for the queues and this would
    // cause an concurrent modification exception.
    completedContainer(clusterResource, application, nodeToUnreserve,
        nodeToUnreserve.getReservedContainer(),
        SchedulerUtils.createAbnormalContainerStatus(nodeToUnreserve
            .getReservedContainer().getContainerId(),
            SchedulerUtils.UNRESERVED_CONTAINER),
        RMContainerEventType.RELEASED, null, false);
    return true;
  }

  @Private
  protected boolean checkLimitsToReserve(Resource clusterResource,
      FiCaSchedulerApp application, Resource capability,
      boolean needToUnreserve) {
    if (needToUnreserve) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("we needed to unreserve to be able to allocate");
      }
      return false;
    }

    // we can't reserve if we got here based on the limit
    // checks assuming we could unreserve!!!
    Resource userLimit = computeUserLimitAndSetHeadroom(application,
        clusterResource, capability, null);

    // Check queue max-capacity limit,
    // TODO: Consider reservation on labels
    //仅仅循环每一个标签,查看标签的使用资源占比是否满足条件,如果有任意一个标签使用占比超量,则返回false
    if (!canAssignToThisQueue(clusterResource, capability, null, application, false)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("was going to reserve but hit queue limit");
      }
      return false;
    }

    // Check user limit
    if (!assignToUser(clusterResource, application.getUser(), userLimit,
        application, false, null)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("was going to reserve but hit user limit");
      }
      return false;
    }
    return true;
  }


  /**
   * 对本地节点分配容器资源
   * @param clusterResource 集群资源
   * @param nodeLocalResourceRequest 请求的资源量
   * @param node 在该node上去请求一个容器执行任务
   * @param application 执行哪个app上的资源任务
   * @param priority 该app的任务的优先级
   * @param reservedContainer
   * @param needToUnreserve
   * @return
   */
  private Resource assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, boolean needToUnreserve) {
    if (canAssign(application, priority, node, NodeType.NODE_LOCAL, 
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
          needToUnreserve);
    }
    
    return Resources.none();
  }

  private Resource assignRackLocalContainers(
      Resource clusterResource, ResourceRequest rackLocalResourceRequest,  
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, boolean needToUnreserve) {
    if (canAssign(application, priority, node, NodeType.RACK_LOCAL, 
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
          needToUnreserve);
    }
    
    return Resources.none();
  }

  private Resource assignOffSwitchContainers(
      Resource clusterResource, ResourceRequest offSwitchResourceRequest,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority, 
      RMContainer reservedContainer, boolean needToUnreserve) {
    if (canAssign(application, priority, node, NodeType.OFF_SWITCH, 
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
          needToUnreserve);
    }
    
    return Resources.none();
  }

  boolean canAssign(FiCaSchedulerApp application, Priority priority, 
      FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Clearly we need containers for this application...
    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      // 'Delay' off-switch
      ResourceRequest offSwitchRequest = 
          application.getResourceRequest(priority, ResourceRequest.ANY);
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      long requiredContainers = offSwitchRequest.getNumContainers(); //需要多少个容器
      
      float localityWaitFactor = 
        application.getLocalityWaitFactor(priority, 
            scheduler.getNumClusterNodes());
      
      return ((requiredContainers * localityWaitFactor) < missedOpportunities);
    }

    // Check if we need containers on this rack 
    ResourceRequest rackLocalRequest = 
      application.getResourceRequest(priority, node.getRackName());
    if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
      return false;
    }
      
    // If we are here, we do need containers on this rack for RACK_LOCAL req
    if (type == NodeType.RACK_LOCAL) {
      // 'Delay' rack-local just a little bit...
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      return (
          Math.min(scheduler.getNumClusterNodes(), getNodeLocalityDelay()) < 
          missedOpportunities
          );
    }

    // Check if we need containers on this host
    if (type == NodeType.NODE_LOCAL) {
      // Now check if we need containers on this host...
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getNodeName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }
  
  private Container getContainer(RMContainer rmContainer, 
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      Resource capability, Priority priority) {
    return (rmContainer != null) ? rmContainer.getContainer() :
      createContainer(application, node, capability, priority);
  }

  /**
   * 创建一个容器
   */
  Container createContainer(FiCaSchedulerApp application, FiCaSchedulerNode node, 
      Resource capability, Priority priority) {
  
    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(application
        .getApplicationAttemptId(), application.getNewContainerId());
  
    // Create the container
    Container container =
        BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
          .getHttpAddress(), capability, priority, null);
  
    return container;
  }


  private Resource assignContainer(Resource clusterResource, FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      boolean needToUnreserve) {
    if (LOG.isDebugEnabled()) {
    	//打印日志,在node节点上为app的某个优先级分配一个容器,执行一定资源量
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " application=" + application.getApplicationId()
        + " priority=" + priority.getPriority()
        + " request=" + request + " type=" + type
        + " needToUnreserve= " + needToUnreserve);
    }
    
    // check if the resource request can access the label
    if (!SchedulerUtils.checkNodeLabelExpression(
        labelManager.getLabelsOnNode(node.getNodeID()),
        request.getNodeLabelExpression())) {
      // this is a reserved container, but we cannot allocate it now according
      // to label not match. This can be caused by node label changed
      // We should un-reserve this container.
      if (rmContainer != null) {
        unreserve(application, priority, node, rmContainer);
      }
      return Resources.none();
    }
    
    Resource capability = request.getCapability();//请求需要的资源
    Resource available = node.getAvailableResource();//node节点可用资源
    Resource totalResource = node.getTotalResource();//node节点总资源

    if (!Resources.fitsIn(capability, totalResource)) {//说明capability>totalResource,即请求的资源要大于目前节点总资源,因此打印日志即可
    	//打印日志,节点资源不足应付该请求
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for request : " + request
          + " node total capability : " + node.getTotalResource());
      return Resources.none();
    }
    
    //断言,可用资源一定要存在
    assert Resources.greaterThan(
        resourceCalculator, clusterResource, available, Resources.none());

    // Create the container if necessary 创建一个容器
    Container container = 
        getContainer(rmContainer, application, node, capability, priority);
  
    // something went wrong getting/creating the container 说明创建容器的时候失败了
    if (container == null) {
      LOG.warn("Couldn't get container for allocation!");
      return Resources.none();
    }

    // default to true since if reservation continue look feature isn't on
    // needContainers is checked earlier and we wouldn't have gotten this far
    boolean canAllocContainer = true;
    if (this.reservationsContinueLooking) {
      // based on reservations can we allocate/reserve more or do we need
      // to unreserve one first
      canAllocContainer = needContainers(application, priority, capability);
      if (LOG.isDebugEnabled()) {
        LOG.debug("can alloc container is: " + canAllocContainer);
      }
    }

    // Can we allocate a container on this node? 
    //计算available/capability ,如果大于0,说明该node节点有足够的资源可以运行该任务
    int availableContainers = 
        resourceCalculator.computeAvailableContainers(available, capability);
    if (availableContainers > 0) {//说明该node节点有足够的资源可以运行该任务
      // Allocate...

      // Did we previously reserve containers at this 'priority'?
      if (rmContainer != null) {
        unreserve(application, priority, node, rmContainer);
      } else if (this.reservationsContinueLooking
          && (!canAllocContainer || needToUnreserve)) {
        // need to unreserve some other container first
        boolean res = findNodeToUnreserve(clusterResource, node, application,
            priority, capability);
        if (!res) {
          return Resources.none();
        }
      } else {
        // we got here by possibly ignoring queue capacity limits. If the
        // parameter needToUnreserve is true it means we ignored one of those
        // limits in the chance we could unreserve. If we are here we aren't
        // trying to unreserve so we can't allocate anymore due to that parent
        // limit.
        if (needToUnreserve) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("we needed to unreserve to be able to allocate, skipping");
          }
          return Resources.none();
        }
      }

      // Inform the application
      RMContainer allocatedContainer = 
          application.allocate(type, node, priority, request, container);

      // Does the application need this resource?
      if (allocatedContainer == null) {
        return Resources.none();
      }

      // Inform the node 在该node上调度该容器
      node.allocateContainer(allocatedContainer);

      LOG.info("assignedContainer" +
          " application attempt=" + application.getApplicationAttemptId() +
          " container=" + container + 
          " queue=" + this + 
          " clusterResource=" + clusterResource);

      return container.getResource();
    } else {//说明该节点运行该任务资源不足
      // if we are allowed to allocate but this node doesn't have space, reserve it or
      // if this was an already a reserved container, reserve it again
      if ((canAllocContainer) || (rmContainer != null)) {

        if (reservationsContinueLooking) {
          // we got here by possibly ignoring parent queue capacity limits. If
          // the parameter needToUnreserve is true it means we ignored one of
          // those limits in the chance we could unreserve. If we are here
          // we aren't trying to unreserve so we can't allocate
          // anymore due to that parent limit
          boolean res = checkLimitsToReserve(clusterResource, application, capability, 
              needToUnreserve);
          if (!res) {
            return Resources.none();
          }
        }

        // Reserve by 'charging' in advance...
        reserve(application, priority, node, rmContainer, container);

        LOG.info("Reserved container " + 
            " application=" + application.getApplicationId() + 
            " resource=" + request.getCapability() + 
            " queue=" + this.toString() + 
            " usedCapacity=" + getUsedCapacity() + 
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + 
            " used=" + usedResources +
            " cluster=" + clusterResource);

        return request.getCapability();
      }
      return Resources.none();
    }
  }

  private void reserve(FiCaSchedulerApp application, Priority priority, 
      FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
    // Update reserved metrics if this is the first reservation
    if (rmContainer == null) {
      getMetrics().reserveResource(
          application.getUser(), container.getResource());
    }

    // Inform the application 
    rmContainer = application.reserve(node, priority, rmContainer, container);
    
    // Update the node
    node.reserveResource(application, priority, rmContainer);
  }

  private boolean unreserve(FiCaSchedulerApp application, Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer) {
    // Done with the reservation?
    if (application.unreserve(node, priority)) {
      node.unreserveResource(application);

      // Update reserved metrics
      getMetrics().unreserveResource(application.getUser(),
          rmContainer.getContainer().getResource());
      return true;
    }
    return false;
  }

  /**
   * 一个容器完成了
   */
  @Override
  public void completedContainer(Resource clusterResource, 
      FiCaSchedulerApp application, FiCaSchedulerNode node, RMContainer rmContainer, 
      ContainerStatus containerStatus, RMContainerEventType event, CSQueue childQueue,
      boolean sortQueues) {
    if (application != null) {

      boolean removed = false;

      // Careful! Locking order is important!
      synchronized (this) {

        Container container = rmContainer.getContainer();

        // Inform the application & the node
        // Note: It's safe to assume that all state changes to RMContainer
        // happen under scheduler's lock... 
        // So, this is, in effect, a transaction across application & node
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          removed = unreserve(application, rmContainer.getReservedPriority(),
              node, rmContainer);
        } else {
          removed =
            application.containerCompleted(rmContainer, containerStatus, event);
          node.releaseContainer(container);
        }

        // Book-keeping
        if (removed) {
          releaseResource(clusterResource, application,
              container.getResource(),
              labelManager.getLabelsOnNode(node.getNodeID()));
          LOG.info("completedContainer" +
              " container=" + container +
              " queue=" + this +
              " cluster=" + clusterResource);
        }
      }

      if (removed) {
        // Inform the parent queue _outside_ of the leaf-queue lock
        getParent().completedContainer(clusterResource, application, node,
          rmContainer, null, event, this, sortQueues);
      }
    }
  }

  /**
   * 添加资源的统计
   */
  synchronized void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      Set<String> nodeLabels) {
    super.allocateResource(clusterResource, resource, nodeLabels);
    
    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.assignContainer(resource, nodeLabels);
    // Note this is a bit unconventional since it gets the object and modifies
    // it here, rather then using set routine
    Resources.subtractFrom(application.getHeadroom(), resource); // headroom
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
    
    if (LOG.isDebugEnabled()) {
      LOG.info(getQueueName() + 
          " user=" + userName + 
          " used=" + usedResources + " numContainers=" + numContainers +
          " headroom = " + application.getHeadroom() +
          " user-resources=" + user.getTotalConsumedResources()
          );
    }
  }

  /**
   * 释放资源的统计
   */
  synchronized void releaseResource(Resource clusterResource, 
      FiCaSchedulerApp application, Resource resource, Set<String> nodeLabels) {
    super.releaseResource(clusterResource, resource, nodeLabels);
    
    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.releaseContainer(resource, nodeLabels);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
      
    LOG.info(getQueueName() + 
        " used=" + usedResources + " numContainers=" + numContainers + 
        " user=" + userName + " user-resources=" + user.getTotalConsumedResources());
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource) {
    lastClusterResource = clusterResource;
    
    // Update queue properties
    maxActiveApplications = 
        CSQueueUtils.computeMaxActiveApplications(
            resourceCalculator,
            clusterResource, minimumAllocation, 
            maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    maxActiveAppsUsingAbsCap = 
        CSQueueUtils.computeMaxActiveApplications(
            resourceCalculator,
            clusterResource, minimumAllocation, 
            maxAMResourcePerQueuePercent, absoluteCapacity);
    maxActiveApplicationsPerUser = 
        CSQueueUtils.computeMaxActiveApplicationsPerUser(
            maxActiveAppsUsingAbsCap, userLimit, userLimitFactor);
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, getParent(), clusterResource, 
        minimumAllocation);

    // queue metrics are updated, more resource may be available
    // activate the pending applications if possible
    activateApplications();

    // Update application properties
    for (FiCaSchedulerApp application : activeApplications) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource, 
            Resources.none(), null);
      }
    }
  }

  @VisibleForTesting
  public static class User {
    //该用户使用的总资源
    Resource consumed = Resources.createResource(0, 0);
    //该用户使用的资源中,每一个label对应的资源数量,所有的label集合之和与consumed相同
    Map<String, Resource> consumedByLabel = new HashMap<String, Resource>();
    //该用户提交的任务,但是没有被开始的任务数
    int pendingApplications = 0;
    //该用户正在执行的任务数
    int activeApplications = 0;

    public Resource getTotalConsumedResources() {
      return consumed;
    }
    
    public Resource getConsumedResourceByLabel(String label) {
      Resource r = consumedByLabel.get(label);
      if (null != r) {
        return r;
      }
      return Resources.none();
    }

    public int getPendingApplications() {
      return pendingApplications;
    }

    public int getActiveApplications() {
      return activeApplications;
    }

    public int getTotalApplications() {
      return getPendingApplications() + getActiveApplications();
    }
    
    public synchronized void submitApplication() {
      ++pendingApplications;
    }
    
    public synchronized void activateApplication() {
      --pendingApplications;
      ++activeApplications;
    }

    public synchronized void finishApplication(boolean wasActive) {
      if (wasActive) {
        --activeApplications;
      }
      else {
        --pendingApplications;
      }
    }

    /**
     * 为该节点添加了一个容器
     * 因此增加该节点该用户的资源大小,同时增加相应table的资源大小
     * @param resource
     * @param nodeLabels
     */
    public synchronized void assignContainer(Resource resource,Set<String> nodeLabels) {
      Resources.addTo(consumed, resource);
      
      if (nodeLabels == null || nodeLabels.isEmpty()) {
        if (!consumedByLabel.containsKey(RMNodeLabelsManager.NO_LABEL)) {
          consumedByLabel.put(RMNodeLabelsManager.NO_LABEL,Resources.createResource(0));
        }
        Resources.addTo(consumedByLabel.get(RMNodeLabelsManager.NO_LABEL),resource);
      } else {
        for (String label : nodeLabels) {
          if (!consumedByLabel.containsKey(label)) {
            consumedByLabel.put(label, Resources.createResource(0));
          }
          Resources.addTo(consumedByLabel.get(label), resource);
        }
      }
    }

    public synchronized void releaseContainer(Resource resource, Set<String> nodeLabels) {
      Resources.subtractFrom(consumed, resource);
      
      // Update usedResources by labels
      if (nodeLabels == null || nodeLabels.isEmpty()) {
        if (!consumedByLabel.containsKey(RMNodeLabelsManager.NO_LABEL)) {
          consumedByLabel.put(RMNodeLabelsManager.NO_LABEL,
              Resources.createResource(0));
        }
        Resources.subtractFrom(
            consumedByLabel.get(RMNodeLabelsManager.NO_LABEL), resource);
      } else {
        for (String label : nodeLabels) {
          if (!consumedByLabel.containsKey(label)) {
            consumedByLabel.put(label, Resources.createResource(0));
          }
          Resources.subtractFrom(consumedByLabel.get(label), resource);
        }
      }
    }  
  }

  @Override
  public void recoverContainer(Resource clusterResource,SchedulerApplicationAttempt attempt, RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    // Careful! Locking order is important! 
    synchronized (this) {
      allocateResource(clusterResource, attempt, rmContainer.getContainer()
          .getResource(), labelManager.getLabelsOnNode(rmContainer
          .getContainer().getNodeId()));
    }
    getParent().recoverContainer(clusterResource, attempt, rmContainer);
  }

  /**
   * Obtain (read-only) collection of active applications.
   * 仅仅返回正在执行的应用集合
   */
  public Set<FiCaSchedulerApp> getApplications() {
    // need to access the list of apps from the preemption monitor
    return activeApplications;
  }

  // return a single Resource capturing the overal amount of pending resources
  //获取所有活跃应用的总等待资源数
  public Resource getTotalResourcePending() {
    Resource ret = BuilderUtils.newResource(0, 0);
    for (FiCaSchedulerApp f : activeApplications) {
      Resources.addTo(ret, f.getTotalPendingRequests());
    }
    return ret;
  }

  /**
   * 将该队列中所有的任务添加到参数集合中
   */
  @Override
  public void collectSchedulerApplications(Collection<ApplicationAttemptId> apps) {
    for (FiCaSchedulerApp pendingApp : pendingApplications) {
      apps.add(pendingApp.getApplicationAttemptId());
    }
    for (FiCaSchedulerApp app : activeApplications) {
      apps.add(app.getApplicationAttemptId());
    }
  }

  @Override
  public void attachContainer(Resource clusterResource,FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      allocateResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), labelManager.getLabelsOnNode(rmContainer
          .getContainer().getNodeId()));
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveIn=" + this + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
          + usedResources + " cluster=" + clusterResource);
      // Inform the parent queue
      getParent().attachContainer(clusterResource, application, rmContainer);
    }
  }

  @Override
  public void detachContainer(Resource clusterResource,FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      releaseResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), labelManager.getLabelsOnNode(rmContainer.getContainer()
          .getNodeId()));
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveOut=" + this + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
          + usedResources + " cluster=" + clusterResource);
      // Inform the parent queue
      getParent().detachContainer(clusterResource, application, rmContainer);
    }
  }

  @Override
  public float getAbsActualCapacity() {
    if (Resources.lessThanOrEqual(resourceCalculator, lastClusterResource,lastClusterResource, Resources.none())) {
      return absoluteCapacity;
    }

    Resource resourceRespectLabels = labelManager == null ? lastClusterResource : labelManager.getQueueResource(queueName, accessibleLabels, lastClusterResource);
    float absActualCapacity = Resources.divide(resourceCalculator, lastClusterResource,resourceRespectLabels, lastClusterResource);
    
    return absActualCapacity > absoluteCapacity ? absoluteCapacity : absActualCapacity;
        
  }
  
  public void setCapacity(float capacity) {
    this.capacity = capacity;
  }

  public void setAbsoluteCapacity(float absoluteCapacity) {
    this.absoluteCapacity = absoluteCapacity;
  }

  public void setMaxApplications(int maxApplications) {
    this.maxApplications = maxApplications;
  }
  
  /*
   * Holds shared values used by all applications in
   * the queue to calculate headroom on demand
   */
  static class QueueHeadroomInfo {
    private Resource queueMaxCap;//该队列可以使用的资源最大量
    private Resource clusterResource;//该集群总资源
    
    public void setQueueMaxCap(Resource queueMaxCap) {
      this.queueMaxCap = queueMaxCap;
    }
    
    public Resource getQueueMaxCap() {
      return queueMaxCap;
    }
    
    public void setClusterResource(Resource clusterResource) {
      this.clusterResource = clusterResource;
    }
    
    public Resource getClusterResource() {
      return clusterResource;
    }
  }
}

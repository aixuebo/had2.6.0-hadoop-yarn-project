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
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.Sets;

@Private
@Evolving
public class ParentQueue extends AbstractCSQueue {

  private static final Log LOG = LogFactory.getLog(ParentQueue.class);

  protected final Set<CSQueue> childQueues;//包含的子队列集合
  private final boolean rootQueue;//是否是根队列
  final Comparator<CSQueue> queueComparator;
  volatile int numApplications;//队列包含的任务数

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null); 

  public ParentQueue(CapacitySchedulerContext cs,String queueName, CSQueue parent, CSQueue old) throws IOException { 
    super(cs, queueName, parent, old);
    
    this.queueComparator = cs.getQueueComparator();

    this.rootQueue = (parent == null);

    //获取配置文件中配置该队列的容量, root的单位是100
    float rawCapacity = cs.getConfiguration().getCapacity(getQueuePath());

    //root队列必须是100容量
    if (rootQueue && (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      throw new IllegalArgumentException("Illegal " +
          "capacity of " + rawCapacity + " for queue " + queueName +
          ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }

    /**
     * 百分比,root为1,该值在0-1之间，即该队列占用父节点总资源的百分比
     */
    float capacity = (float) rawCapacity / 100;//占比,占父容器的百分比
    
    /**
     * 父节点绝对值,该值在0-1之间,父节点的占用总结点的百分比
     */
    float parentAbsoluteCapacity = (rootQueue) ? 1.0f : parent.getAbsoluteCapacity(); 
      
    //计算本队列容量:父队列容量*百分比,即该队列占用总资源的百分比
    float absoluteCapacity = parentAbsoluteCapacity * capacity; 

    //获取该队列的最大capacity
    float  maximumCapacity = (float) cs.getConfiguration().getMaximumCapacity(getQueuePath()) / 100;
      
    //计算父亲的最大容量*maximumCapacity
    float absoluteMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent); 
    
    //获取该队列的状态
    QueueState state = cs.getConfiguration().getState(getQueuePath());

    //获取该队里的权限
    Map<QueueACL, AccessControlList> acls = cs.getConfiguration().getAcls(getQueuePath()); 
      
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());

    setupQueueConfigs(cs.getClusterResource(), capacity, absoluteCapacity,
        maximumCapacity, absoluteMaxCapacity, state, acls, accessibleLabels,
        defaultLabelExpression, capacitiyByNodeLabels, maxCapacityByNodeLabels, 
        cs.getConfiguration().getReservationContinueLook());
    
    this.childQueues = new TreeSet<CSQueue>(queueComparator);

    LOG.info("Initialized parent-queue " + queueName + 
        " name=" + queueName + 
        ", fullname=" + getQueuePath()); 
  }

  synchronized void setupQueueConfigs(Resource clusterResource, float capacity,
      float absoluteCapacity, float maximumCapacity, float absoluteMaxCapacity,
      QueueState state, Map<QueueACL, AccessControlList> acls,
      Set<String> accessibleLabels, String defaultLabelExpression,
      Map<String, Float> nodeLabelCapacities,
      Map<String, Float> maximumCapacitiesByLabel, 
      boolean reservationContinueLooking) throws IOException {
    super.setupQueueConfigs(clusterResource, capacity, absoluteCapacity,
        maximumCapacity, absoluteMaxCapacity, state, acls, accessibleLabels,
        defaultLabelExpression, nodeLabelCapacities, maximumCapacitiesByLabel,
        reservationContinueLooking);
    
    //打印信息
   StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }

    StringBuilder labelStrBuilder = new StringBuilder(); 
    if (accessibleLabels != null) {
      for (String s : accessibleLabels) {
        labelStrBuilder.append(s);
        labelStrBuilder.append(",");
      }
    }

    LOG.info(queueName +
        ", capacity=" + capacity +
        ", asboluteCapacity=" + absoluteCapacity +
        ", maxCapacity=" + maximumCapacity +
        ", asboluteMaxCapacity=" + absoluteMaxCapacity + 
        ", state=" + state +
        ", acls=" + aclsString + 
        ", labels=" + labelStrBuilder.toString() + "\n" +
        ", reservationsContinueLooking=" + reservationsContinueLooking);
  }

  private static float PRECISION = 0.0005f; // 0.05% precision
  
  /**
   * 添加子队列,并且校验
   */
  void setChildQueues(Collection<CSQueue> childQueues) {
    // Validate 校验
    float childCapacities = 0;
    for (CSQueue queue : childQueues) {
      childCapacities += queue.getCapacity();
    }

    float delta = Math.abs(1.0f - childCapacities);  // crude way to check
    // allow capacities being set to 0, and enforce child 0 if parent is 0
    if (((capacity > 0) && (delta > PRECISION)) || //子节点所有的容量之和应该等于1
        ((capacity == 0) && (childCapacities > 0))) {//并且如果capacity=0,则子节点容量一定不能大于0
      throw new IllegalArgumentException("Illegal" +
      		" capacity of " + childCapacities + 
      		" for children of queue " + queueName);
    }
    
    //校验label标签
    // check label capacities
    for (String nodeLabel : labelManager.getClusterNodeLabels()) {//循环集群所有的标签
      float capacityByLabel = getCapacityByNodeLabel(nodeLabel);//返回配置文件中配置的该队列对该标签对应的容量信息
      // check children's labels
      float sum = 0;
      for (CSQueue queue : childQueues) {//计算子队列中为该标签配置的容量之和
        sum += queue.getCapacityByNodeLabel(nodeLabel);
      }
      if ((capacityByLabel > 0 && Math.abs(1.0f - sum) > PRECISION) //子节点有标签的所有的容量之和应该等于1
          || (capacityByLabel == 0) && (sum > 0)) {//如果没有配置标签,则子节点不允许有该标签
        throw new IllegalArgumentException("Illegal" + " capacity of "
            + sum + " for children of queue " + queueName
            + " for label=" + nodeLabel);
      }
    }
    
    //重新设置子队列
    this.childQueues.clear();
    this.childQueues.addAll(childQueues);
    if (LOG.isDebugEnabled()) {
      LOG.debug("setChildQueues: " + getChildQueuesToPrint());
    }
  }

  @Override
  public String getQueuePath() {
    String parentPath = ((parent == null) ? "" : (parent.getQueuePath() + "."));
    return parentPath + getQueueName();
  }

  /**
   * @param includeChildQueues true表示要添加子队列的信息
   * @param recursive 表示获取子队列信息的时候是否要获取孙子辈的信息
   */
  @Override
  public synchronized QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) { 
    queueInfo.setCurrentCapacity(usedCapacity);

    List<QueueInfo> childQueuesInfo = new ArrayList<QueueInfo>();
    if (includeChildQueues) {//表示要添加子队列的信息
      for (CSQueue child : childQueues) {
        // Get queue information recursively?
        childQueuesInfo.add(child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueuesInfo);
    
    return queueInfo;
  }

  /**
   * 设置该用户可以访问的权限
   */
  private synchronized QueueUserACLInfo getUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = recordFactory.newRecordInstance(QueueUserACLInfo.class); 
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
  public synchronized List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
      
    List<QueueUserACLInfo> userAcls = new ArrayList<QueueUserACLInfo>();
    
    // Add parent queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    for (CSQueue child : childQueues) {
      userAcls.addAll(child.getQueueUserAclInfo(user));
    }
 
    return userAcls;
  }

  public String toString() {
    return queueName + ": " +
        "numChildQueue= " + childQueues.size() + ", " + 
        "capacity=" + capacity + ", " +  
        "absoluteCapacity=" + absoluteCapacity + ", " +
        "usedResources=" + usedResources + 
        "usedCapacity=" + getUsedCapacity() + ", " + 
        "numApps=" + getNumApplications() + ", " + 
        "numContainers=" + getNumContainers();
  }
  
  @Override
  public synchronized void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
  throws IOException {
    // Sanity check 必须还得是父节点以及节点名称不能变
    if (!(newlyParsedQueue instanceof ParentQueue) ||
        !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() +
          " from " + newlyParsedQueue.getQueuePath());
    }

    ParentQueue newlyParsedParentQueue = (ParentQueue)newlyParsedQueue;

    // Set new configs
    setupQueueConfigs(clusterResource,
        newlyParsedParentQueue.capacity, 
        newlyParsedParentQueue.absoluteCapacity,
        newlyParsedParentQueue.maximumCapacity, 
        newlyParsedParentQueue.absoluteMaxCapacity,
        newlyParsedParentQueue.state, 
        newlyParsedParentQueue.acls,
        newlyParsedParentQueue.accessibleLabels,
        newlyParsedParentQueue.defaultLabelExpression,
        newlyParsedParentQueue.capacitiyByNodeLabels,
        newlyParsedParentQueue.maxCapacityByNodeLabels,
        newlyParsedParentQueue.reservationsContinueLooking);

    // Re-configure existing child queues and add new ones
    // The CS has already checked to ensure all existing child queues are present!
    //改变前的子队列
    Map<String, CSQueue> currentChildQueues = getQueues(childQueues);
    //改变后的子队列
    Map<String, CSQueue> newChildQueues = 
        getQueues(newlyParsedParentQueue.childQueues);
    
    //循环新的子队列
    for (Map.Entry<String, CSQueue> e : newChildQueues.entrySet()) {
      String newChildQueueName = e.getKey();//队列名称
      CSQueue newChildQueue = e.getValue();//队列对象

      //从老的队列中获取该队列
      CSQueue childQueue = currentChildQueues.get(newChildQueueName);
      
      // Check if the child-queue already exists
      if (childQueue != null) {//说明该子队列以前存在
        // Re-init existing child queues 重新初始化该子队列信息
        childQueue.reinitialize(newChildQueue, clusterResource);
        LOG.info(getQueueName() + ": re-configured queue: " + childQueue);
      } else {//说明以前不存在,该子队列新添加的
        // New child queue, do not re-init
        
        // Set parent to 'this'
        newChildQueue.setParent(this);//为新队列添加父队列关系
        
        // Save in list of current child queues 追加到当前队列集合中
        currentChildQueues.put(newChildQueueName, newChildQueue);
        
        LOG.info(getQueueName() + ": added new child queue: " + newChildQueue);
      }
    }

    // Re-sort all queues
    childQueues.clear();
    childQueues.addAll(currentChildQueues.values());
  }

  /**
   * 将队列集合,转换成队列Map映射关系
   */
  Map<String, CSQueue> getQueues(Set<CSQueue> queues) {
    Map<String, CSQueue> queuesMap = new HashMap<String, CSQueue>();
    for (CSQueue queue : queues) {
      queuesMap.put(queue.getQueueName(), queue);
    }
    return queuesMap;
  }

  /**
   * 提交一个应用
   */
  @Override
  public void submitApplication(ApplicationId applicationId, String user,String queue) throws AccessControlException {
      
    
    synchronized (this) {
      // Sanity check
      if (queue.equals(queueName)) {//不能提交应用到非叶子节点上
        throw new AccessControlException("Cannot submit application " +
            "to non-leaf queue: " + queueName);
      }
      
      if (state != QueueState.RUNNING) {//该队列是非运行的队列,也是不允许的
        throw new AccessControlException("Queue " + getQueuePath() +
            " is STOPPED. Cannot accept submission of application: " +
            applicationId);
      }

      addApplication(applicationId, user);
    }
    
    // Inform the parent queue
    if (parent != null) {
      try {
        parent.submitApplication(applicationId, user, queue);
      } catch (AccessControlException ace) {
        LOG.info("Failed to submit application to parent-queue: " + 
            parent.getQueuePath(), ace);
        removeApplication(applicationId, user);
        throw ace;
      }
    }
  }


  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,String userName) {
    // submit attempt logic.
  }

  @Override
  public void finishApplicationAttempt(FiCaSchedulerApp application,String queue) {
    // finish attempt logic.
  }

  //增加该队列运行的应用job数量
  private synchronized void addApplication(ApplicationId applicationId,String user) {

    ++numApplications;

    LOG.info("Application added -" +
        " appId: " + applicationId + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }
  
  /**
   * 完成一个队列,减少该队列运行的应用job数量
   */
  @Override
  public void finishApplication(ApplicationId application, String user) {
    
    synchronized (this) {
      removeApplication(application, user);
    }
    
    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, user);
    }
  }

  //减少该队列运行的应用job数量
  private synchronized void removeApplication(ApplicationId applicationId,String user) { 
    
    --numApplications;

    LOG.info("Application removed -" +
        " appId: " + applicationId + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }

  /**
   * 为node分配一些容器
   */
  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource, FiCaSchedulerNode node, boolean needToUnreserve) {
    
    CSAssignment assignment = new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL); 
    
    // if our queue cannot access this node, just return 判断该队列的标签集合能否在该node上执行
    if (!SchedulerUtils.checkQueueAccessToNode(accessibleLabels,labelManager.getLabelsOnNode(node.getNodeID()))) {
      return assignment;//说明不能执行,则返回
    }
    
    //该节点不允许有分配预留资源并且该node节点的可用资源要大于每一个应用的最小资源
    while (canAssign(clusterResource, node)) {//不断为该node分配容器
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign containers to child-queue of "+ getQueueName());
      }
      
      boolean localNeedToUnreserve = false;
      //该node上的标签集合
      Set<String> nodeLabels = labelManager.getLabelsOnNode(node.getNodeID()); 
      
      // Are we over maximum-capacity for this queue?是否超过了该队列的最大容量限制
      //true表示该资源可以存储在改队列上
      if (!canAssignToThisQueue(clusterResource, nodeLabels)) {//不能分配到该队列
        // check to see if we could if we unreserve first
        localNeedToUnreserve = assignToQueueIfUnreserve(clusterResource);
        if (!localNeedToUnreserve) {
          break;
        }
      }
      
      // Schedule 真正去在子队列中找到可以在该node上执行的容器,然后返回
      CSAssignment assignedToChild = 
          assignContainersToChildQueues(clusterResource, node, localNeedToUnreserve | needToUnreserve);
      assignment.setType(assignedToChild.getType());
      
      // Done if no child-queue assigned anything
      if (Resources.greaterThan(
              resourceCalculator, clusterResource, 
              assignedToChild.getResource(), Resources.none())) {//assignedToChild.getResource() > none,说明在该node上已经分配了容器
        // Track resource utilization for the parent-queue
        super.allocateResource(clusterResource, assignedToChild.getResource(),
            nodeLabels);//添加该队列以及该队列上node标签上对应的资源使用情况
        
        // Track resource utilization in this pass of the scheduler将资源添加到全局assignment.getResource()中
        Resources.addTo(assignment.getResource(), assignedToChild.getResource());
        
        LOG.info("assignedContainer" +
            " queue=" + getQueueName() + 
            " usedCapacity=" + getUsedCapacity() +
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
            " used=" + usedResources + 
            " cluster=" + clusterResource);

      } else {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("ParentQ=" + getQueueName()
          + " assignedSoFarInThisIteration=" + assignment.getResource()
          + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity());
      }

      // Do not assign more than one container if this isn't the root queue
      // or if we've already assigned an off-switch container
      //如果不是root队列,则仅会为该node分一个容器即可退出
      //如果分发到了OFF_SWITCH节点上了,则仅会为该node分一个容器即可退出
      if (!rootQueue || assignment.getType() == NodeType.OFF_SWITCH) {//因为一直都是从root队列进来的,因此在root队列的时候会一直不断进行循环的,不会进入到if中
        if (LOG.isDebugEnabled()) {
          if (rootQueue && assignment.getType() == NodeType.OFF_SWITCH) {
            LOG.debug("Not assigning more than one off-switch container," +
                " assignments so far: " + assignment);
          }
        }
        break;
      }
    } 
    
    return assignment;
  }

  /**
   * 判断能否在该队列中分配
   * @param clusterResource 集群资源
   * @param nodeLabels 该node节点的可用标签
   * true表示该资源可以存储在改队列上
   */
  private synchronized boolean canAssignToThisQueue(Resource clusterResource,Set<String> nodeLabels) {
    //获取标签集合,要该队列的标签集合和该node标签集合的交集
    Set<String> labelCanAccess = new HashSet<String>(
            accessibleLabels.contains(CommonNodeLabelsManager.ANY) ? nodeLabels
                : Sets.intersection(accessibleLabels, nodeLabels));
    if (nodeLabels.isEmpty()) {
      // Any queue can always access any node without label 一个队列总是可以让任何没有设置标签的node节点访问
      labelCanAccess.add(RMNodeLabelsManager.NO_LABEL);
    }
    
    boolean canAssign = true;
    for (String label : labelCanAccess) {//循环可以访问的label标签
      //为每一个标签设置资源
      if (!usedResourcesByNodeLabels.containsKey(label)) {
        usedResourcesByNodeLabels.put(label, Resources.createResource(0));
      }
      
      /**
       * usedResourcesByNodeLabels.get(label)/labelManager.getResourceByLabel(label, clusterResource)
       * 即该label在该队列中已经使用的资源量/该label总的使用资源量 = 当前该label已经使用量的占比
       */
      float currentAbsoluteLabelUsedCapacity =
          Resources.divide(resourceCalculator, clusterResource,
              usedResourcesByNodeLabels.get(label),
              labelManager.getResourceByLabel(label, clusterResource));
      // if any of the label doesn't beyond limit, we can allocate on this node
      if (currentAbsoluteLabelUsedCapacity >= 
            getAbsoluteMaximumCapacityByNodeLabel(label)) {//该标签,已经超过最大占比了,则不能分配该资源了
        if (LOG.isDebugEnabled()) {
          LOG.debug(getQueueName() + " used=" + usedResources
              + " current-capacity (" + usedResourcesByNodeLabels.get(label) + ") "
              + " >= max-capacity ("
              + labelManager.getResourceByLabel(label, clusterResource) + ")");
        }
        canAssign = false;
        break;
      }
    }
    
    return canAssign;
  }

  
  private synchronized boolean assignToQueueIfUnreserve(Resource clusterResource) {
    if (this.reservationsContinueLooking) {      
      // check to see if we could potentially use this node instead of a reserved
      // node

      Resource reservedResources = Resources.createResource(getMetrics()
          .getReservedMB(), getMetrics().getReservedVirtualCores());
      float capacityWithoutReservedCapacity = Resources.divide(
          resourceCalculator, clusterResource,
          Resources.subtract(usedResources, reservedResources),
          clusterResource);

      if (capacityWithoutReservedCapacity <= absoluteMaxCapacity) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("parent: try to use reserved: " + getQueueName()
            + " usedResources: " + usedResources.getMemory()
            + " clusterResources: " + clusterResource.getMemory()
            + " reservedResources: " + reservedResources.getMemory()
            + " currentCapacity " + ((float) usedResources.getMemory())
            / clusterResource.getMemory()
            + " potentialNewWithoutReservedCapacity: "
            + capacityWithoutReservedCapacity + " ( " + " max-capacity: "
            + absoluteMaxCapacity + ")");
        }
        // we could potentially use this node instead of reserved node
        return true;
      }
    }
    return false;
   }

  /**
   * 该节点不允许有分配预留资源并且该node节点的可用资源要大于每一个应用的最小资源
   */
  private boolean canAssign(Resource clusterResource, FiCaSchedulerNode node) {
    return (node.getReservedContainer() == null) && 
        Resources.greaterThanOrEqual(resourceCalculator, clusterResource, 
            node.getAvailableResource(), minimumAllocation);
  }
  
  /**
   * 在一个子队列中分配一个容器,返回分配后的信息
   */
  private synchronized CSAssignment assignContainersToChildQueues(Resource cluster,FiCaSchedulerNode node, boolean needToUnreserve) { 
      
    CSAssignment assignment = new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL); 
    
    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    //循环每一个子队列,看看将那些任务分配到该node上,只要在一个队列上分配了任务则退出该循环
    for (Iterator<CSQueue> iter=childQueues.iterator(); iter.hasNext();) {
      CSQueue childQueue = iter.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign to queue: " + childQueue.getQueuePath()
          + " stats: " + childQueue);
      }
      
      //见该node分配到该子队列上
      assignment = childQueue.assignContainers(cluster, node, needToUnreserve);
      if(LOG.isDebugEnabled()) {
    	  //打印日志,已经分发到该子队列上,并且子队列的状态...,打印在该队列上申请了多少资源,以及申请的type类型
        LOG.debug("Assigned to queue: " + childQueue.getQueuePath() +
          " stats: " + childQueue + " --> " + 
          assignment.getResource() + ", " + assignment.getType());
      }

      // If we do assign, remove the queue and re-insert in-order to re-sort
      if (Resources.greaterThan(
              resourceCalculator, cluster, 
              assignment.getResource(), Resources.none())) {//如果我们申请到的资源比none空资源大,说明我们已经申请到资源了,则重新删除该队列,再重新插入,这样调整队列的顺序,可以使每一个子队列公平被调度
        // Remove and re-insert to sort 移除然后在插入,目的是为了排序
        iter.remove();//先移除队列
        LOG.info("Re-sorting assigned queue: " + childQueue.getQueuePath() + 
            " stats: " + childQueue);//打印重新排列队列日志
        childQueues.add(childQueue);//重新添加该队列
        if (LOG.isDebugEnabled()) {
          printChildQueues();
        }
        break;
      }
    }
    
    return assignment;
  }

  /**
   * 打印子队列信息
   */
  String getChildQueuesToPrint() {
    StringBuilder sb = new StringBuilder();
    for (CSQueue q : childQueues) {
      sb.append(q.getQueuePath() + 
          "usedCapacity=(" + q.getUsedCapacity() + "), " + 
          " label=("
          + StringUtils.join(q.getAccessibleNodeLabels().iterator(), ",") 
          + ")");
    }
    return sb.toString();
  }

  /**
   * 打印子队列信息
   */
  private void printChildQueues() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("printChildQueues - queue: " + getQueuePath()
        + " child-queues: " + getChildQueuesToPrint());
    }
  }
  
  @Override
  public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      RMContainer rmContainer, ContainerStatus containerStatus, 
      RMContainerEventType event, CSQueue completedChildQueue,
      boolean sortQueues) {
    if (application != null) {
      // Careful! Locking order is important!
      // Book keeping
      synchronized (this) {
        super.releaseResource(clusterResource, rmContainer.getContainer()
            .getResource(), labelManager.getLabelsOnNode(node.getNodeID()));

        LOG.info("completedContainer" +
            " queue=" + getQueueName() + 
            " usedCapacity=" + getUsedCapacity() +
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
            " used=" + usedResources + 
            " cluster=" + clusterResource);
      }

      // Note that this is using an iterator on the childQueues so this can't be
      // called if already within an iterator for the childQueues. Like  
      // from assignContainersToChildQueues.
      if (sortQueues) {
        // reinsert the updated queue
        for (Iterator<CSQueue> iter=childQueues.iterator(); iter.hasNext();) {
          CSQueue csqueue = iter.next();
          if(csqueue.equals(completedChildQueue))
          {
            iter.remove();
            LOG.info("Re-sorting completed queue: " + csqueue.getQueuePath() + 
                " stats: " + csqueue);
            childQueues.add(csqueue);
            break;
          }
        }
      }
      
      // Inform the parent
      if (parent != null) {
        // complete my parent
        parent.completedContainer(clusterResource, application, 
            node, rmContainer, null, event, this, sortQueues);
      }    
    }
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource) {
    // Update all children
    for (CSQueue childQueue : childQueues) {
      childQueue.updateClusterResource(clusterResource);
    }
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, parent, clusterResource, minimumAllocation);
  }
  
  /**
   * 返回子队列信息
   */
  @Override
  public synchronized List<CSQueue> getChildQueues() {
    return new ArrayList<CSQueue>(childQueues);
  }
  
  @Override
  public void recoverContainer(Resource clusterResource,SchedulerApplicationAttempt attempt, RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    // Careful! Locking order is important! 
    synchronized (this) {
      super.allocateResource(clusterResource, rmContainer.getContainer()
          .getResource(), labelManager.getLabelsOnNode(rmContainer
          .getContainer().getNodeId()));
    }
    if (parent != null) {
      parent.recoverContainer(clusterResource, attempt, rmContainer);
    }
  }
  
  @Override
  public ActiveUsersManager getActiveUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void collectSchedulerApplications(Collection<ApplicationAttemptId> apps) {
    for (CSQueue queue : childQueues) {
      queue.collectSchedulerApplications(apps);
    }
  }

  @Override
  public void attachContainer(Resource clusterResource,FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      super.allocateResource(clusterResource, rmContainer.getContainer()
          .getResource(), labelManager.getLabelsOnNode(rmContainer
          .getContainer().getNodeId()));
      LOG.info("movedContainer" + " queueMoveIn=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + usedResources + " cluster="
          + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.attachContainer(clusterResource, application, rmContainer);
      }
    }
  }

  @Override
  public void detachContainer(Resource clusterResource,FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      super.releaseResource(clusterResource,
          rmContainer.getContainer().getResource(),
          labelManager.getLabelsOnNode(rmContainer.getContainer().getNodeId()));
      LOG.info("movedContainer" + " queueMoveOut=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + usedResources + " cluster="
          + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.detachContainer(clusterResource, application, rmContainer);
      }
    }
  }

  @Override
  public float getAbsActualCapacity() {
    // for now, simply return actual capacity = guaranteed capacity for parent
    // queue
    return absoluteCapacity;
  }
  
  public synchronized int getNumApplications() {
    return numApplications;
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.utils.BuilderUtils.ContainerIdComparator;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used to keep track of all the applications/containers
 * running on a node.
 * 该类表示ResourceManager上持有关于该NodeManager节点上应用和容器运行的一些信息
 */
@Private
@Unstable
@SuppressWarnings("unchecked")
public class RMNodeImpl implements RMNode, EventHandler<RMNodeEvent> {

  private static final Log LOG = LogFactory.getLog(RMNodeImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final ReadLock readLock;
  private final WriteLock writeLock;

  //一个队列,表示每次状态更新的时候,存储远程节点刚刚添加的容器以及刚刚完成的容器,内容由远程节点控制
  private final ConcurrentLinkedQueue<UpdatedContainerInfo> nodeUpdateQueue;
  private volatile boolean nextHeartBeat = true;

  private final NodeId nodeId;
  private final RMContext context;
  private final String hostName;
  private final int commandPort;
  private int httpPort;
  private final String nodeAddress; // The containerManager address
  private String httpAddress;
  private volatile Resource totalCapability;
  private final Node node;

  private String healthReport;//远程节点不健康的话,输出什么信息
  private long lastHealthReportTime;//更新远程节点最后一次健康检查时间
  private String nodeManagerVersion;

  /* set of containers that have just launched 说明节点上已经启动的容器,内容由远程节点控制*/
  private final Set<ContainerId> launchedContainers = new HashSet<ContainerId>();

  /* set of containers that need to be cleaned 待清理的容器*/
  private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(new ContainerIdComparator());

  /*
   * set of containers to notify NM to remove them from its context. Currently,
   * this includes containers that were notified to AM about their completion
   * 要从Node上移除的容器
   */
  private final Set<ContainerId> containersToBeRemovedFromNM = new HashSet<ContainerId>();

  /* the list of applications that have finished and need to be purged 已经完成的应用*/
  private final List<ApplicationId> finishedApplications = new ArrayList<ApplicationId>();

  //记录该节点的上一次心跳请求返回的信息
  private NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
  
  private static final StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent> stateMachineFactory 
                 = new StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent>(NodeState.NEW)
  
     //Transitions from NEW state
     .addTransition(NodeState.NEW, NodeState.RUNNING, 
         RMNodeEventType.STARTED, new AddNodeTransition())
     .addTransition(NodeState.NEW, NodeState.NEW,
         RMNodeEventType.RESOURCE_UPDATE, 
         new UpdateNodeResourceWhenUnusableTransition())

     //Transitions from RUNNING state
     .addTransition(NodeState.RUNNING,
         EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
         RMNodeEventType.STATUS_UPDATE, new StatusUpdateWhenHealthyTransition())
     .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONED,
         RMNodeEventType.DECOMMISSION,
         new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
     .addTransition(NodeState.RUNNING, NodeState.LOST,
         RMNodeEventType.EXPIRE,
         new DeactivateNodeTransition(NodeState.LOST))
     .addTransition(NodeState.RUNNING, NodeState.REBOOTED,
         RMNodeEventType.REBOOTING,
         new DeactivateNodeTransition(NodeState.REBOOTED))
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenRunningTransition())

     //Transitions from REBOOTED state
     .addTransition(NodeState.REBOOTED, NodeState.REBOOTED,
         RMNodeEventType.RESOURCE_UPDATE,
         new UpdateNodeResourceWhenUnusableTransition())
         
     //Transitions from DECOMMISSIONED state
     .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
         RMNodeEventType.RESOURCE_UPDATE,
         new UpdateNodeResourceWhenUnusableTransition())
     .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())

     //Transitions from LOST state
     .addTransition(NodeState.LOST, NodeState.LOST,
         RMNodeEventType.RESOURCE_UPDATE,
         new UpdateNodeResourceWhenUnusableTransition())
     .addTransition(NodeState.LOST, NodeState.LOST,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())

     //Transitions from UNHEALTHY state
     .addTransition(NodeState.UNHEALTHY,
         EnumSet.of(NodeState.UNHEALTHY, NodeState.RUNNING),
         RMNodeEventType.STATUS_UPDATE,
         new StatusUpdateWhenUnHealthyTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONED,
         RMNodeEventType.DECOMMISSION,
         new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
     .addTransition(NodeState.UNHEALTHY, NodeState.LOST,
         RMNodeEventType.EXPIRE,
         new DeactivateNodeTransition(NodeState.LOST))
     .addTransition(NodeState.UNHEALTHY, NodeState.REBOOTED,
         RMNodeEventType.REBOOTING,
         new DeactivateNodeTransition(NodeState.REBOOTED))
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenUnusableTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())

     // create the topology tables
     .installTopology(); 

  private final StateMachine<NodeState, RMNodeEventType,RMNodeEvent> stateMachine;

  /**
   * 节点传递来的信息
   * @param nodeId 节点ID
   * @param context ResourceManager上下文
   * @param hostName 节点nodeId上面存储的host
   * @param cmPort 节点nodeId上存储的端口号
   * @param httpPort 节点传过来的http端口号
   * @param node 通过该方法httpPort,resolve(host)返回的Node
   * @param capability 节点允许的最大资源
   * @param nodeManagerVersion 节点上保留的Yarn版本
   */
  public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
      int cmPort, int httpPort, Node node, Resource capability, String nodeManagerVersion) {
    this.nodeId = nodeId;
    this.context = context;
    this.hostName = hostName;
    this.commandPort = cmPort;
    this.httpPort = httpPort;
    this.totalCapability = capability; 
    this.nodeAddress = hostName + ":" + cmPort;
    this.httpAddress = hostName + ":" + httpPort;
    this.node = node;
    this.healthReport = "Healthy";
    this.lastHealthReportTime = System.currentTimeMillis();
    this.nodeManagerVersion = nodeManagerVersion;

    this.latestNodeHeartBeatResponse.setResponseId(0);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);
    
    this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();  
  }

  @Override
  public String toString() {
    return this.nodeId.toString();
  }

  @Override
  public String getHostName() {
    return hostName;
  }

  @Override
  public int getCommandPort() {
    return commandPort;
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  @Override
  public NodeId getNodeID() {
    return this.nodeId;
  }

  @Override
  public String getNodeAddress() {
    return this.nodeAddress;
  }

  @Override
  public String getHttpAddress() {
    return this.httpAddress;
  }

  @Override
  public Resource getTotalCapability() {
    return this.totalCapability;
  }

  @Override
  public String getRackName() {
    return node.getNetworkLocation();
  }
  
  @Override
  public Node getNode() {
    return this.node;
  }
  
  @Override
  public String getHealthReport() {
    this.readLock.lock();

    try {
      return this.healthReport;
    } finally {
      this.readLock.unlock();
    }
  }
  
  public void setHealthReport(String healthReport) {
    this.writeLock.lock();

    try {
      this.healthReport = healthReport;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  public void setLastHealthReportTime(long lastHealthReportTime) {
    this.writeLock.lock();

    try {
      this.lastHealthReportTime = lastHealthReportTime;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Override
  public long getLastHealthReportTime() {
    this.readLock.lock();

    try {
      return this.lastHealthReportTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getNodeManagerVersion() {
    return nodeManagerVersion;
  }

  @Override
  public NodeState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  //复制信息
  @Override
  public List<ApplicationId> getAppsToCleanup() {
    this.readLock.lock();

    try {
      return new ArrayList<ApplicationId>(this.finishedApplications);
    } finally {
      this.readLock.unlock();
    }

  }
  
  //复制信息
  @Override
  public List<ContainerId> getContainersToCleanUp() {

    this.readLock.lock();

    try {
      return new ArrayList<ContainerId>(this.containersToClean);
    } finally {
      this.readLock.unlock();
    }
  };

  /**
   * 参数是准备回复给心跳节点的对象
   */
  @Override
  public void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response) {
    this.writeLock.lock();

    try {
      response.addAllContainersToCleanup(new ArrayList<ContainerId>(this.containersToClean));
      response.addAllApplicationsToCleanup(this.finishedApplications);
      response.addContainersToBeRemovedFromNM(new ArrayList<ContainerId>(this.containersToBeRemovedFromNM));
          
      this.containersToClean.clear();
      this.finishedApplications.clear();
      this.containersToBeRemovedFromNM.clear();
    } finally {
      this.writeLock.unlock();
    }
  };

  /**
   * 记录该节点的上一次请求返回的信息
   */
  @Override
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {

    this.readLock.lock();

    try {
      return this.latestNodeHeartBeatResponse;
    } finally {
      this.readLock.unlock();
    }
  }

  public void handle(RMNodeEvent event) {
    LOG.debug("Processing " + event.getNodeId() + " of type " + event.getType());
    try {
      writeLock.lock();
      NodeState oldState = getState();
      try {
         stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + 
            " on Node  " + this.nodeId);
      }
      if (oldState != getState()) {
        LOG.info(nodeId + " Node Transitioned from " + oldState + " to "
                 + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  private void updateMetricsForRejoinedNode(NodeState previousNodeState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    metrics.incrNumActiveNodes();

    switch (previousNodeState) {
    case LOST:
      metrics.decrNumLostNMs();
      break;
    case REBOOTED:
      metrics.decrNumRebootedNMs();
      break;
    case DECOMMISSIONED:
      metrics.decrDecommisionedNMs();
      break;
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    }
  }

  private void updateMetricsForDeactivatedNode(NodeState initialState,
                                               NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();

    switch (initialState) {
      case RUNNING:
        metrics.decrNumActiveNodes();
        break;
      case UNHEALTHY:
        metrics.decrNumUnhealthyNMs();
        break;
    }

    switch (finalState) {
    case DECOMMISSIONED:
        metrics.incrDecommisionedNMs();
      break;
    case LOST:
      metrics.incrNumLostNMs();
      break;
    case REBOOTED:
      metrics.incrNumRebootedNMs();
      break;
    case UNHEALTHY:
      metrics.incrNumUnhealthyNMs();
      break;
    }
  }

  //处理该应用在该远程节点已经启动了
  private static void handleRunningAppOnNode(RMNodeImpl rmNode,
      RMContext context, ApplicationId appId, NodeId nodeId) {
	  
	//确保资源管理器一定有该节点
    RMApp app = context.getRMApps().get(appId);

    // if we failed getting app by appId, maybe something wrong happened, just
    // add the app to the finishedApplications list so that the app can be
    // cleaned up on the NM
    if (null == app) {
      LOG.warn("Cannot get RMApp by appId=" + appId
          + ", just added it to finishedApplications list for cleanup");
      rmNode.finishedApplications.add(appId);
      return;
    }
    context.getDispatcher().getEventHandler().handle(new RMAppRunningOnNodeEvent(appId, nodeId));
  }
  
  /**
   * 更新节点totalCapability总资源
   */
  private static void updateNodeResourceFromEvent(RMNodeImpl rmNode, 
     RMNodeResourceUpdateEvent event){
      ResourceOption resourceOption = event.getResourceOption();
      // Set resource on RMNode
      rmNode.totalCapability = resourceOption.getResource();
  }

  /**
   * 由new状态改成Runing状态,表示该节点在注册时候触发 
   */
  public static class AddNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler
      RMNodeStartedEvent startEvent = (RMNodeStartedEvent) event;
      List<NMContainerStatus> containers = null;

      String host = rmNode.nodeId.getHost();
      
      //key是host,value是RMNodeImpl,即已经在ResourceManager中激活的所有NodeManager节点与节点所在host的映射管理
      if (rmNode.context.getInactiveRMNodes().containsKey(host)) {
        // Old node rejoining 返回以前存在的节点
        RMNode previouRMNode = rmNode.context.getInactiveRMNodes().get(host);
        //移除以前的host对应的NodeManager节点
        rmNode.context.getInactiveRMNodes().remove(host);//如果该曾经是node是不活跃的,则现在活跃了,则从集合中删除
        rmNode.updateMetricsForRejoinedNode(previouRMNode.getState());
      } else {
        // Increment activeNodes explicitly because this is a new node.
        ClusterMetrics.getMetrics().incrNumActiveNodes();//加入统计全局中活跃的节点数量

        //活跃的容器集合
        containers = startEvent.getNMContainerStatuses();
        if (containers != null && !containers.isEmpty()) {
          for (NMContainerStatus container : containers) {
            if (container.getContainerState() == ContainerState.RUNNING) {//容器如果是运行中,则加入到缓存中
              rmNode.launchedContainers.add(container.getContainerId());
            }
          }
        }
      }
      
      //活跃的应用集合
      if (null != startEvent.getRunningApplications()) {
        for (ApplicationId appId : startEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }
      }

      //向调度器发起通知,哪些节点可用,以及哪些节点的哪些容器已经被加载了
      rmNode.context.getDispatcher().getEventHandler()
        .handle(new NodeAddedSchedulerEvent(rmNode, containers));
      rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_USABLE, rmNode));
    }
  }

  public static class ReconnectNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeReconnectEvent reconnectEvent = (RMNodeReconnectEvent) event;
      RMNode newNode = reconnectEvent.getReconnectedNode();
      rmNode.nodeManagerVersion = newNode.getNodeManagerVersion();
      List<ApplicationId> runningApps = reconnectEvent.getRunningApplications();
      boolean noRunningApps = 
          (runningApps == null) || (runningApps.size() == 0);
      
      // No application running on the node, so send node-removal event with 
      // cleaning up old container info.
      if (noRunningApps) {
        rmNode.nodeUpdateQueue.clear();
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeRemovedSchedulerEvent(rmNode));
        
        if (rmNode.getHttpPort() == newNode.getHttpPort()) {
          // Reset heartbeat ID since node just restarted.
          rmNode.getLastNodeHeartBeatResponse().setResponseId(0);
          if (rmNode.getState() != NodeState.UNHEALTHY) {
            // Only add new node if old state is not UNHEALTHY
            rmNode.context.getDispatcher().getEventHandler().handle(
                new NodeAddedSchedulerEvent(newNode));
          }
        } else {
          // Reconnected node differs, so replace old node and start new node
          switch (rmNode.getState()) {
            case RUNNING:
              ClusterMetrics.getMetrics().decrNumActiveNodes();
              break;
            case UNHEALTHY:
              ClusterMetrics.getMetrics().decrNumUnhealthyNMs();
              break;
            }
            rmNode.context.getRMNodes().put(newNode.getNodeID(), newNode);
            rmNode.context.getDispatcher().getEventHandler().handle(
                new RMNodeStartedEvent(newNode.getNodeID(), null, null));
        }
      } else {
        rmNode.httpPort = newNode.getHttpPort();
        rmNode.httpAddress = newNode.getHttpAddress();
        rmNode.totalCapability = newNode.getTotalCapability();
      
        // Reset heartbeat ID since node just restarted.
        rmNode.getLastNodeHeartBeatResponse().setResponseId(0);
      }

      if (null != reconnectEvent.getRunningApplications()) {
        for (ApplicationId appId : reconnectEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }
      }

      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodesListManagerEvent(
              NodesListManagerEventType.NODE_USABLE, rmNode));
      if (rmNode.getState().equals(NodeState.RUNNING)) {
        // Update scheduler node's capacity for reconnect node.
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeResourceUpdateSchedulerEvent(rmNode, 
                ResourceOption.newInstance(newNode.getTotalCapability(), -1)));
      }
      
    }
  }
  
  /**
   * 当前接收的请求是RMNodeEventType.RESOURCE_UPDATE,即更新资源,并且此时的节点状态是NodeState.RUNNING的时候才进入该函数
   * 1.更新节点totalCapability总资源
   * 2.触发事件SchedulerEventType.NODE_RESOURCE_UPDATE,通知调度器集群资源有变化
   */
  public static class UpdateNodeResourceWhenRunningTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeResourceUpdateEvent updateEvent = (RMNodeResourceUpdateEvent)event;
      updateNodeResourceFromEvent(rmNode, updateEvent);
      // Notify new resourceOption to scheduler
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeResourceUpdateSchedulerEvent(rmNode, updateEvent.getResourceOption()));
    }
  }
  
  /**
   * 当前接收的请求是RMNodeEventType.RESOURCE_UPDATE,即更新资源,并且此时的节点状态是NodeState.NEW,DECOMMISSIONED,LOST,REBOOTED的时候才进入该函数
   * 仅仅更新totalResource属性
   */
  public static class UpdateNodeResourceWhenUnusableTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // The node is not usable, only log a warn message
      LOG.warn("Try to update resource on a "+ rmNode.getState().toString() +
          " node: "+rmNode.toString());
      updateNodeResourceFromEvent(rmNode, (RMNodeResourceUpdateEvent)event);
      // No need to notify scheduler as schedulerNode is not function now
      // and can sync later from RMnode.
    }
  }
  
  /**
   * 状态更新
   * 在集群正常运转的情况下,产生的状态更新事件
   */
  public static class StatusUpdateWhenHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {
    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {

      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      //记录该节点的上一次心跳请求返回的信息
      rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();

      //记录上一次远程节点的健康检查统计信息
      NodeHealthStatus remoteNodeHealthStatus = statusEvent.getNodeHealthStatus();
      //记录远程节点上一次健康检查的报告信息,如果检查有问题,则会产生提示字符串
      rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
      //记录远程节点上一次健康检查的时间
      rmNode.setLastHealthReportTime(
          remoteNodeHealthStatus.getLastHealthReportTime());
      
      //说明远程节点已经不健康了
      if (!remoteNodeHealthStatus.getIsNodeHealthy()) {
        LOG.info("Node " + rmNode.nodeId + " reported UNHEALTHY with details: "
            + remoteNodeHealthStatus.getHealthReport());//报告检查失败的信息
        rmNode.nodeUpdateQueue.clear();
        // Inform the scheduler 当某一个节点不健康了,则从调度器移除该节点
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeRemovedSchedulerEvent(rmNode));
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodesListManagerEvent(
                NodesListManagerEventType.NODE_UNUSABLE, rmNode));
        // Update metrics
        rmNode.updateMetricsForDeactivatedNode(rmNode.getState(),
            NodeState.UNHEALTHY);
        return NodeState.UNHEALTHY;//返回不健康状态
      }

      // Filter the map to only obtain just launched containers and finished
      // containers.
      //本次状态更新,发现远程节点新启动的容器集合
      List<ContainerStatus> newlyLaunchedContainers = 
          new ArrayList<ContainerStatus>();
      //本次状态更新,发现远程节点已经完成的的容器集合
      List<ContainerStatus> completedContainers = 
          new ArrayList<ContainerStatus>();
      
      //循环每一个节点的容器信息
      for (ContainerStatus remoteContainer : statusEvent.getContainers()) {
        ContainerId containerId = remoteContainer.getContainerId();

        //如果该容器已经在调度器中被设置为去清理或者该容器对应的应用已经在调度器中设置成完成了,则都不在进一步处理了,因此continue
        // Don't bother with containers already scheduled for cleanup, or for
        // applications already killed. The scheduler doens't need to know any
        // more about this container
        if (rmNode.containersToClean.contains(containerId)) {
          LOG.info("Container " + containerId + " already scheduled for " +
          		"cleanup, no further processing");
          continue;
        }
        if (rmNode.finishedApplications.contains(containerId
            .getApplicationAttemptId().getApplicationId())) {
          LOG.info("Container " + containerId
              + " belongs to an application that is already killed,"
              + " no further processing");
          continue;
        }

        // Process running containers远程的该容器的状态是运行中,并且本地管理的容器说明没有被启动,则加入到启动容器中
        if (remoteContainer.getState() == ContainerState.RUNNING) {
          if (!rmNode.launchedContainers.contains(containerId)) {
            // Just launched container. RM knows about it the first time.
            rmNode.launchedContainers.add(containerId);
            newlyLaunchedContainers.add(remoteContainer);
          }
        } else {//如果该容器已经不是运行的,则从运行的容器中移除
          // A finished container
          rmNode.launchedContainers.remove(containerId);
          completedContainers.add(remoteContainer);
        }
      }
      
      //发现新的容器被跟踪到了,或者是刚刚启动,也或者是刚刚完成该容器
      if(newlyLaunchedContainers.size() != 0 
          || completedContainers.size() != 0) {
        rmNode.nodeUpdateQueue.add(new UpdatedContainerInfo
            (newlyLaunchedContainers, completedContainers));
      }
      
      
      //通知调度器,一些节点的容器有变更.即远程节点有新容器被启动或者老容器被完成
      if(rmNode.nextHeartBeat) {
        rmNode.nextHeartBeat = false;
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeUpdateSchedulerEvent(rmNode));
      }

      // Update DTRenewer in secure mode to keep these apps alive. Today this is
      // needed for log-aggregation to finish long after the apps are gone.
      if (UserGroupInformation.isSecurityEnabled()) {
        rmNode.context.getDelegationTokenRenewer().updateKeepAliveApplications(
          statusEvent.getKeepAliveAppIds());
      }

      //依然返回健康的运行状态
      return NodeState.RUNNING;
    }
  }
  
  /**
   * 状态更新
   * 1.记录远程节点最后请求的信息
   * 2.记录远程节点如果健康检查失败的话,返回什么失败内容
   * 3.记录远程节点最后一次检查健康的时间。
   * 4.如果现在远程节点已经健康了,则发送事件
   *   a.SchedulerEventType.NODE_ADDED 目的是通知调度器,集群中增加了一个节点
   *   b.NodesListManagerEventType.NODE_USABLE 目的是通知节点已经可以使用了
   */
  public static class StatusUpdateWhenUnHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      //记录该节点的上一次心跳请求返回的信息
      rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();
      //获取该节点是否健康的回复对象
      NodeHealthStatus remoteNodeHealthStatus = statusEvent.getNodeHealthStatus();
      //不健康的话,输出什么信息
      rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
      //更新远程节点最后一次健康检查时间
      rmNode.setLastHealthReportTime(
          remoteNodeHealthStatus.getLastHealthReportTime());
      
      if (remoteNodeHealthStatus.getIsNodeHealthy()) {//远程节点依然健康
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeAddedSchedulerEvent(rmNode));
        rmNode.context.getDispatcher().getEventHandler().handle(
                new NodesListManagerEvent(
                    NodesListManagerEventType.NODE_USABLE, rmNode));
        // ??? how about updating metrics before notifying to ensure that
        // notifiers get update metadata because they will very likely query it
        // upon notification
        // Update metrics
        rmNode.updateMetricsForRejoinedNode(NodeState.UNHEALTHY);
        return NodeState.RUNNING;//说明该节点依然健康
      }

      //说明该节点已经不健康了
      return NodeState.UNHEALTHY;
    }
  }
  
  public static class CleanUpAppTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.finishedApplications.add(((RMNodeCleanAppEvent) event).getAppId());
    }
  }

  public static class CleanUpContainerTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToClean.add(((
          RMNodeCleanContainerEvent) event).getContainerId());
    }
  }

  public static class AddContainersToBeRemovedFromNMTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToBeRemovedFromNM.addAll(((
          RMNodeFinishedContainersPulledByAMEvent) event).getContainers());
    }
  }

  public static class DeactivateNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    private final NodeState finalState;
    public DeactivateNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler
      rmNode.nodeUpdateQueue.clear();
      // If the current state is NodeState.UNHEALTHY
      // Then node is already been removed from the
      // Scheduler
      NodeState initialState = rmNode.getState();
      if (!initialState.equals(NodeState.UNHEALTHY)) {
        rmNode.context.getDispatcher().getEventHandler()
          .handle(new NodeRemovedSchedulerEvent(rmNode));
      }
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodesListManagerEvent(
              NodesListManagerEventType.NODE_UNUSABLE, rmNode));

      // Deactivate the node
      rmNode.context.getRMNodes().remove(rmNode.nodeId);
      LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
          + finalState);
      rmNode.context.getInactiveRMNodes().put(rmNode.nodeId.getHost(), rmNode);

      //Update the metrics
      rmNode.updateMetricsForDeactivatedNode(initialState, finalState);
    }
  }

  /**
   * 获取所有的远程节点刚刚新启动的容器以及完成的容器
   */
  @Override
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    List<UpdatedContainerInfo> latestContainerInfoList = 
        new ArrayList<UpdatedContainerInfo>();
    while(nodeUpdateQueue.peek() != null){
      latestContainerInfoList.add(nodeUpdateQueue.poll());
    }
    this.nextHeartBeat = true;
    return latestContainerInfoList;
  }

  @VisibleForTesting
  public void setNextHeartBeat(boolean nextHeartBeat) {
    this.nextHeartBeat = nextHeartBeat;
  }
  
  @VisibleForTesting
  public int getQueueSize() {
    return nodeUpdateQueue.size();
  }

  // For test only.
  @VisibleForTesting
  public Set<ContainerId> getLaunchedContainers() {
    return this.launchedContainers;
  }

  @Override
  public Set<String> getNodeLabels() {
    if (context.getNodeLabelManager() == null) {
      return CommonNodeLabelsManager.EMPTY_STRING_SET;
    }
    return context.getNodeLabelManager().getLabelsOnNode(nodeId);
  }
 }

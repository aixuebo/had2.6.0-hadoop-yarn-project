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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import com.google.common.annotations.VisibleForTesting;

/**
 * 统计和更新节点信息
 */
public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  public static final String YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS =
      YarnConfiguration.NM_PREFIX + "duration-to-track-stopped-containers";

  private static final Log LOG = LogFactory.getLog(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();

  private final Context context;
  private final Dispatcher dispatcher;

  private NodeId nodeId;//该节点id
  private long nextHeartBeatInterval;//该节点下一次向ResourceManager发送心跳的间隔
  private ResourceTracker resourceTracker;//远程ResourceManager代理
  private Resource totalResource;//该节点的总资源信息
  private int httpPort;//该节点端口
  private String nodeManagerVersionId;//yarn版本号
  private String minimumResourceManagerVersion;//resourceManager最小的版本
  private volatile boolean isStopped;
  private boolean tokenKeepAliveEnabled;
  private long tokenRemovalDelayMs;//多次时间还没有反应,则该节点说明已经死了
  /** Keeps track of when the next keep alive request should be sent for an app*/
  private Map<ApplicationId, Long> appTokenKeepAliveMap = new HashMap<ApplicationId, Long>();
  private Random keepAliveDelayRandom = new Random();
  // It will be used to track recently stopped containers on node manager, this
  // is to avoid the misleading no-such-container exception messages on NM, when
  // the AM finishes it informs the RM to stop the may-be-already-completed
  // containers.最近停止的容器缓存起来,key是容器ID,value是该容器删除的时间,允许这段时间内虽然容器停止了,但是该容器的信息还可以查询到
  private final Map<ContainerId, Long> recentlyStoppedContainers;
  // Duration for which to track recently stopped container.
  private long durationToTrackStoppedContainers;

  private final NodeHealthCheckerService healthChecker;
  private final NodeManagerMetrics metrics;

  private Runnable statusUpdaterRunnable;
  private Thread  statusUpdater;
  private long rmIdentifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;//resourceManaer的启动时间戳,当注册成功后就会返回该值
  /**
   * 心跳回复说可以移除的容器集合,先缓存起来,等待去移除
   */
  Set<ContainerId> pendingContainersToRemove = new HashSet<ContainerId>();

  public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
    super(NodeStatusUpdaterImpl.class.getName());
    this.healthChecker = healthChecker;
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    this.recentlyStoppedContainers = new LinkedHashMap<ContainerId, Long>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    int memoryMb = 
        conf.getInt(
            YarnConfiguration.NM_PMEM_MB, YarnConfiguration.DEFAULT_NM_PMEM_MB);//按M分配该节点的内存使用
    float vMemToPMem =             
        conf.getFloat(
            YarnConfiguration.NM_VMEM_PMEM_RATIO, 
            YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO); //虚拟内存使用比例
    int virtualMemoryMb = (int)Math.ceil(memoryMb * vMemToPMem);//虚拟内存使用大小
    
    int virtualCores =
        conf.getInt(
            YarnConfiguration.NM_VCORES, YarnConfiguration.DEFAULT_NM_VCORES);//该节点分配多少核

    this.totalResource = Resource.newInstance(memoryMb, virtualCores);//设置总资源
    metrics.addResource(totalResource);
    this.tokenKeepAliveEnabled = isTokenKeepAliveEnabled(conf);
    this.tokenRemovalDelayMs =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);

    this.minimumResourceManagerVersion = conf.get(
        YarnConfiguration.NM_RESOURCEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_NM_RESOURCEMANAGER_MINIMUM_VERSION);
    
    // Default duration to track stopped containers on nodemanager is 10Min.
    // This should not be assigned very large value as it will remember all the
    // containers stopped during that time.
    durationToTrackStoppedContainers =
        conf.getLong(YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
          600000);
    if (durationToTrackStoppedContainers < 0) {
      String message = "Invalid configuration for "
        + YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS + " default "
          + "value is 10Min(600000).";//60*10*1000= 10分钟
      LOG.error(message);
      throw new YarnException(message);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS + " :"
        + durationToTrackStoppedContainers);
    }
    super.serviceInit(conf);
    LOG.info("Initialized nodemanager for " + nodeId + ":" +
        " physical-memory=" + memoryMb + " virtual-memory=" + virtualMemoryMb +
        " virtual-cores=" + virtualCores);
  }

  @Override
  protected void serviceStart() throws Exception {

    // NodeManager is the last service to start, so NodeId is available.
    this.nodeId = this.context.getNodeId();
    this.httpPort = this.context.getHttpPort();
    this.nodeManagerVersionId = YarnVersionInfo.getVersion();
    try {
      // Registration has to be in start so that ContainerManager can get the
      // perNM tokens needed to authenticate ContainerTokens.
      //获取远程ResourceManager代理
      this.resourceTracker = getRMClient();
      //向远程ResourceManager注册节点
      registerWithRM();
      super.serviceStart();
      startStatusUpdater();
    } catch (Exception e) {
      String errorMessage = "Unexpected error starting NodeStatusUpdater";
      LOG.error(errorMessage, e);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    // Interrupt the updater.
    this.isStopped = true;
    stopRMProxy();
    super.serviceStop();
  }

  protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
    // Interrupt the updater.
    this.isStopped = true;

    try {
      statusUpdater.join();
      registerWithRM();
      statusUpdater = new Thread(statusUpdaterRunnable, "Node Status Updater");
      this.isStopped = false;
      statusUpdater.start();
      LOG.info("NodeStatusUpdater thread is reRegistered and restarted");
    } catch (Exception e) {
      String errorMessage = "Unexpected error rebooting NodeStatusUpdater";
      LOG.error(errorMessage, e);
      throw new YarnRuntimeException(e);
    }
  }

  @VisibleForTesting
  protected void stopRMProxy() {
    if(this.resourceTracker != null) {
      RPC.stopProxy(this.resourceTracker);
    }
  }

  @Private
  protected boolean isTokenKeepAliveEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)
        && UserGroupInformation.isSecurityEnabled();
  }

  /**
   * 生成远程ResourceManager代理
   */
  @VisibleForTesting
  protected ResourceTracker getRMClient() throws IOException {
    Configuration conf = getConfig();
    return ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
  }

  /**
   * 向远程ResourceManager注册节点
   */
  @VisibleForTesting
  protected void registerWithRM() throws YarnException, IOException {
    List<NMContainerStatus> containerReports = getNMContainerStatuses();
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(nodeId, httpPort, totalResource,
          nodeManagerVersionId, containerReports, getRunningApplications());
    if (containerReports != null) {
      LOG.info("Registering with RM using containers :" + containerReports);
    }
    //发送请求注册节点,返回信息
    RegisterNodeManagerResponse regNMResponse = resourceTracker.registerNodeManager(request);
    this.rmIdentifier = regNMResponse.getRMIdentifier();
    // if the Resourcemanager instructs NM to shutdown.
    if (NodeAction.SHUTDOWN.equals(regNMResponse.getNodeAction())) {//注册异常,则抛异常
      String message =
          "Message from ResourceManager: "
              + regNMResponse.getDiagnosticsMessage();
      throw new YarnRuntimeException(
        "Recieved SHUTDOWN signal from Resourcemanager ,Registration of NodeManager failed, "
            + message);
    }

    // if ResourceManager version is too old then shutdown
    if (!minimumResourceManagerVersion.equals("NONE")){
      if (minimumResourceManagerVersion.equals("EqualToNM")){
        minimumResourceManagerVersion = nodeManagerVersionId;
      }
      String rmVersion = regNMResponse.getRMVersion();//resourceMamager的版本号
      if (rmVersion == null) {//不允许为空,一定有该值
        String message = "The Resource Manager's did not return a version. "
            + "Valid version cannot be checked.";
        throw new YarnRuntimeException("Shutting down the Node Manager. "
            + message);
      }
      if (VersionUtil.compareVersions(rmVersion,minimumResourceManagerVersion) < 0) {//如果比该值小,则抛异常
        String message = "The Resource Manager's version ("
            + rmVersion +") is less than the minimum "
            + "allowed version " + minimumResourceManagerVersion;
        throw new YarnRuntimeException("Shutting down the Node Manager on RM "
            + "version error, " + message);
      }
    }
    //注册校验信息
    MasterKey masterKey = regNMResponse.getContainerTokenMasterKey();
    // do this now so that its set before we start heartbeating to RM
    // It is expected that status updater is started by this point and
    // RM gives the shared secret in registration during
    // StatusUpdater#start().
    if (masterKey != null) {
      this.context.getContainerTokenSecretManager().setMasterKey(masterKey);
    }
    
    masterKey = regNMResponse.getNMTokenMasterKey();
    if (masterKey != null) {
      this.context.getNMTokenSecretManager().setMasterKey(masterKey);
    }

    LOG.info("Registered with ResourceManager as " + this.nodeId
        + " with total resource of " + this.totalResource);
    LOG.info("Notifying ContainerManager to unblock new container-requests");
    //设置可以允许有新容器请求过来
    ((ContainerManagerImpl) this.context.getContainerManager()).setBlockNewContainerRequests(false);
  }

  /**
   * 查找活跃的应用
   */
  private List<ApplicationId> createKeepAliveApplicationList() {
    if (!tokenKeepAliveEnabled) {
      return Collections.emptyList();
    }

    List<ApplicationId> appList = new ArrayList<ApplicationId>();
    for (Iterator<Entry<ApplicationId, Long>> i = this.appTokenKeepAliveMap.entrySet().iterator(); i.hasNext();) {
      Entry<ApplicationId, Long> e = i.next();
      ApplicationId appId = e.getKey();
      Long nextKeepAlive = e.getValue();
      if (!this.context.getApplications().containsKey(appId)) {
        // Remove if the application has finished.
        i.remove();
      } else if (System.currentTimeMillis() > nextKeepAlive) {
        // KeepAlive list for the next hearbeat.
        appList.add(appId);
        trackAppForKeepAlive(appId);
      }
    }
    return appList;
  }

  /**
   * 节点的一些元数据信息
   */
  private NodeStatus getNodeStatus(int responseId) throws IOException {

    /**
     * 存放该节点的健康信息
     */
    NodeHealthStatus nodeHealthStatus = this.context.getNodeHealthStatus();
    nodeHealthStatus.setHealthReport(healthChecker.getHealthReport());
    nodeHealthStatus.setIsNodeHealthy(healthChecker.isHealthy());
    nodeHealthStatus.setLastHealthReportTime(healthChecker.getLastHealthReportTime());
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node's health-status : " + nodeHealthStatus.getIsNodeHealthy()
          + ", " + nodeHealthStatus.getHealthReport());
    }
    //获取当前容器的状态
    List<ContainerStatus> containersStatuses = getContainerStatuses();
    NodeStatus nodeStatus =
        NodeStatus.newInstance(nodeId, responseId, containersStatuses,
          createKeepAliveApplicationList(), nodeHealthStatus);

    return nodeStatus;
  }

  // Iterate through the NMContext and clone and get all the containers'
  // statuses. If it's a completed container, add into the
  // recentlyStoppedContainers collections.
  //获取当前的容器集合
  @VisibleForTesting
  protected List<ContainerStatus> getContainerStatuses() throws IOException {
    List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
    for (Container container : this.context.getContainers().values()) {
      ContainerId containerId = container.getContainerId();
      ApplicationId applicationId = container.getContainerId().getApplicationAttemptId().getApplicationId();
      org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
      containerStatuses.add(containerStatus);
      //当容器完成的时候,在上下文中删除该容器
      if (containerStatus.getState() == ContainerState.COMPLETE) {
        if (isApplicationStopped(applicationId)) {//该应用已经完成
          if (LOG.isDebugEnabled()) {
            LOG.debug(applicationId + " is completing, " + " remove "
                + containerId + " from NM context.");
          }
          context.getContainers().remove(containerId);
        } else {
          // Adding to finished containers cache. Cache will keep it around at
          // least for #durationToTrackStoppedContainers duration. In the
          // subsequent call to stop container it will get removed from cache.
          addCompletedContainer(container.getContainerId());
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending out " + containerStatuses.size()
          + " container statuses: " + containerStatuses);
    }
    return containerStatuses;
  }
  
  /**
   * 返回正在运行的应用集合
   */
  private List<ApplicationId> getRunningApplications() {
    List<ApplicationId> runningApplications = new ArrayList<ApplicationId>();
    runningApplications.addAll(this.context.getApplications().keySet());
    return runningApplications;
  }

  // These NMContainerStatus are sent on NM registration and used by YARN only.
  //在注册的时候发送一些信息,但是我觉得注册的时候还没有容器呢，因此不太理解为什么容器还要循环,难道是未来还原操作么?
  private List<NMContainerStatus> getNMContainerStatuses() throws IOException {
    List<NMContainerStatus> containerStatuses = new ArrayList<NMContainerStatus>();
    for (Container container : this.context.getContainers().values()) {
      ContainerId containerId = container.getContainerId();
      ApplicationId applicationId = container.getContainerId().getApplicationAttemptId().getApplicationId();
      if (!this.context.getApplications().containsKey(applicationId)) {//已经没有该应用了,则删除该容器
        context.getContainers().remove(containerId);
        continue;
      }
      NMContainerStatus status = container.getNMContainerStatus();
      containerStatuses.add(status);
      if (status.getContainerState() == ContainerState.COMPLETE) {
        // Adding to finished containers cache. Cache will keep it around at
        // least for #durationToTrackStoppedContainers duration. In the
        // subsequent call to stop container it will get removed from cache.
        addCompletedContainer(container.getContainerId());
      }
    }
    LOG.info("Sending out " + containerStatuses.size()
      + " NM container statuses: " + containerStatuses);
    return containerStatuses;
  }

  /**
   * 判断该应用是否已经停止
   */
  private boolean isApplicationStopped(ApplicationId applicationId) {
    if (!this.context.getApplications().containsKey(applicationId)) {//没有该应用,则返回true
      return true;
    }

    ApplicationState applicationState = this.context.getApplications().get(applicationId).getApplicationState();
    if (applicationState == ApplicationState.FINISHING_CONTAINERS_WAIT
        || applicationState == ApplicationState.APPLICATION_RESOURCES_CLEANINGUP
        || applicationState == ApplicationState.FINISHED) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * 容器完成后再节点上缓存一段时间,然后在删除
   * 允许这段时间内虽然容器停止了,但是该容器的信息还可以查询到
   */
  @Override
  public void addCompletedContainer(ContainerId containerId) {
    synchronized (recentlyStoppedContainers) {
      removeVeryOldStoppedContainersFromCache();//删除缓存的完成容器
      if (!recentlyStoppedContainers.containsKey(containerId)) {
        recentlyStoppedContainers.put(containerId,
            System.currentTimeMillis() + durationToTrackStoppedContainers);
      }
    }
  }

  /**
   * 心跳回复说可以移除的容器集合
   */
  @VisibleForTesting
  @Private
  public void removeOrTrackCompletedContainersFromContext(List<ContainerId> containerIds) throws IOException {
    //存储真正移除的容器
    Set<ContainerId> removedContainers = new HashSet<ContainerId>();
    //心跳回复说可以移除的容器集合,先缓存起来,等待去移除
    pendingContainersToRemove.addAll(containerIds);
    Iterator<ContainerId> iter = pendingContainersToRemove.iterator();
    //真正去移除这些ResourceManager通知给节点说已经完成的容器
    while (iter.hasNext()) {
      ContainerId containerId = iter.next();
      // remove the container only if the container is at DONE state 移除这些容器,仅仅是这些容器在节点上状态是done,其实这个肯定是的,因为ResourceManager都已经知道是DONE了,肯定是节点告诉ResourceManager的
      Container nmContainer = context.getContainers().get(containerId);
      if (nmContainer != null && nmContainer.getContainerState().equals(
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE)) {
        //从上下文中移除该容器
        context.getContainers().remove(containerId);
        //添加真正移除的容器
        removedContainers.add(containerId);
        iter.remove();
      }
    }

    if (!removedContainers.isEmpty()) {
      LOG.info("Removed completed containers from NM context: "
          + removedContainers);
    }
  }

  private void trackAppsForKeepAlive(List<ApplicationId> appIds) {
    if (tokenKeepAliveEnabled && appIds != null && appIds.size() > 0) {
      for (ApplicationId appId : appIds) {
        trackAppForKeepAlive(appId);
      }
    }
  }

  private void trackAppForKeepAlive(ApplicationId appId) {
    // Next keepAlive request for app between 0.7 & 0.9 of when the token will
    // likely expire.
    long nextTime = System.currentTimeMillis()
    + (long) (0.7 * tokenRemovalDelayMs + (0.2 * tokenRemovalDelayMs
        * keepAliveDelayRandom.nextInt(100))/100);
    appTokenKeepAliveMap.put(appId, nextTime);
  }

  @Override
  public void sendOutofBandHeartBeat() {
    synchronized (this.heartbeatMonitor) {
      this.heartbeatMonitor.notify();
    }
  }

  /**
   * 查看该容器是否是在最近停止的容器中
   */
  public boolean isContainerRecentlyStopped(ContainerId containerId) {
    synchronized (recentlyStoppedContainers) {
      return recentlyStoppedContainers.containsKey(containerId);
    }
  }

  /**
   * 清理已经完成的容器在缓存中的数据
   */
  @Override
  public void clearFinishedContainersFromCache() {
    synchronized (recentlyStoppedContainers) {
      recentlyStoppedContainers.clear();
    }
  }
  
  /**
   * 移除很久以前停止的容器,但是该容器被缓存起来了
   * 允许这段时间内虽然容器停止了,但是该容器的信息还可以查询到
   */
  @Private
  @VisibleForTesting
  public void removeVeryOldStoppedContainersFromCache() {
    synchronized (recentlyStoppedContainers) {
      long currentTime = System.currentTimeMillis();
      Iterator<ContainerId> i = recentlyStoppedContainers.keySet().iterator();
      while (i.hasNext()) {
        ContainerId cid = i.next();
        if (recentlyStoppedContainers.get(cid) < currentTime) {
          if (!context.getContainers().containsKey(cid)) {
            i.remove();
            try {
              context.getNMStateStore().removeContainer(cid);
            } catch (IOException e) {
              LOG.error("Unable to remove container " + cid + " in store", e);
            }
          }
        } else {
          break;
        }
      }
    }
  }
  
  @Override
  public long getRMIdentifier() {
    return this.rmIdentifier;
  }

  private static Map<ApplicationId, Credentials> parseCredentials(Map<ApplicationId, ByteBuffer> systemCredentials) throws IOException {
    Map<ApplicationId, Credentials> map = new HashMap<ApplicationId, Credentials>();
    for (Map.Entry<ApplicationId, ByteBuffer> entry : systemCredentials.entrySet()) {
      Credentials credentials = new Credentials();
      DataInputByteBuffer buf = new DataInputByteBuffer();
      ByteBuffer buffer = entry.getValue();
      buffer.rewind();
      buf.reset(buffer);
      credentials.readTokenStorageStream(buf);
      map.put(entry.getKey(), credentials);
    }
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<ApplicationId, Credentials> entry : map.entrySet()) {
        LOG.debug("Retrieved credentials form RM for " + entry.getKey() + ": "
            + entry.getValue().getAllTokens());
      }
    }
    return map;
  }

  protected void startStatusUpdater() {

    statusUpdaterRunnable = new Runnable() {
      @Override
      @SuppressWarnings("unchecked")
      public void run() {
        int lastHeartBeatID = 0;
        while (!isStopped) {
          // Send heartbeat
          try {
            //发送心跳请求后的结果
            NodeHeartbeatResponse response = null;
            //收集节点的一些元数据信息
            NodeStatus nodeStatus = getNodeStatus(lastHeartBeatID);
            
            //发送心跳请求,因为该类是内部类,所以调用外部类的属性要使用NodeStatusUpdaterImpl.this.context格式
            NodeHeartbeatRequest request =
                NodeHeartbeatRequest.newInstance(nodeStatus,
                  NodeStatusUpdaterImpl.this.context.getContainerTokenSecretManager().getCurrentKey(),
                  NodeStatusUpdaterImpl.this.context.getNMTokenSecretManager().getCurrentKey());
            response = resourceTracker.nodeHeartbeat(request);
            //get next heartbeat interval from response 设置下一次发送心跳的时间
            nextHeartBeatInterval = response.getNextHeartBeatInterval();
            //设置密钥
            updateMasterKeys(response);

            if (response.getNodeAction() == NodeAction.SHUTDOWN) {//接收到shutdown命令
              LOG.warn("Recieved SHUTDOWN signal from Resourcemanager as part of heartbeat,"
                    + " hence shutting down.");
              LOG.warn("Message from ResourceManager: " + response.getDiagnosticsMessage());
              context.setDecommissioned(true);
              dispatcher.getEventHandler().handle(new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
              break;
            }
            if (response.getNodeAction() == NodeAction.RESYNC) {
              LOG.warn("Node is out of sync with ResourceManager,"
                  + " hence resyncing.");
              LOG.warn("Message from ResourceManager: " + response.getDiagnosticsMessage());
              // Invalidate the RMIdentifier while resync
              NodeStatusUpdaterImpl.this.rmIdentifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;
              dispatcher.getEventHandler().handle(new NodeManagerEvent(NodeManagerEventType.RESYNC));
              break;
            }

            // Explicitly put this method after checking the resync response. We
            // don't want to remove the completed containers before resync
            // because these completed containers will be reported back to RM
            // when NM re-registers with RM.
            // Only remove the cleanedup containers that are acked
            //心跳回复说可以移除的容器集合
            removeOrTrackCompletedContainersFromContext(response.getContainersToBeRemovedFromNM());

            lastHeartBeatID = response.getResponseId();
            List<ContainerId> containersToCleanup = response.getContainersToCleanup();
            if (!containersToCleanup.isEmpty()) {
              dispatcher.getEventHandler().handle(new CMgrCompletedContainersEvent(containersToCleanup,CMgrCompletedContainersEvent.Reason.BY_RESOURCEMANAGER));
            }
            List<ApplicationId> appsToCleanup = response.getApplicationsToCleanup();
            //Only start tracking for keepAlive on FINISH_APP
            trackAppsForKeepAlive(appsToCleanup);
            if (!appsToCleanup.isEmpty()) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedAppsEvent(appsToCleanup,
                      CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER));
            }

            Map<ApplicationId, ByteBuffer> systemCredentials = response.getSystemCredentialsForApps();
            if (systemCredentials != null && !systemCredentials.isEmpty()) {
              ((NMContext) context).setSystemCrendentialsForApps(parseCredentials(systemCredentials));
            }
          } catch (ConnectException e) {
            //catch and throw the exception if tried MAX wait time to connect RM
            dispatcher.getEventHandler().handle(new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
            throw new YarnRuntimeException(e);
          } catch (Throwable e) {

            // TODO Better error handling. Thread can die with the rest of the
            // NM still running.
            LOG.error("Caught exception in status-updater", e);
          } finally {
            synchronized (heartbeatMonitor) {
              nextHeartBeatInterval = nextHeartBeatInterval <= 0 ?
                  YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS :
                    nextHeartBeatInterval;
              try {
                heartbeatMonitor.wait(nextHeartBeatInterval);
              } catch (InterruptedException e) {
                // Do Nothing
              }
            }
          }
        }
      }

      /**
       * 更新心跳后ResourceManager发过来的密钥
       */
      private void updateMasterKeys(NodeHeartbeatResponse response) {
        // See if the master-key has rolled over
        MasterKey updatedMasterKey = response.getContainerTokenMasterKey();
        if (updatedMasterKey != null) {
          // Will be non-null only on roll-over on RM side
          context.getContainerTokenSecretManager().setMasterKey(updatedMasterKey);
        }
        
        updatedMasterKey = response.getNMTokenMasterKey();
        if (updatedMasterKey != null) {
          context.getNMTokenSecretManager().setMasterKey(updatedMasterKey);
        }
      }
    };
    statusUpdater = new Thread(statusUpdaterRunnable, "Node Status Updater");
    statusUpdater.start();
  }
  
  
}

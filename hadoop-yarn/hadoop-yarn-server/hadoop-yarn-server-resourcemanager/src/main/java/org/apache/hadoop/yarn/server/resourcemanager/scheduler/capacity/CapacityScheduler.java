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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueNotFoundException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

/**
 * CapacityScheduler:计算能力调度器,意义是先选择资源利用率低的queue，然后在queue中同时考虑FIFO和memory constraint因素
 */
@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class CapacityScheduler extends
    AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode> implements
    PreemptableResourceScheduler, CapacitySchedulerContext, Configurable {

  private static final Log LOG = LogFactory.getLog(CapacityScheduler.class);

  private CSQueue root;
  // timeout to join when we stop this service
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  //队列比较器,先比较队列的使用量,再比较队列的path
  static final Comparator<CSQueue> queueComparator = new Comparator<CSQueue>() {
    @Override
    public int compare(CSQueue q1, CSQueue q2) {
      if (q1.getUsedCapacity() < q2.getUsedCapacity()) {
        return -1;
      } else if (q1.getUsedCapacity() > q2.getUsedCapacity()) {
        return 1;
      }

      return q1.getQueuePath().compareTo(q2.getQueuePath());
    }
  };

  //应用比较器,比较应用的ID即可
  static final Comparator<FiCaSchedulerApp> applicationComparator = 
    new Comparator<FiCaSchedulerApp>() {
    @Override
    public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
      return a1.getApplicationId().compareTo(a2.getApplicationId());
    }
  };

  @Override
  public void setConf(Configuration conf) {
      yarnConf = conf;
  }
  
  /**
   * 校验最大、最下内存、cpu配置
   */
  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }

    // validate scheduler vcores allocation setting
    int minVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores <= 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
        + "=" + minVcores
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
        + "=" + maxVcores + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }

  @Override
  public Configuration getConf() {
    return yarnConf;
  }

  private CapacitySchedulerConfiguration conf;//加载capacity-scheduler.xml配置文件
  private Configuration yarnConf;

  //解析配置文件后,所有的队列的name与队列之间的映射关系
  private Map<String, CSQueue> queues = new ConcurrentHashMap<String, CSQueue>();

  //记录集群中节点数量
  private AtomicInteger numNodeManagers = new AtomicInteger(0);

  private ResourceCalculator calculator;
  private boolean usePortForNodeName;

  private boolean scheduleAsynchronously;
  private AsyncScheduleThread asyncSchedulerThread;
  private RMNodeLabelsManager labelManager;
  
  /**
   * EXPERT
   */
  private long asyncScheduleInterval;
  //yarn.scheduler.capacity.schedule-asynchronously.scheduling-interval-ms
  private static final String ASYNC_SCHEDULER_INTERVAL =
      CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
          + ".scheduling-interval-ms";
  private static final long DEFAULT_ASYNC_SCHEDULER_INTERVAL = 5;
  
  private boolean overrideWithQueueMappings = false;//是否启动user、group映射到组特性,yarn.scheduler.capacity.queue-mappings-override.enable,值是boolean类型
  private List<QueueMapping> mappings = null;//获取每一个user或者group对应使用哪个队列
  private Groups groups;//系统中所有的组信息

  @VisibleForTesting
  public synchronized String getMappedQueueForTest(String user)
      throws IOException {
    return getMappedQueue(user);
  }

  public CapacityScheduler() {
    super(CapacityScheduler.class.getName());
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return root.getMetrics();
  }

  public CSQueue getRootQueue() {
    return root;
  }
  
  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.rmContext.getContainerTokenSecretManager();
  }

  @Override
  public Comparator<FiCaSchedulerApp> getApplicationComparator() {
    return applicationComparator;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return calculator;
  }

  @Override
  public Comparator<CSQueue> getQueueComparator() {
    return queueComparator;
  }

  @Override
  public int getNumClusterNodes() {
    return numNodeManagers.get();
  }

  @Override
  public synchronized RMContext getRMContext() {
    return this.rmContext;
  }

  @Override
  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  private synchronized void initScheduler(Configuration configuration) throws IOException {
      
    this.conf = loadCapacitySchedulerConfiguration(configuration);//加载capacity-scheduler.xml配置文件
    validateConf(this.conf);
    this.minimumAllocation = this.conf.getMinimumAllocation();
    this.maximumAllocation = this.conf.getMaximumAllocation();
    this.calculator = this.conf.getResourceCalculator();
    this.usePortForNodeName = this.conf.getUsePortForNodeName();
    this.applications =
        new ConcurrentHashMap<ApplicationId,
            SchedulerApplication<FiCaSchedulerApp>>();
    this.labelManager = rmContext.getNodeLabelManager();

    initializeQueues(this.conf);

    scheduleAsynchronously = this.conf.getScheduleAynschronously();
    asyncScheduleInterval =
        this.conf.getLong(ASYNC_SCHEDULER_INTERVAL,
            DEFAULT_ASYNC_SCHEDULER_INTERVAL);
    if (scheduleAsynchronously) {
      asyncSchedulerThread = new AsyncScheduleThread(this);
    }

    LOG.info("Initialized CapacityScheduler with " +
        "calculator=" + getResourceCalculator().getClass() + ", " +
        "minimumAllocation=<" + getMinimumResourceCapability() + ">, " +
        "maximumAllocation=<" + getMaximumResourceCapability() + ">, " +
        "asynchronousScheduling=" + scheduleAsynchronously + ", " +
        "asyncScheduleInterval=" + asyncScheduleInterval + "ms");
  }

  /**
   * 启动定时线程
   */
  private synchronized void startSchedulerThreads() {
    if (scheduleAsynchronously) {
      Preconditions.checkNotNull(asyncSchedulerThread,
          "asyncSchedulerThread is null");
      asyncSchedulerThread.start();
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Configuration configuration = new Configuration(conf);
    initScheduler(configuration);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    startSchedulerThreads();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    synchronized (this) {
      if (scheduleAsynchronously && asyncSchedulerThread != null) {
        asyncSchedulerThread.interrupt();
        asyncSchedulerThread.join(THREAD_JOIN_TIMEOUT_MS);
      }
    }
    super.serviceStop();
  }

  @Override
  public synchronized void
  reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    Configuration configuration = new Configuration(conf);
    CapacitySchedulerConfiguration oldConf = this.conf;
    this.conf = loadCapacitySchedulerConfiguration(configuration);
    validateConf(this.conf);
    try {
      LOG.info("Re-initializing queues...");
      reinitializeQueues(this.conf);
    } catch (Throwable t) {
      this.conf = oldConf;
      throw new IOException("Failed to re-init queues", t);
    }
  }
  
  long getAsyncScheduleInterval() {
    return asyncScheduleInterval;
  }

  private final static Random random = new Random(System.currentTimeMillis());
  
  /**
   * Schedule on all nodes by starting at a random point.从随机一个位置,开始调度所有的节点
   * @param cs
   * 依次向每一个Node节点派发任务
   */
  static void schedule(CapacityScheduler cs) {
    // First randomize the start point
    int current = 0;
    Collection<FiCaSchedulerNode> nodes = cs.getAllNodes().values();
    int start = random.nextInt(nodes.size());//随机生成一个序号
    for (FiCaSchedulerNode node : nodes) {
      if (current++ >= start) {//从随机生成的序号位置开始执行
        cs.allocateContainersToNode(node);
      }
    }
    // Now, just get everyone to be safe
    for (FiCaSchedulerNode node : nodes) {
      cs.allocateContainersToNode(node);
    }
    try {
      Thread.sleep(cs.getAsyncScheduleInterval());
    } catch (InterruptedException e) {}
  }
  
  static class AsyncScheduleThread extends Thread {

    private final CapacityScheduler cs;
    private AtomicBoolean runSchedules = new AtomicBoolean(false);

    public AsyncScheduleThread(CapacityScheduler cs) {
      this.cs = cs;
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        if (!runSchedules.get()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {}
        } else {
          schedule(cs);
        }
      }
    }

    public void beginSchedule() {
      runSchedules.set(true);
    }

    public void suspendSchedule() {
      runSchedules.set(false);
    }

  }
  
  //yarn.scheduler.capacity.root
  @Private
  public static final String ROOT_QUEUE = CapacitySchedulerConfiguration.PREFIX + CapacitySchedulerConfiguration.ROOT; 

  static class QueueHook {
    public CSQueue hook(CSQueue queue) {
      return queue;
    }
  }
  private static final QueueHook noop = new QueueHook();

  /**
   * 初始化user、或者group映射到队列
   * @throws IOException
   */
  private void initializeQueueMappings() throws IOException {
    overrideWithQueueMappings = conf.getOverrideWithQueueMappings();//是否启动user、group映射到组特性
    LOG.info("Initialized queue mappings, override: "
        + overrideWithQueueMappings);
    // Get new user/group mappings
    List<QueueMapping> newMappings = conf.getQueueMappings();//获取每一个user或者group对应使用哪个队列
    //check if mappings refer to valid queues 校验每一个配置的合法性,该节点必须是叶子节点
    for (QueueMapping mapping : newMappings) {
      if (!mapping.queue.equals(CURRENT_USER_MAPPING) &&
          !mapping.queue.equals(PRIMARY_GROUP_MAPPING)) {
        CSQueue queue = queues.get(mapping.queue);
        if (queue == null || !(queue instanceof LeafQueue)) {
          throw new IOException(
              "mapping contains invalid or non-leaf queue " + mapping.queue);
        }
      }
    }
    //apply the new mappings since they are valid
    mappings = newMappings;
    // initialize groups if mappings are present
    if (mappings.size() > 0) {
      groups = new Groups(conf);
    }
  }

  @Lock(CapacityScheduler.class)
  private void initializeQueues(CapacitySchedulerConfiguration conf)
    throws IOException {
    root = parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT,queues, queues, noop);//解析配置文件
    labelManager.reinitializeQueueLabels(getQueueToLabels());
    LOG.info("Initialized root queue " + root);
    initializeQueueMappings();//初始化user、或者group映射到队列
  }

  @Lock(CapacityScheduler.class)
  private void reinitializeQueues(CapacitySchedulerConfiguration conf) 
  throws IOException {
    // Parse new queues
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot = parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT,newQueues, queues, noop); 
    
    // Ensure all existing queues are still present校验老集合中非ReservationQueue的队列必须在集合中依然存在
    validateExistingQueues(queues, newQueues);

    // Add new queues 重新加载队列后,会可能有新的队列被添加到新的配置文件中,因此这部分主要是整合新老队列
    addNewQueues(queues, newQueues);
    
    // Re-configure queues
    root.reinitialize(newRoot, clusterResource);
    initializeQueueMappings();

    // Re-calculate headroom for active applications
    root.updateClusterResource(clusterResource);

    labelManager.reinitializeQueueLabels(getQueueToLabels());
  }
  
  /**
   * 返回每一个队列对应的标签集合
   * key是队列的name,value是队列的标签集合
   */
  private Map<String, Set<String>> getQueueToLabels() {
    Map<String, Set<String>> queueToLabels = new HashMap<String, Set<String>>();
    for (CSQueue queue : queues.values()) {
      queueToLabels.put(queue.getQueueName(), queue.getAccessibleNodeLabels());
    }
    return queueToLabels;
  }

  /**
   * Ensure all existing queues are present. Queues cannot be deleted
   * @param queues existing queues 老配置文件加载后的队列集合
   * @param newQueues new queues 新加载的配置文件的队列集合
   * 校验老集合中非ReservationQueue的队列必须在集合中依然存在
   * 重新加载队列后,会可能有新的队列被添加到新的配置文件中,因此这部分主要是整合新老队列
   */
  @Lock(CapacityScheduler.class)
  private void validateExistingQueues(Map<String, CSQueue> queues, Map<String, CSQueue> newQueues) throws IOException {
    // check that all static queues are included in the newQueues list
    for (Map.Entry<String, CSQueue> e : queues.entrySet()) {//循环老的集合
      if (!(e.getValue() instanceof ReservationQueue)) {
        if (!newQueues.containsKey(e.getKey())) {
          throw new IOException(e.getKey() + " cannot be found during refresh!");
        }
      }
    }
  }

  /**
   * Add the new queues (only) to our list of queues...
   * ... be careful, do not overwrite existing queues.
   * @param queues 老配置文件加载后的队列集合
   * @param newQueues 新加载的配置文件的队列集合
   * 添加一些队列集合,新集合中不会覆盖老集合信息
   * 重新加载队列后,会可能有新的队列被添加到新的配置文件中,因此这部分主要是整合新老队列
   */
  @Lock(CapacityScheduler.class)
  private void addNewQueues(Map<String, CSQueue> queues, Map<String, CSQueue> newQueues){ 
    for (Map.Entry<String, CSQueue> e : newQueues.entrySet()) {
      String queueName = e.getKey();
      CSQueue queue = e.getValue();
      if (!queues.containsKey(queueName)) {
        queues.put(queueName, queue);
      }
    }
  }
  
  /**
   * 解析配置文件递归生成所有队列集合映射关系
   * @param csContext
   * @param conf
   * @param parent
   * @param queueName
   * @param queues 最终新城的所有队列映射集合
   * @param oldQueues 老队列用于重新加载配置文件的时候,老队列表示上一次读取配置文件后加载的队列
   * @param hook
   * @return
   * @throws IOException
   */
  @Lock(CapacityScheduler.class)
  static CSQueue parseQueue(
      CapacitySchedulerContext csContext,
      CapacitySchedulerConfiguration conf, 
      CSQueue parent, String queueName, Map<String, CSQueue> queues,
      Map<String, CSQueue> oldQueues, 
      QueueHook hook) throws IOException {
    CSQueue queue;
    //根据父节点和本节点名称,解析去路径
    String fullQueueName = (parent == null) ? queueName : (parent.getQueuePath() + "." + queueName);
    //获取该节点下所有子节点集合
    String[] childQueueNames = conf.getQueues(fullQueueName); 
    //该队列是否支持预约
    boolean isReservableQueue = conf.isReservable(fullQueueName);
    //没有子队列
    if (childQueueNames == null || childQueueNames.length == 0) {
      if (null == parent) {//没有子队列,说明一定存在父队列
        throw new IllegalStateException(
            "Queue configuration missing child queue names for " + queueName);
      }
      // Check if the queue will be dynamically managed by the Reservation
      // system
      if (isReservableQueue) {//支持预约
        queue = new PlanQueue(csContext, queueName, parent,oldQueues.get(queueName));
      } else {
        queue = new LeafQueue(csContext, queueName, parent,oldQueues.get(queueName));
        // Used only for unit tests
        queue = hook.hook(queue);
      }
    } else {//说明设置了子节点,因此该节点属于父节点
      //仅仅叶子节点才允许设置预留,非叶子节点不允许,因此抛异常
      if (isReservableQueue) {
        throw new IllegalStateException(
            "Only Leaf Queues can be reservable for " + queueName);
      }
      ParentQueue parentQueue = new ParentQueue(csContext, queueName, parent,oldQueues.get(queueName)); 
      // Used only for unit tests
      queue = hook.hook(parentQueue);

      List<CSQueue> childQueues = new ArrayList<CSQueue>();
      for (String childQueueName : childQueueNames) {
        CSQueue childQueue = parseQueue(csContext, conf, queue, childQueueName,queues, oldQueues, hook); 
        childQueues.add(childQueue);
      }
      parentQueue.setChildQueues(childQueues);
    }

    /**
     * 不允许叶子节点有相同的节点名称
     * 不允许有两个相同name的叶子节点,但是允许有两个相同名字的父节点,以及允许两个相同的,一个是父节点,一个是叶子节点的两个节点
     */
    if(queue instanceof LeafQueue == true && queues.containsKey(queueName) && queues.get(queueName) instanceof LeafQueue == true) {
      throw new IOException("Two leaf queues were named " + queueName
        + ". Leaf queue names must be distinct");
    }
    queues.put(queueName, queue);

    LOG.info("Initialized queue: " + queue);
    return queue;
  }

  public synchronized CSQueue getQueue(String queueName) {
    if (queueName == null) {
      return null;
    }
    return queues.get(queueName);
  }

  private static final String CURRENT_USER_MAPPING = "%user";

  private static final String PRIMARY_GROUP_MAPPING = "%primary_group";

  /**
   * 映射user应该对应的队列
   */
  private String getMappedQueue(String user) throws IOException {
    for (QueueMapping mapping : mappings) {
      if (mapping.type == MappingType.USER) {
        if (mapping.source.equals(CURRENT_USER_MAPPING)) {
          if (mapping.queue.equals(CURRENT_USER_MAPPING)) {
            return user;
          }
          else if (mapping.queue.equals(PRIMARY_GROUP_MAPPING)) {
            return groups.getGroups(user).get(0);
          }
          else {
            return mapping.queue;
          }
        }
        if (user.equals(mapping.source)) {
          return mapping.queue;
        }
      }
      if (mapping.type == MappingType.GROUP) {
        for (String userGroups : groups.getGroups(user)) {
          if (userGroups.equals(mapping.source)) {
            return mapping.queue;
          }
        }
      }
    }
    return null;
  }

  /**
   * 响应事件,一个应用被添加事件
   */
  private synchronized void addApplication(ApplicationId applicationId,String queueName, String user, boolean isAppRecovering) {
    if (mappings != null && mappings.size() > 0) {
      try {
        //根据用户查找对应的队列
        String mappedQueue = getMappedQueue(user);
        if (mappedQueue != null) {
          // We have a mapping, should we use it?
        	//如果队列是default或者overrideWithQueueMappings是true,则重新为应用分配队列
          if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)
              || overrideWithQueueMappings) {
            LOG.info("Application " + applicationId + " user " + user
                + " mapping [" + queueName + "] to [" + mappedQueue
                + "] override " + overrideWithQueueMappings);
            queueName = mappedQueue;
            RMApp rmApp = rmContext.getRMApps().get(applicationId);
            rmApp.setQueue(queueName);//为该应用重新分配队列
          }
        }
      } catch (IOException ioex) {
        String message = "Failed to submit application " + applicationId +
            " submitted by user " + user + " reason: " + ioex.getMessage();
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppRejectedEvent(applicationId, message));
        return;
      }
    }

    // sanity checks.安全检查
    CSQueue queue = getQueue(queueName);
    if (queue == null) {//队列不存在,则抛异常
      //During a restart, this indicates a queue was removed, which is
      //not presently supported
      if (isAppRecovering) {
        String queueErrorMsg = "Queue named " + queueName
           + " missing during application recovery."
           + " Queue removal during recovery is not presently supported by the"
           + " capacity scheduler, please restart with all queues configured"
           + " which were present before shutdown/restart.";
        LOG.fatal(queueErrorMsg);
        throw new QueueNotFoundException(queueErrorMsg);
      }
      String message = "Application " + applicationId + 
      " submitted by user " + user + " to unknown queue: " + queueName;
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, message));
      return;
    }
    if (!(queue instanceof LeafQueue)) {//队列不是叶子节点,则抛异常
      String message = "Application " + applicationId + 
          " submitted by user " + user + " to non-leaf queue: " + queueName;
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, message));
      return;
    }
    // Submit to the queue
    try {
      queue.submitApplication(applicationId, user, queueName);//提交一个应用到队列中
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application " + applicationId + " to queue "
          + queueName + " from user " + user, ace);
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, ace.toString()));
      return;
    }
    // update the metrics
    queue.getMetrics().submitApp(user);
    SchedulerApplication<FiCaSchedulerApp> application =
        new SchedulerApplication<FiCaSchedulerApp>(queue, user);
    applications.put(applicationId, application);
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName);
    if (isAppRecovering) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
      }
    } else {
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
    }
  }

  /**
   * 响应事件,一个应用实例被添加事件,即APP_ATTEMPT_ADDED
   */
  private synchronized void addApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
    
	  //获取属于该应用对应的调度实例
    SchedulerApplication<FiCaSchedulerApp> application = applications.get(applicationAttemptId.getApplicationId());
    CSQueue queue = (CSQueue) application.getQueue();//获取所属队列

    //创建一个app实例对象
    FiCaSchedulerApp attempt =
        new FiCaSchedulerApp(applicationAttemptId, application.getUser(),
          queue, queue.getActiveUsersManager(), rmContext);
    if (transferStateFromPreviousAttempt) {
      attempt.transferStateFromPreviousAttempt(application
        .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(attempt);

    queue.submitApplicationAttempt(attempt, application.getUser());
    LOG.info("Added Application Attempt " + applicationAttemptId
        + " to scheduler from user " + application.getUser() + " in queue "
        + queue.getQueueName());
    if (isAttemptRecovering) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(applicationAttemptId
            + " is recovering. Skipping notifying ATTEMPT_ADDED");
      }
    } else {
      rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
    }
  }

  /**
   * 响应事件,一个应用被移除事件
   * @param finalState 最终app状态
   */
  private synchronized void doneApplication(ApplicationId applicationId,RMAppState finalState) {
    SchedulerApplication<FiCaSchedulerApp> application = applications.get(applicationId);
    if (application == null){
      // The AppRemovedSchedulerEvent maybe sent on recovery for completed apps,
      // ignore it.
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }
    CSQueue queue = (CSQueue) application.getQueue();
    if (!(queue instanceof LeafQueue)) {
      LOG.error("Cannot finish application " + "from non-leaf queue: "
          + queue.getQueueName());
    } else {
      queue.finishApplication(applicationId, application.getUser());
    }
    application.stop(finalState);
    applications.remove(applicationId);
  }


  /**
   * 响应事件,一个应用实例被移除事件,即APP_ATTEMPT_REMOVED
   */
  private synchronized void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
    LOG.info("Application Attempt " + applicationAttemptId + " is done." +
        " finalState=" + rmAppAttemptFinalState);
    
    FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);//获取尝试任务
    SchedulerApplication<FiCaSchedulerApp> application = applications.get(applicationAttemptId.getApplicationId());

    if (application == null || attempt == null) {
      LOG.info("Unknown application " + applicationAttemptId + " has completed!");
      return;
    }

    // Release all the allocated, acquired, running containers杀死所有的该任务的容器
    for (RMContainer rmContainer : attempt.getLiveContainers()) {
      if (keepContainers
          && rmContainer.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + rmContainer.getContainerId());
        continue;
      }
      completedContainer(
        rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(
          rmContainer.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.KILL);
    }

    // Release all reserved containers
    for (RMContainer rmContainer : attempt.getReservedContainers()) {
      completedContainer(
        rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(
          rmContainer.getContainerId(), "Application Complete"),
        RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.停止该尝试任务
    attempt.stop(rmAppAttemptFinalState);

    // Inform the queue
    String queueName = attempt.getQueue().getQueueName();
    CSQueue queue = queues.get(queueName);
    if (!(queue instanceof LeafQueue)) {
      LOG.error("Cannot finish application " + "from non-leaf queue: "
          + queueName);
    } else {
      queue.finishApplicationAttempt(attempt, queue.getQueueName());
    }
  }

  /**
   * @param ApplicationAttemptId 为该尝试任务分配资源
   * @param ask 准备调度的任务
   * @param release 准备去释放的容器
   * @param blacklistAdditions 添加的黑名单
   * @param blacklistRemovals 移除的黑名单集合
   */
  @Override
  @Lock(Lock.NoLock.class)
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release,
      List<String> blacklistAdditions, List<String> blacklistRemovals) {

	  //获取尝试任务
    FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }
    
    // Sanity check
    SchedulerUtils.normalizeRequests(
        ask, getResourceCalculator(), getClusterResource(),
        getMinimumResourceCapability(), maximumAllocation);

    // Release containers释放容器
    releaseContainers(release, application);

    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        LOG.info("Calling allocate on a stopped " +
            "application " + applicationAttemptId);
        return EMPTY_ALLOCATION;
      }

      if (!ask.isEmpty()) {

        if(LOG.isDebugEnabled()) {
          LOG.debug("allocate: pre-update" +
            " applicationAttemptId=" + applicationAttemptId + 
            " application=" + application);
        }
        application.showRequests();
  
        // Update application requests
        application.updateResourceRequests(ask);
  
        LOG.debug("allocate: post-update");
        application.showRequests();
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("allocate:" +
          " applicationAttemptId=" + applicationAttemptId + 
          " #ask=" + ask.size());
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);

      return application.getAllocation(getResourceCalculator(),
                   clusterResource, getMinimumResourceCapability());
    }
  }

  //返回队列的信息
  @Override
  @Lock(Lock.NoLock.class)
  public QueueInfo getQueueInfo(String queueName, 
      boolean includeChildQueues, boolean recursive) 
  throws IOException {
    CSQueue queue = null;

    synchronized (this) {
      queue = this.queues.get(queueName); 
    }

    if (queue == null) {
      throw new IOException("Unknown queue: " + queueName);
    }
    return queue.getQueueInfo(includeChildQueues, recursive);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    UserGroupInformation user = null;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      // should never happen
      return new ArrayList<QueueUserACLInfo>();
    }

    return root.getQueueUserAclInfo(user);
  }

  /**
   * 节点通过心跳,发送该节点的容器信息,更新该节点将要启动的容器以及完成的容器
   */
  private synchronized void nodeUpdate(RMNode nm) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeUpdate: " + nm + " clusterResources: " + clusterResource);
    }

    FiCaSchedulerNode node = getNode(nm.getNodeID());
    
    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    
    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    }
    
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      completedContainer(getRMContainer(containerId), 
          completedContainer, RMContainerEventType.FINISHED);
    }

    // Now node data structures are upto date and ready for scheduling.
    if(LOG.isDebugEnabled()) {
      LOG.debug("Node being looked for scheduling " + nm
        + " availableResource: " + node.getAvailableResource());
    }
  }
  
  /**
   * Process resource update on a node.
   * 一个节点重新上报他的总资源数量
   * 因此要更改集群资源数量
   * 即集群中先减去老的资源，再加上新的资源
   */
  private synchronized void updateNodeAndQueueResource(RMNode nm,ResourceOption resourceOption) { 
    updateNodeResource(nm, resourceOption);
    root.updateClusterResource(clusterResource);
  }

  /**
   * 为该node分配一个容器
   */
  private synchronized void allocateContainersToNode(FiCaSchedulerNode node) {
    if (rmContext.isWorkPreservingRecoveryEnabled()
        && !rmContext.isSchedulerReadyForAllocatingContainers()) {
      return;
    }

    // Assign new containers...因为要为该节点分配一个容器
    // 1. Check for reserved applications 检查是否有预留保存的应用需要一个容器
    // 2. Schedule if there are no reservations 调度一个容器
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {//属于第一种可能,即有预留的容器
      FiCaSchedulerApp reservedApplication =
          getCurrentAttemptForContainer(reservedContainer.getContainerId());//通过容器ID获取当前执行该容器的尝试任务
      
      // Try to fulfill the reservation 打印日志,在该节点执行预留的应用
      LOG.info("Trying to fulfill reservation for application " + 
          reservedApplication.getApplicationId() + " on node: " + 
          node.getNodeID());
      
      //该应用所在队列一定是叶子节点
      LeafQueue queue = ((LeafQueue)reservedApplication.getQueue());
      CSAssignment assignment = queue.assignContainers(clusterResource, node,
          false);
      
      RMContainer excessReservation = assignment.getExcessReservation();
      if (excessReservation != null) {
      Container container = excessReservation.getContainer();
      queue.completedContainer(
          clusterResource, assignment.getApplication(), node, 
          excessReservation, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getId(), 
              SchedulerUtils.UNRESERVED_CONTAINER), 
          RMContainerEventType.RELEASED, null, true);
      }

    }

    // Try to schedule more if there are no reservations to fulfill 如果该节点上没有预存的应用,则
    if (node.getReservedContainer() == null) {
    	//available.getMemory() / required.getMemory(),
      if (calculator.computeAvailableContainers(node.getAvailableResource(),minimumAllocation) > 0) {//说明有内存资源充足
        if (LOG.isDebugEnabled()) {
          LOG.debug("Trying to schedule on node: " + node.getNodeName() +
              ", available: " + node.getAvailableResource());
        }
        root.assignContainers(clusterResource, node, false);
      }
    } else {
      LOG.info("Skipping scheduling since node " + node.getNodeID() + 
          " is reserved by application " + 
          node.getReservedContainer().getContainerId().getApplicationAttemptId()
          );
    }
  
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch(event.getType()) {
    case NODE_ADDED:
    {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
      recoverContainersOnNode(nodeAddedEvent.getContainerReports(),nodeAddedEvent.getAddedRMNode());
    }
    break;
    case NODE_REMOVED:
    {
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
    }
    break;
    /**
     * 一个节点重新上报他的总资源数量
     * 因此要更改集群资源数量
     * 即集群中先减去老的资源，再加上新的资源
     */
    case NODE_RESOURCE_UPDATE:
    {
      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = 
          (NodeResourceUpdateSchedulerEvent)event;
      updateNodeAndQueueResource(nodeResourceUpdatedEvent.getRMNode(),
        nodeResourceUpdatedEvent.getResourceOption());
    }
    break;
    case NODE_UPDATE://节点通过心跳,发送该节点的容器信息,更新该节点将要启动的容器以及完成的容器
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      RMNode node = nodeUpdatedEvent.getRMNode();
      nodeUpdate(node);//更新该节点的信息
      if (!scheduleAsynchronously) {
        allocateContainersToNode(getNode(node.getNodeID()));//为该节点分配容器
      }
    }
    break;
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      String queueName =
          resolveReservationQueueName(appAddedEvent.getQueue(),
              appAddedEvent.getApplicationId(),
              appAddedEvent.getReservationID());
      if (queueName != null) {
        addApplication(appAddedEvent.getApplicationId(),
            queueName,
            appAddedEvent.getUser(),
            appAddedEvent.getIsAppRecovering());
      }
    }
    break;
    case APP_REMOVED:
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      doneApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
    }
    break;
    case APP_ATTEMPT_ADDED:
    {
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
        appAttemptAddedEvent.getIsAttemptRecovering());
    }
    break;
    case APP_ATTEMPT_REMOVED:
    {
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      doneApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
        appAttemptRemovedEvent.getFinalAttemptState(),
        appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
    }
    break;
    case CONTAINER_EXPIRED:
    {
      ContainerExpiredSchedulerEvent containerExpiredEvent = 
          (ContainerExpiredSchedulerEvent) event;
      ContainerId containerId = containerExpiredEvent.getContainerId();
      completedContainer(getRMContainer(containerId), 
          SchedulerUtils.createAbnormalContainerStatus(
              containerId, 
              SchedulerUtils.EXPIRED_CONTAINER), 
          RMContainerEventType.EXPIRE);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  /**
   * 响应事件,一个节点被添加事件
   */
  private synchronized void addNode(RMNode nodeManager) {
    // update this node to node label manager
    if (labelManager != null) {
      labelManager.activateNode(nodeManager.getNodeID(),nodeManager.getTotalCapability());
    }
    
    this.nodes.put(nodeManager.getNodeID(), new FiCaSchedulerNode(nodeManager,usePortForNodeName));
    //集群中添加总资源
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
    //更新总资源
    root.updateClusterResource(clusterResource);
    //增加节点个数
    int numNodes = numNodeManagers.incrementAndGet();
    
    LOG.info("Added node " + nodeManager.getNodeAddress() + 
        " clusterResource: " + clusterResource);

    if (scheduleAsynchronously && numNodes == 1) {
      asyncSchedulerThread.beginSchedule();
    }
  }
  
  /**
   * 响应事件,一个节点被移除事件
   */
  private synchronized void removeNode(RMNode nodeInfo) {
    // update this node to node label manager
    if (labelManager != null) {
      labelManager.deactivateNode(nodeInfo.getNodeID());
    }
    
    FiCaSchedulerNode node = nodes.get(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }
    //减少资源
    Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
    //为根节点更新集群资源
    root.updateClusterResource(clusterResource);
    //减少集群中节点数量
    int numNodes = numNodeManagers.decrementAndGet();

    //没有节点了,则挂起
    if (scheduleAsynchronously && numNodes == 0) {
      asyncSchedulerThread.suspendSchedule();
    }
    
    /**
     * 因为节点移除了,则需要移除该节点对应的容器
     */
    // Remove running containers
    List<RMContainer> runningContainers = node.getRunningContainers();
    for (RMContainer container : runningContainers) {
      //  SchedulerUtils.createAbnormalContainerStatus生成该容器完成的时的状态
      completedContainer(container, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER), 
          RMContainerEventType.KILL);
    }
    
    // Remove reservations, if any
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      completedContainer(reservedContainer, 
          SchedulerUtils.createAbnormalContainerStatus(
              reservedContainer.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER), 
          RMContainerEventType.KILL);
    }

    this.nodes.remove(nodeInfo.getNodeID());

    LOG.info("Removed node " + nodeInfo.getNodeAddress() + 
        " clusterResource: " + clusterResource);
  }
  
  /**
   * 完成一个容器,该容器可以是成功完成、也可以是中断完成,取决于容器状态对象ContainerStatus
   */
  @Lock(CapacityScheduler.class)
  @Override
  protected synchronized void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }
    
    Container container = rmContainer.getContainer();
    
    // Get the application for the finished container
    FiCaSchedulerApp application = getCurrentAttemptForContainer(container.getId());
    ApplicationId appId = container.getId().getApplicationAttemptId().getApplicationId();
    if (application == null) {
      LOG.info("Container " + container + " of" + " unknown application "
          + appId + " completed with event " + event);
      return;
    }
    
    // Get the node on which the container was allocated
    FiCaSchedulerNode node = getNode(container.getNodeId());
    
    // Inform the queue 队列完成一个容器处理
    LeafQueue queue = (LeafQueue)application.getQueue();
    queue.completedContainer(clusterResource, application, node, rmContainer, containerStatus, event, null, true);

    LOG.info("Application attempt " + application.getApplicationAttemptId()
        + " released container " + container.getId() + " on node: " + node
        + " with event: " + event);
  }

  //获取给定的app对应的尝试任务
  @Lock(Lock.NoLock.class)
  @VisibleForTesting
  @Override
  public FiCaSchedulerApp getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    return super.getApplicationAttempt(applicationAttemptId);
  }
  
  @Lock(Lock.NoLock.class)
  public FiCaSchedulerNode getNode(NodeId nodeId) {
    return nodes.get(nodeId);
  }
  
  @Lock(Lock.NoLock.class)
  Map<NodeId, FiCaSchedulerNode> getAllNodes() {
    return nodes;
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void recover(RMState state) throws Exception {
    // NOT IMPLEMENTED
  }

  @Override
  public void dropContainerReservation(RMContainer container) {
    if(LOG.isDebugEnabled()){
      LOG.debug("DROP_RESERVATION:" + container.toString());
    }
    completedContainer(container,
        SchedulerUtils.createAbnormalContainerStatus(
            container.getContainerId(),
            SchedulerUtils.UNRESERVED_CONTAINER),
        RMContainerEventType.KILL);
  }

  @Override
  public void preemptContainer(ApplicationAttemptId aid, RMContainer cont) {
    if(LOG.isDebugEnabled()){
      LOG.debug("PREEMPT_CONTAINER: application:" + aid.toString() +
          " container: " + cont.toString());
    }
    FiCaSchedulerApp app = getApplicationAttempt(aid);
    if (app != null) {
      app.addPreemptContainer(cont.getContainerId());
    }
  }

  @Override
  public void killContainer(RMContainer cont) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("KILL_CONTAINER: container" + cont.toString());
    }
    recoverResourceRequestForContainer(cont);
    completedContainer(cont, SchedulerUtils.createPreemptedContainerStatus(
      cont.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER),
      RMContainerEventType.KILL);
  }

  /**
   * 检查该用户是否有权限访问该队列
   */
  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ACL not found for queue access-type " + acl
            + " for queue " + queueName);
      }
      return false;
    }
    return queue.hasAccess(acl, callerUGI);
  }

  /**
   * 获取该队列中所有应用集合
   */
  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    CSQueue queue = queues.get(queueName);
    if (queue == null) {
      return null;
    }
    List<ApplicationAttemptId> apps = new ArrayList<ApplicationAttemptId>();
    queue.collectSchedulerApplications(apps);
    return apps;
  }

  /**
   * 生成调度器配置文件
   */
  private CapacitySchedulerConfiguration loadCapacitySchedulerConfiguration(Configuration configuration) throws IOException {
    try {
      //读取capacity-scheduler.xml配置文件
      InputStream CSInputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(configuration,
                  YarnConfiguration.CS_CONFIGURATION_FILE);
      if (CSInputStream != null) {
        configuration.addResource(CSInputStream);
        return new CapacitySchedulerConfiguration(configuration, false);
      }
      return new CapacitySchedulerConfiguration(configuration, true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * 分解预留队列
   */
  private synchronized String resolveReservationQueueName(String queueName,
      ApplicationId applicationId, ReservationId reservationID) {
    CSQueue queue = getQueue(queueName);
    // Check if the queue is a plan queue
    if ((queue == null) || !(queue instanceof PlanQueue)) {
      return queueName;
    }
    if (reservationID != null) {
      String resQName = reservationID.toString();
      queue = getQueue(resQName);
      if (queue == null) {
        String message =
            "Application "
                + applicationId
                + " submitted to a reservation which is not yet currently active: "
                + resQName;
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppRejectedEvent(applicationId, message));
        return null;
      }
      if (!queue.getParent().getQueueName().equals(queueName)) {
        String message =
            "Application: " + applicationId + " submitted to a reservation "
                + resQName + " which does not belong to the specified queue: "
                + queueName;
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppRejectedEvent(applicationId, message));
        return null;
      }
      // use the reservation queue to run the app
      queueName = resQName;
    } else {
      // use the default child queue of the plan for unreserved apps
      queueName = queueName + PlanQueue.DEFAULT_QUEUE_SUFFIX;
    }
    return queueName;
  }

  /**
   * 移除某个队列,前提是该队列已经没有在运行的任务，同时该队列属于ReservationQueue类型的
   */
  @Override
  public synchronized void removeQueue(String queueName) throws SchedulerDynamicEditException {
    LOG.info("Removing queue: " + queueName);
    CSQueue q = this.getQueue(queueName);
    if (!(q instanceof ReservationQueue)) {
      throw new SchedulerDynamicEditException("The queue that we are asked "
          + "to remove (" + queueName + ") is not a ReservationQueue");
    }
    ReservationQueue disposableLeafQueue = (ReservationQueue) q;
    // at this point we should have no more apps
    if (disposableLeafQueue.getNumApplications() > 0) {
      throw new SchedulerDynamicEditException("The queue " + queueName
          + " is not empty " + disposableLeafQueue.getApplications().size()
          + " active apps " + disposableLeafQueue.pendingApplications.size()
          + " pending apps");
    }

    ((PlanQueue) disposableLeafQueue.getParent()).removeChildQueue(q);
    this.queues.remove(queueName);
    LOG.info("Removal of ReservationQueue " + queueName + " has succeeded");
  }

  @Override
  public synchronized void addQueue(Queue queue)
      throws SchedulerDynamicEditException {

    if (!(queue instanceof ReservationQueue)) {
      throw new SchedulerDynamicEditException("Queue " + queue.getQueueName()
          + " is not a ReservationQueue");
    }

    ReservationQueue newQueue = (ReservationQueue) queue;

    if (newQueue.getParent() == null
        || !(newQueue.getParent() instanceof PlanQueue)) {
      throw new SchedulerDynamicEditException("ParentQueue for "
          + newQueue.getQueueName()
          + " is not properly set (should be set and be a PlanQueue)");
    }

    PlanQueue parentPlan = (PlanQueue) newQueue.getParent();
    String queuename = newQueue.getQueueName();
    parentPlan.addChildQueue(newQueue);
    this.queues.put(queuename, newQueue);
    LOG.info("Creation of ReservationQueue " + newQueue + " succeeded");
  }

  @Override
  public synchronized void setEntitlement(String inQueue,QueueEntitlement entitlement) throws SchedulerDynamicEditException,YarnException {
    //检查该队列是叶子队列同时也必须存在该队列
    LeafQueue queue = getAndCheckLeafQueue(inQueue);
    //返回父队列,即目录队列
    ParentQueue parent = (ParentQueue) queue.getParent();

    if (!(queue instanceof ReservationQueue)) {
      throw new SchedulerDynamicEditException("Entitlement can not be"
          + " modified dynamically since queue " + inQueue
          + " is not a ReservationQueue");
    }

    if (!(parent instanceof PlanQueue)) {
      throw new SchedulerDynamicEditException("The parent of ReservationQueue "
          + inQueue + " must be an PlanQueue");
    }

    ReservationQueue newQueue = (ReservationQueue) queue;

    float sumChilds = ((PlanQueue) parent).sumOfChildCapacities();
    float newChildCap = sumChilds - queue.getCapacity() + entitlement.getCapacity();

    if (newChildCap >= 0 && newChildCap < 1.0f + CSQueueUtils.EPSILON) {
      // note: epsilon checks here are not ok, as the epsilons might accumulate
      // and become a problem in aggregate
      if (Math.abs(entitlement.getCapacity() - queue.getCapacity()) == 0
          && Math.abs(entitlement.getMaxCapacity() - queue.getMaximumCapacity()) == 0) {
        return;
      }
      newQueue.setEntitlement(entitlement);
    } else {
      throw new SchedulerDynamicEditException(
          "Sum of child queues would exceed 100% for PlanQueue: "
              + parent.getQueueName());
    }
    LOG.info("Set entitlement for ReservationQueue " + inQueue + "  to "
        + queue.getCapacity() + " request was (" + entitlement.getCapacity() + ")");
  }

  /**
   * 移动该应用到另外一个队列中
   */
  @Override
  public synchronized String moveApplication(ApplicationId appId,
      String targetQueueName) throws YarnException {
    FiCaSchedulerApp app =
        getApplicationAttempt(ApplicationAttemptId.newInstance(appId, 0));
    String sourceQueueName = app.getQueue().getQueueName();
    LeafQueue source = getAndCheckLeafQueue(sourceQueueName);
    String destQueueName = handleMoveToPlanQueue(targetQueueName);
    LeafQueue dest = getAndCheckLeafQueue(destQueueName);
    // Validation check - ACLs, submission limits for user & queue
    String user = app.getUser();
    try {
      dest.submitApplication(appId, user, destQueueName);
    } catch (AccessControlException e) {
      throw new YarnException(e);
    }
    // Move all live containers
    for (RMContainer rmContainer : app.getLiveContainers()) {
      source.detachContainer(clusterResource, app, rmContainer);
      // attach the Container to another queue
      dest.attachContainer(clusterResource, app, rmContainer);
    }
    // Detach the application..
    source.finishApplicationAttempt(app, sourceQueueName);
    source.getParent().finishApplication(appId, app.getUser());
    // Finish app & update metrics
    app.move(dest);
    // Submit to a new queue
    dest.submitApplicationAttempt(app, user);
    applications.get(appId).setQueue(dest);
    LOG.info("App: " + app.getApplicationId() + " successfully moved from "
        + sourceQueueName + " to: " + destQueueName);
    return targetQueueName;
  }

  /**
   * 检查该队列是叶子队列同时也必须存在该队列
   * Check that the String provided in input is the name of an existing,
   * LeafQueue, if successful returns the queue.
   *
   * @param queue
   * @return the LeafQueue
   * @throws YarnException
   */
  private LeafQueue getAndCheckLeafQueue(String queue) throws YarnException {
    CSQueue ret = this.getQueue(queue);
    if (ret == null) {
      throw new YarnException("The specified Queue: " + queue
          + " doesn't exist");
    }
    if (!(ret instanceof LeafQueue)) {
      throw new YarnException("The specified Queue: " + queue
          + " is not a Leaf Queue. Move is supported only for Leaf Queues.");
    }
    return (LeafQueue) ret;
  }

  /** {@inheritDoc} 
   * 该调度器以什么调度为准
   * */
  @Override
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes() {
    if (calculator.getClass().getName()
      .equals(DefaultResourceCalculator.class.getName())) {
      return EnumSet.of(SchedulerResourceTypes.MEMORY);
    }
    return EnumSet
      .of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU);
  }
  
  private String handleMoveToPlanQueue(String targetQueueName) {
    CSQueue dest = getQueue(targetQueueName);
    if (dest != null && dest instanceof PlanQueue) {
      // use the default child reservation queue of the plan
      targetQueueName = targetQueueName + PlanQueue.DEFAULT_QUEUE_SUFFIX;
    }
    return targetQueueName;
  }

  /**
   * 获取PlanQueue类型的队列集合
   */
  @Override
  public Set<String> getPlanQueues() {
    Set<String> ret = new HashSet<String>();
    for (Map.Entry<String, CSQueue> l : queues.entrySet()) {
      if (l.getValue() instanceof PlanQueue) {
        ret.add(l.getKey());
      }
    }
    return ret;
  }
}

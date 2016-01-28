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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;

public class CapacitySchedulerConfiguration extends Configuration {

  private static final Log LOG = 
    LogFactory.getLog(CapacitySchedulerConfiguration.class);
  
  private static final String CS_CONFIGURATION_FILE = "capacity-scheduler.xml";
  
  @Private
  public static final String PREFIX = "yarn.scheduler.capacity.";
  
  @Private
  public static final String DOT = ".";
  
  @Private
  public static final String MAXIMUM_APPLICATIONS_SUFFIX =
    "maximum-applications";
  
  //yarn.scheduler.capacity.maximum-applications
  @Private
  public static final String MAXIMUM_SYSTEM_APPLICATIONS =
    PREFIX + MAXIMUM_APPLICATIONS_SUFFIX;
  
  @Private
  public static final String MAXIMUM_AM_RESOURCE_SUFFIX =
    "maximum-am-resource-percent";
  
  //yarn.scheduler.capacity.maximum-am-resource-percent
  @Private
  public static final String MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT =
    PREFIX + MAXIMUM_AM_RESOURCE_SUFFIX;
  
  @Private
  public static final String QUEUES = "queues";
  
  @Private
  public static final String CAPACITY = "capacity";
  
  //某个队列的最大capacity
  @Private
  public static final String MAXIMUM_CAPACITY = "maximum-capacity";
  
  @Private
  public static final String USER_LIMIT = "minimum-user-limit-percent";
  
  @Private
  public static final String USER_LIMIT_FACTOR = "user-limit-factor";

  @Private
  public static final String STATE = "state";
  
  @Private
  public static final String ACCESSIBLE_NODE_LABELS = "accessible-node-labels";
  
  @Private
  public static final String DEFAULT_NODE_LABEL_EXPRESSION =
      "default-node-label-expression";

  //yarn.scheduler.capacity.reservations-continue-look-all-nodes
  public static final String RESERVE_CONT_LOOK_ALL_NODES = PREFIX
      + "reservations-continue-look-all-nodes";
  
  @Private
  public static final boolean DEFAULT_RESERVE_CONT_LOOK_ALL_NODES = true;

  @Private
  public static final int DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS = 10000;
  
  @Private
  public static final float 
  DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT = 0.1f;
  
  @Private
  public static final float UNDEFINED = -1;
  
  @Private
  public static final float MINIMUM_CAPACITY_VALUE = 0;
  
  @Private
  public static final float MAXIMUM_CAPACITY_VALUE = 100;
  
  @Private
  public static final float DEFAULT_MAXIMUM_CAPACITY_VALUE = -1.0f;
  
  @Private
  public static final int DEFAULT_USER_LIMIT = 100;
  
  @Private
  public static final float DEFAULT_USER_LIMIT_FACTOR = 1.0f;

  @Private
  public static final String ALL_ACL = "*";

  @Private
  public static final String NONE_ACL = " ";

  //yarn.scheduler.capacity.user-metrics.enable是否开启队列统计功能
  @Private public static final String ENABLE_USER_METRICS = PREFIX +"user-metrics.enable";
  
  //默认对队列统计功能是关闭的
  @Private public static final boolean DEFAULT_ENABLE_USER_METRICS = false;

  //yarn.scheduler.capacity.resource-calculator 资源计算器
  /** ResourceComparator for scheduling. */
  @Private public static final String RESOURCE_CALCULATOR_CLASS = PREFIX + "resource-calculator";

  @Private public static final Class<? extends ResourceCalculator> 
  DEFAULT_RESOURCE_CALCULATOR_CLASS = DefaultResourceCalculator.class;
  
  @Private
  public static final String ROOT = "root";

  //yarn.scheduler.capacity.node-locality-delay
  @Private 
  public static final String NODE_LOCALITY_DELAY = 
     PREFIX + "node-locality-delay";

  @Private 
  public static final int DEFAULT_NODE_LOCALITY_DELAY = -1;

  //yarn.scheduler.capacity.schedule-asynchronously
  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_PREFIX =
      PREFIX + "schedule-asynchronously";

  //yarn.scheduler.capacity.schedule-asynchronously.enable
  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_ENABLE =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".enable";

  @Private
  public static final boolean DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE = false;

  //yarn.scheduler.capacity.queue-mappings  什么用户映射到什么组
  @Private
  public static final String QUEUE_MAPPING = PREFIX + "queue-mappings";

  //yarn.scheduler.capacity.queue-mappings-override.enable  是否启动user、group映射到组特性
  @Private
  public static final String ENABLE_QUEUE_MAPPING_OVERRIDE = QUEUE_MAPPING + "-override.enable";

  @Private
  public static final boolean DEFAULT_ENABLE_QUEUE_MAPPING_OVERRIDE = false;

  @Private
  public static class QueueMapping {

    public enum MappingType {

      USER("u"),
      GROUP("g");
      private final String type;
      private MappingType(String type) {
        this.type = type;
      }

      public String toString() {
        return type;
      }

    };

    MappingType type;//用户还是租
    String source;//用户名或者组名
    String queue;//映射的队列

    public QueueMapping(MappingType type, String source, String queue) {
      this.type = type;
      this.source = source;
      this.queue = queue;
    }
  }
  
  @Private
  public static final String AVERAGE_CAPACITY = "average-capacity";

  @Private
  public static final String IS_RESERVABLE = "reservable";

  @Private
  public static final String RESERVATION_WINDOW = "reservation-window";

  @Private
  public static final String INSTANTANEOUS_MAX_CAPACITY =
      "instantaneous-max-capacity";

  @Private
  public static final long DEFAULT_RESERVATION_WINDOW = 86400000L;

  @Private
  public static final String RESERVATION_ADMISSION_POLICY =
      "reservation-policy";

  @Private
  public static final String RESERVATION_AGENT_NAME = "reservation-agent";

  @Private
  public static final String RESERVATION_SHOW_RESERVATION_AS_QUEUE =
      "show-reservations-as-queues";

  @Private
  public static final String DEFAULT_RESERVATION_ADMISSION_POLICY =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy";

  @Private
  public static final String DEFAULT_RESERVATION_AGENT_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.GreedyReservationAgent";

  @Private
  public static final String RESERVATION_PLANNER_NAME = "reservation-planner";

  @Private
  public static final String DEFAULT_RESERVATION_PLANNER_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.SimpleCapacityReplanner";

  @Private
  public static final String RESERVATION_MOVE_ON_EXPIRY =
      "reservation-move-on-expiry";

  @Private
  public static final boolean DEFAULT_RESERVATION_MOVE_ON_EXPIRY = true;

  @Private
  public static final String RESERVATION_ENFORCEMENT_WINDOW =
      "reservation-enforcement-window";

  // default to 1h lookahead enforcement
  @Private
  public static final long DEFAULT_RESERVATION_ENFORCEMENT_WINDOW = 3600000;

  public CapacitySchedulerConfiguration() {
    this(new Configuration());
  }
  
  public CapacitySchedulerConfiguration(Configuration configuration) {
    this(configuration, true);
  }

  public CapacitySchedulerConfiguration(Configuration configuration,
      boolean useLocalConfigurationProvider) {
    super(configuration);
    if (useLocalConfigurationProvider) {
      addResource(CS_CONFIGURATION_FILE);//加载capacity-scheduler.xml
    }
  }

  //yarn.scheduler.capacity.$queue.
  private String getQueuePrefix(String queue) {
    String queueName = PREFIX + queue + DOT;
    return queueName;
  }
  
  //yarn.scheduler.capacity.$queue.accessible-node-labels.label.
  private String getNodeLabelPrefix(String queue, String label) {
    return getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS + DOT + label + DOT;
  }
  
  //yarn.scheduler.capacity.maximum-applications
  public int getMaximumSystemApplications() {
    int maxApplications = 
      getInt(MAXIMUM_SYSTEM_APPLICATIONS, DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS);
    return maxApplications;
  }
  
  //yarn.scheduler.capacity.maximum-am-resource-percent
  public float getMaximumApplicationMasterResourcePercent() {
    return getFloat(MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 
        DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT);
  }


  /**
   * Get the maximum applications per queue setting.
   * @param queue name of the queue
   * @return setting specified or -1 if not set
   */
  public int getMaximumApplicationsPerQueue(String queue) {
	  //yarn.scheduler.capacity.$queue.maximum-applications
    int maxApplicationsPerQueue = 
      getInt(getQueuePrefix(queue) + MAXIMUM_APPLICATIONS_SUFFIX, 
          (int)UNDEFINED);
    return maxApplicationsPerQueue;
  }

  /**
   * Get the maximum am resource percent per queue setting.
   * @param queue name of the queue
   * @return per queue setting or defaults to the global am-resource-percent 
   *         setting if per queue setting not present
   */
  public float getMaximumApplicationMasterResourcePerQueuePercent(String queue) {
	//yarn.scheduler.capacity.$queue.maximum-am-resource-percent
    return getFloat(getQueuePrefix(queue) + MAXIMUM_AM_RESOURCE_SUFFIX, 
    		getMaximumApplicationMasterResourcePercent());//默认是yarn.scheduler.capacity.maximum-am-resource-percent
  }
  
  /**
   * 获取配置文件中配置该队列的容量 
   */
  public float getCapacity(String queue) {
    //yarn.scheduler.capacity.$queue.capacity
    float capacity = queue.equals("root") ? 100.0f : getFloat(getQueuePrefix(queue) + CAPACITY, UNDEFINED);
    if (capacity < MINIMUM_CAPACITY_VALUE || capacity > MAXIMUM_CAPACITY_VALUE) {//该值在0-100之间
      throw new IllegalArgumentException("Illegal " +
      		"capacity of " + capacity + " for queue " + queue);
    }
    LOG.debug("CSConf - getCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", capacity=" + capacity);
    return capacity;
  }
  
  public void setCapacity(String queue, float capacity) {
    if (queue.equals("root")) {
      throw new IllegalArgumentException(
          "Cannot set capacity, root queue has a fixed capacity of 100.0f");
    }
    //yarn.scheduler.capacity.$queue.capacity
    setFloat(getQueuePrefix(queue) + CAPACITY, capacity);
    LOG.debug("CSConf - setCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", capacity=" + capacity);
  }

  public float getMaximumCapacity(String queue) {
    //获取该队列的最大capacity
	//yarn.scheduler.capacity.$queue.maximum-capacity该队列的最大capacity
    float maxCapacity = getFloat(getQueuePrefix(queue) + MAXIMUM_CAPACITY, MAXIMUM_CAPACITY_VALUE);
    maxCapacity = (maxCapacity == DEFAULT_MAXIMUM_CAPACITY_VALUE) ? MAXIMUM_CAPACITY_VALUE : maxCapacity; 
    return maxCapacity;
  }
  
  public void setMaximumCapacity(String queue, float maxCapacity) {
    if (maxCapacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException("Illegal " +
          "maximum-capacity of " + maxCapacity + " for queue " + queue);
    }
    //yarn.scheduler.capacity.$queue.maximum-capacity
    setFloat(getQueuePrefix(queue) + MAXIMUM_CAPACITY, maxCapacity);
    LOG.debug("CSConf - setMaxCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", maxCapacity=" + maxCapacity);
  }
  
  //yarn.scheduler.capacity.$queue.accessible-node-labels.label.capacity
  public void setCapacityByLabel(String queue, String label, float capacity) {
    setFloat(getNodeLabelPrefix(queue, label) + CAPACITY, capacity);
  }
  
  //yarn.scheduler.capacity.$queue.accessible-node-labels.label.maximum-capacity
  public void setMaximumCapacityByLabel(String queue, String label,
      float capacity) {
    setFloat(getNodeLabelPrefix(queue, label) + MAXIMUM_CAPACITY, capacity);
  }
  
  //yarn.scheduler.capacity.$queue.minimum-user-limit-percent
  public int getUserLimit(String queue) {
    int userLimit = getInt(getQueuePrefix(queue) + USER_LIMIT,
        DEFAULT_USER_LIMIT);
    return userLimit;
  }

  //yarn.scheduler.capacity.$queue.minimum-user-limit-percent
  public void setUserLimit(String queue, int userLimit) {
    setInt(getQueuePrefix(queue) + USER_LIMIT, userLimit);
    LOG.debug("here setUserLimit: queuePrefix=" + getQueuePrefix(queue) + 
        ", userLimit=" + getUserLimit(queue));
  }
  
  //yarn.scheduler.capacity.$queue.user-limit-factor
  public float getUserLimitFactor(String queue) {
    float userLimitFactor = 
      getFloat(getQueuePrefix(queue) + USER_LIMIT_FACTOR, 
          DEFAULT_USER_LIMIT_FACTOR);
    return userLimitFactor;
  }
  //yarn.scheduler.capacity.$queue.user-limit-factor
  public void setUserLimitFactor(String queue, float userLimitFactor) {
    setFloat(getQueuePrefix(queue) + USER_LIMIT_FACTOR, userLimitFactor); 
  }
  
  //yarn.scheduler.capacity.$queue.state
  public QueueState getState(String queue) {
    String state = get(getQueuePrefix(queue) + STATE);
    return (state != null) ? 
        QueueState.valueOf(state.toUpperCase()) : QueueState.RUNNING;
  }
  
  public void setAccessibleNodeLabels(String queue, Set<String> labels) {
    if (labels == null) {
      return;
    }
    String str = StringUtils.join(",", labels);
    //yarn.scheduler.capacity.$queue.accessible-node-labels
    set(getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS, str);
  }
  
  public Set<String> getAccessibleNodeLabels(String queue) {
	  
	//yarn.scheduler.capacity.$queue.accessible-node-labels
    String accessibleLabelStr =
        get(getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS);

    // When accessible-label is null, 
    if (accessibleLabelStr == null) {
      // Only return null when queue is not ROOT
      if (!queue.equals(ROOT)) {
        return null;
      }
    } else {
      // print a warning when accessibleNodeLabel specified in config and queue
      // is ROOT
      if (queue.equals(ROOT)) {
        LOG.warn("Accessible node labels for root queue will be ignored,"
            + " it will be automatically set to \"*\".");
      }
    }

    // always return ANY for queue root
    if (queue.equals(ROOT)) {
      return ImmutableSet.of(RMNodeLabelsManager.ANY);
    }

    // In other cases, split the accessibleLabelStr by ","
    Set<String> set = new HashSet<String>();
    for (String str : accessibleLabelStr.split(",")) {
      if (!str.trim().isEmpty()) {
        set.add(str.trim());
      }
    }
    
    // if labels contains "*", only keep ANY behind
    if (set.contains(RMNodeLabelsManager.ANY)) {
      set.clear();
      set.add(RMNodeLabelsManager.ANY);
    }
    return Collections.unmodifiableSet(set);
  }
  
  public Map<String, Float> getNodeLabelCapacities(String queue,
      Set<String> labels, RMNodeLabelsManager mgr) {
    Map<String, Float> nodeLabelCapacities = new HashMap<String, Float>();
    
    if (labels == null) {
      return nodeLabelCapacities;
    }

    for (String label : labels.contains(CommonNodeLabelsManager.ANY) ? mgr
        .getClusterNodeLabels() : labels) {
     
      //yarn.scheduler.capacity.$queue.accessible-node-labels.label.capacity
      String capacityPropertyName = getNodeLabelPrefix(queue, label) + CAPACITY;
      float capacity = getFloat(capacityPropertyName, 0f);
      if (capacity < MINIMUM_CAPACITY_VALUE
          || capacity > MAXIMUM_CAPACITY_VALUE) {
        throw new IllegalArgumentException("Illegal capacity of " + capacity
            + " for node-label=" + label + " in queue=" + queue
            + ", valid capacity should in range of [0, 100].");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("CSConf - getCapacityOfLabel: prefix="
            + getNodeLabelPrefix(queue, label) + ", capacity=" + capacity);
      }
      
      nodeLabelCapacities.put(label, capacity / 100f);
    }
    return nodeLabelCapacities;
  }
  
  public Map<String, Float> getMaximumNodeLabelCapacities(String queue,
      Set<String> labels, RMNodeLabelsManager mgr) {
    Map<String, Float> maximumNodeLabelCapacities = new HashMap<String, Float>();
    if (labels == null) {
      return maximumNodeLabelCapacities;
    }

    for (String label : labels.contains(CommonNodeLabelsManager.ANY) ? mgr
        .getClusterNodeLabels() : labels) {
    	
      //yarn.scheduler.capacity.$queue.accessible-node-labels.label.maximum-capacity
      float maxCapacity =
          getFloat(getNodeLabelPrefix(queue, label) + MAXIMUM_CAPACITY,
              100f);
      if (maxCapacity < MINIMUM_CAPACITY_VALUE
          || maxCapacity > MAXIMUM_CAPACITY_VALUE) {
        throw new IllegalArgumentException("Illegal " + "capacity of "
            + maxCapacity + " for label=" + label + " in queue=" + queue);
      }
      LOG.debug("CSConf - getCapacityOfLabel: prefix="
          + getNodeLabelPrefix(queue, label) + ", capacity=" + maxCapacity);
      
      maximumNodeLabelCapacities.put(label, maxCapacity / 100f);
    }
    return maximumNodeLabelCapacities;
  }
  
  //yarn.scheduler.capacity.$queue.default-node-label-expression
  public String getDefaultNodeLabelExpression(String queue) {
    return get(getQueuePrefix(queue) + DEFAULT_NODE_LABEL_EXPRESSION);
  }
  
  //yarn.scheduler.capacity.$queue.default-node-label-expression
  public void setDefaultNodeLabelExpression(String queue, String exp) {
    set(getQueuePrefix(queue) + DEFAULT_NODE_LABEL_EXPRESSION, exp);
  }

  /*
   * Returns whether we should continue to look at all heart beating nodes even
   * after the reservation limit was hit. The node heart beating in could
   * satisfy the request thus could be a better pick then waiting for the
   * reservation to be fullfilled.  This config is refreshable.
   */
  public boolean getReservationContinueLook() {
	//yarn.scheduler.capacity.reservations-continue-look-all-nodes
    return getBoolean(RESERVE_CONT_LOOK_ALL_NODES,
        DEFAULT_RESERVE_CONT_LOOK_ALL_NODES);
  }
  
  /**
   * 返回权限字符串
   */
  private static String getAclKey(QueueACL acl) {
    return "acl_" + acl.toString().toLowerCase();
  }

  /**
   * 通过队列和权限,获取使用该权限的用户和用户组信息
   */
  public AccessControlList getAcl(String queue, QueueACL acl) {
	//yarn.scheduler.capacity.$queue.
    String queuePrefix = getQueuePrefix(queue);
    // The root queue defaults to all access if not defined
    // Sub queues inherit access if not defined
    String defaultAcl = queue.equals(ROOT) ? ALL_ACL : NONE_ACL;
    //yarn.scheduler.capacity.$queue.acl_$xxxx
    String aclString = get(queuePrefix + getAclKey(acl), defaultAcl);
    return new AccessControlList(aclString);
  }

  /**
   * 为某一个队列、权限,设置权限组
   */
  public void setAcl(String queue, QueueACL acl, String aclString) {
	//yarn.scheduler.capacity.$queue.
    String queuePrefix = getQueuePrefix(queue);
    //设置属性yarn.scheduler.capacity.$queue.acl_$xxxx
    set(queuePrefix + getAclKey(acl), aclString);
  }

  /**
   * 获取该队列的所有权限，以及该权限对应的用户和用户组对象
   */
  public Map<QueueACL, AccessControlList> getAcls(String queue) {
    Map<QueueACL, AccessControlList> acls = new HashMap<QueueACL, AccessControlList>();
    for (QueueACL acl : QueueACL.values()) {
      acls.put(acl, getAcl(queue, acl));
    }
    return acls;
  }

  /**
   * 设置队列的权限
   * @param queue 队列
   * @param acls 表示某个权限，以及权限对应的使用人和组
   */
  public void setAcls(String queue, Map<QueueACL, AccessControlList> acls) {
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      setAcl(queue, e.getKey(), e.getValue().getAclString());
    }
  }

  /**
   * 获取queue节点的所有子节点数组集合
   */
  public String[] getQueues(String queue) {
    LOG.debug("CSConf - getQueues called for: queuePrefix=" + getQueuePrefix(queue));
    //yarn.scheduler.capacity.$queue.queues
    String[] queues = getStrings(getQueuePrefix(queue) + QUEUES);
    LOG.debug("CSConf - getQueues: queuePrefix=" + getQueuePrefix(queue) + 
        ", queues=" + ((queues == null) ? "" : StringUtils.arrayToString(queues)));
    return queues;
  }
  
  /**
   * yarn.scheduler.capacity.$queue.queues设置数组集合,表示queue下的子节点集合
   */
  public void setQueues(String queue, String[] subQueues) {
	//yarn.scheduler.capacity.$queue.queues
    set(getQueuePrefix(queue) + QUEUES, StringUtils.arrayToString(subQueues));
    LOG.debug("CSConf - setQueues: qPrefix=" + getQueuePrefix(queue) + 
        ", queues=" + StringUtils.arrayToString(subQueues));
  }
  
  /**
   * 获取最小资源
   */
  public Resource getMinimumAllocation() {
    int minimumMemory = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int minimumCores = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    return Resources.createResource(minimumMemory, minimumCores);
  }

  /**
   * 获取最大资源
   */
  public Resource getMaximumAllocation() {
    int maximumMemory = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    int maximumCores = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    return Resources.createResource(maximumMemory, maximumCores);
  }

  //获取该队列是否启动统计功能
  public boolean getEnableUserMetrics() {
	//yarn.scheduler.capacity.user-metrics.enable
    return getBoolean(ENABLE_USER_METRICS, DEFAULT_ENABLE_USER_METRICS);
  }

  //yarn.scheduler.capacity.node-locality-delay
  public int getNodeLocalityDelay() {
    int delay = getInt(NODE_LOCALITY_DELAY, DEFAULT_NODE_LOCALITY_DELAY);
    return (delay == DEFAULT_NODE_LOCALITY_DELAY) ? 0 : delay;
  }
  
  /**
   * 获取资源计算器
   */
  public ResourceCalculator getResourceCalculator() {
	  //yarn.scheduler.capacity.resource-calculator
    return ReflectionUtils.newInstance(
        getClass(
            RESOURCE_CALCULATOR_CLASS, 
            DEFAULT_RESOURCE_CALCULATOR_CLASS, 
            ResourceCalculator.class), 
        this);
  }

  public boolean getUsePortForNodeName() {
    return getBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
  }

  //yarn.scheduler.capacity.resource-calculator
  public void setResourceComparator(
      Class<? extends ResourceCalculator> resourceCalculatorClass) {
    setClass(
        RESOURCE_CALCULATOR_CLASS, 
        resourceCalculatorClass, 
        ResourceCalculator.class);
  }

  //yarn.scheduler.capacity.schedule-asynchronously.enable
  public boolean getScheduleAynschronously() {
    return getBoolean(SCHEDULE_ASYNCHRONOUSLY_ENABLE,
      DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE);
  }

  //yarn.scheduler.capacity.schedule-asynchronously.enable
  public void setScheduleAynschronously(boolean async) {
    setBoolean(SCHEDULE_ASYNCHRONOUSLY_ENABLE, async);
  }

  /**
   * 是否启动user、group映射到组特性
   * yarn.scheduler.capacity.queue-mappings-override.enable
   */
  public boolean getOverrideWithQueueMappings() {
    return getBoolean(ENABLE_QUEUE_MAPPING_OVERRIDE,
        DEFAULT_ENABLE_QUEUE_MAPPING_OVERRIDE);
  }

  /**
   * Returns a collection of strings, trimming leading and trailing whitespeace
   * on each value
   *
   * @param str
   *          String to parse
   * @param delim
   *          delimiter to separate the values
   * @return Collection of parsed elements.
   * 按delim拆分str,拆分结果是集合返回 
   */
  private static Collection<String> getTrimmedStringCollection(String str,String delim) {
    List<String> values = new ArrayList<String>();
    if (str == null)
      return values;
    StringTokenizer tokenizer = new StringTokenizer(str, delim);
    while (tokenizer.hasMoreTokens()) {
      String next = tokenizer.nextToken();
      if (next == null || next.trim().isEmpty()) {
        continue;
      }
      values.add(next.trim());
    }
    return values;
  }

  /**
   * Get user/group mappings to queues.
   *
   * @return user/groups mappings or null on illegal configs
   */
  public List<QueueMapping> getQueueMappings() {
    List<QueueMapping> mappings = new ArrayList<CapacitySchedulerConfiguration.QueueMapping>();
    //yarn.scheduler.capacity.queue-mappings
    Collection<String> mappingsString = getTrimmedStringCollection(QUEUE_MAPPING);
    for (String mappingValue : mappingsString) {
      //字符串按照:拆分成数组,数据源格式:MappingType:source:queue
      String[] mapping = getTrimmedStringCollection(mappingValue, ":").toArray(new String[] {});
      //数组一定是3个,并且第2和3的必须有内容
      if (mapping.length != 3 || mapping[1].length() == 0 || mapping[2].length() == 0) {
        throw new IllegalArgumentException("Illegal queue mapping " + mappingValue);
      }

      QueueMapping m;
      try {
        QueueMapping.MappingType mappingType;
        if (mapping[0].equals("u")) {
          mappingType = QueueMapping.MappingType.USER;
        } else if (mapping[0].equals("g")) {
          mappingType = QueueMapping.MappingType.GROUP;
        } else {
          throw new IllegalArgumentException("unknown mapping prefix " + mapping[0]);
        }
        m = new QueueMapping(
                mappingType,
                mapping[1],
                mapping[2]);
      } catch (Throwable t) {
        throw new IllegalArgumentException(
            "Illegal queue mapping " + mappingValue);
      }

      if (m != null) {
        mappings.add(m);
      }
    }

    return mappings;
  }

  /**
   * 该队列是否支持预约
   */
  public boolean isReservable(String queue) {
    //yarn.scheduler.capacity.$queue.reservable
    boolean isReservable = getBoolean(getQueuePrefix(queue) + IS_RESERVABLE, false);
    return isReservable;
  }

  //yarn.scheduler.capacity.$queue.reservable
  public void setReservable(String queue, boolean isReservable) {
    setBoolean(getQueuePrefix(queue) + IS_RESERVABLE, isReservable);
    LOG.debug("here setReservableQueue: queuePrefix=" + getQueuePrefix(queue)
        + ", isReservableQueue=" + isReservable(queue));
  }

  //yarn.scheduler.capacity.$queue.reservation-window
  public long getReservationWindow(String queue) {
    long reservationWindow =
        getLong(getQueuePrefix(queue) + RESERVATION_WINDOW,
            DEFAULT_RESERVATION_WINDOW);
    return reservationWindow;
  }

  //yarn.scheduler.capacity.$queue.average-capacity
  public float getAverageCapacity(String queue) {
    float avgCapacity =
        getFloat(getQueuePrefix(queue) + AVERAGE_CAPACITY,
            MAXIMUM_CAPACITY_VALUE);
    return avgCapacity;
  }

  //yarn.scheduler.capacity.$queue.instantaneous-max-capacity
  public float getInstantaneousMaxCapacity(String queue) {
    float instMaxCapacity =
        getFloat(getQueuePrefix(queue) + INSTANTANEOUS_MAX_CAPACITY,
            MAXIMUM_CAPACITY_VALUE);
    return instMaxCapacity;
  }

  //yarn.scheduler.capacity.$queue.instantaneous-max-capacity
  public void setInstantaneousMaxCapacity(String queue, float instMaxCapacity) {
    setFloat(getQueuePrefix(queue) + INSTANTANEOUS_MAX_CAPACITY,
        instMaxCapacity);
  }

  //yarn.scheduler.capacity.$queue.reservation-window
  public void setReservationWindow(String queue, long reservationWindow) {
    setLong(getQueuePrefix(queue) + RESERVATION_WINDOW, reservationWindow);
  }

  //yarn.scheduler.capacity.$queue.average-capacity
  public void setAverageCapacity(String queue, float avgCapacity) {
    setFloat(getQueuePrefix(queue) + AVERAGE_CAPACITY, avgCapacity);
  }

  //yarn.scheduler.capacity.$queue.reservation-policy
  public String getReservationAdmissionPolicy(String queue) {
    String reservationPolicy =
        get(getQueuePrefix(queue) + RESERVATION_ADMISSION_POLICY,
            DEFAULT_RESERVATION_ADMISSION_POLICY);
    return reservationPolicy;
  }

  //yarn.scheduler.capacity.$queue.reservation-policy
  public void setReservationAdmissionPolicy(String queue,
      String reservationPolicy) {
    set(getQueuePrefix(queue) + RESERVATION_ADMISSION_POLICY, reservationPolicy);
  }

  //yarn.scheduler.capacity.$queue.reservation-agent
  public String getReservationAgent(String queue) {
    String reservationAgent =
        get(getQueuePrefix(queue) + RESERVATION_AGENT_NAME,
            DEFAULT_RESERVATION_AGENT_NAME);
    return reservationAgent;
  }

  //yarn.scheduler.capacity.$queue.reservation-agent
  public void setReservationAgent(String queue, String reservationPolicy) {
    set(getQueuePrefix(queue) + RESERVATION_AGENT_NAME, reservationPolicy);
  }

  //yarn.scheduler.capacity.$queue.show-reservations-as-queues
  public boolean getShowReservationAsQueues(String queuePath) {
    boolean showReservationAsQueues =
        getBoolean(getQueuePrefix(queuePath)
            + RESERVATION_SHOW_RESERVATION_AS_QUEUE, false);
    return showReservationAsQueues;
  }

  //yarn.scheduler.capacity.$queue.reservation-planner
  public String getReplanner(String queue) {
    String replanner =
        get(getQueuePrefix(queue) + RESERVATION_PLANNER_NAME,
            DEFAULT_RESERVATION_PLANNER_NAME);
    return replanner;
  }

  //yarn.scheduler.capacity.$queue.reservation-move-on-expiry
  public boolean getMoveOnExpiry(String queue) {
    boolean killOnExpiry =
        getBoolean(getQueuePrefix(queue) + RESERVATION_MOVE_ON_EXPIRY,
            DEFAULT_RESERVATION_MOVE_ON_EXPIRY);
    return killOnExpiry;
  }

  //yarn.scheduler.capacity.$queue.reservation-enforcement-window
  public long getEnforcementWindow(String queue) {
    long enforcementWindow =
        getLong(getQueuePrefix(queue) + RESERVATION_ENFORCEMENT_WINDOW,
            DEFAULT_RESERVATION_ENFORCEMENT_WINDOW);
    return enforcementWindow;
  }
}

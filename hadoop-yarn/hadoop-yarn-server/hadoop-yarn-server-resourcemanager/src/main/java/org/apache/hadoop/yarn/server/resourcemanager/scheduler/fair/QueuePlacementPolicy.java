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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.ReflectionUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 队列可替代的代理类
 * 返回所有的查找app到一个队列的规则
 */
@Private
@Unstable
public class QueuePlacementPolicy {
	
	//所有的代理类映射
  private static final Map<String, Class<? extends QueuePlacementRule>> ruleClasses;
  static {
    Map<String, Class<? extends QueuePlacementRule>> map =
        new HashMap<String, Class<? extends QueuePlacementRule>>();
    map.put("user", QueuePlacementRule.User.class);
    map.put("primaryGroup", QueuePlacementRule.PrimaryGroup.class);
    map.put("secondaryGroupExistingQueue",
        QueuePlacementRule.SecondaryGroupExistingQueue.class);
    map.put("specified", QueuePlacementRule.Specified.class);
    map.put("nestedUserQueue",
        QueuePlacementRule.NestedUserQueue.class);
    map.put("default", QueuePlacementRule.Default.class);
    map.put("reject", QueuePlacementRule.Reject.class);
    ruleClasses = Collections.unmodifiableMap(map);
  }
  
  private final List<QueuePlacementRule> rules;
  private final Map<FSQueueType, Set<String>> configuredQueues;
  private final Groups groups;
  
  /**
   * 
   * @param rules 最终的规则
   * @param configuredQueues key是叶子节点还是父节点,value表示对应的队列name集合
   * @param conf
   * @throws AllocationConfigurationException
   */
  public QueuePlacementPolicy(List<QueuePlacementRule> rules,
      Map<FSQueueType, Set<String>> configuredQueues, Configuration conf)
      throws AllocationConfigurationException {
    for (int i = 0; i < rules.size()-1; i++) {//校验非最后一个规则,一定不能是Terminal的,都是可以继续向下查询的
      if (rules.get(i).isTerminal()) {
        throw new AllocationConfigurationException("Rules after rule "
            + i + " in queue placement policy can never be reached");
      }
    }
    if (!rules.get(rules.size()-1).isTerminal()) {//最后一个一定是Terminal的,不允许继续向下查询了
      throw new AllocationConfigurationException(
          "Could get past last queue placement rule without assigning");
    }
    this.rules = rules;
    this.configuredQueues = configuredQueues;
    groups = new Groups(conf);
  }
  
  /**
   * Builds a QueuePlacementPolicy from an xml element.
   * 从xml中建立规则队列
   */
  public static QueuePlacementPolicy fromXml(Element el,
      Map<FSQueueType, Set<String>> configuredQueues, Configuration conf)
      throws AllocationConfigurationException {
    List<QueuePlacementRule> rules = new ArrayList<QueuePlacementRule>();
    
    //循环所有子节点
    NodeList elements = el.getChildNodes();
    //根据子节点,一次创建一个规则,并且串联起来
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        QueuePlacementRule rule = createAndInitializeRule(node);
        rules.add(rule);
      }
    }
    return new QueuePlacementPolicy(rules, configuredQueues, conf);
  }
  
  /**
   * Create and initialize a rule given a xml node
   * @param node
   * @return QueuePlacementPolicy
   * @throws AllocationConfigurationException
   * 通过一个xml的node去创建和初始化该规则
   */
  public static QueuePlacementRule createAndInitializeRule(Node node)
      throws AllocationConfigurationException {
    Element element = (Element) node;

    //获取规则名称
    String ruleName = element.getAttribute("name");
    if ("".equals(ruleName)) {
      throw new AllocationConfigurationException("No name provided for a "
          + "rule element");
    }

    //通过规则名称找到对应的class
    Class<? extends QueuePlacementRule> clazz = ruleClasses.get(ruleName);
    if (clazz == null) {
      throw new AllocationConfigurationException("No rule class found for "
          + ruleName);
    }
    
    //实例化该class规则
    QueuePlacementRule rule = ReflectionUtils.newInstance(clazz, null);
    rule.initializeFromXml(element);
    return rule;
  }
    
  /**
   * Build a simple queue placement policy from the allow-undeclared-pools and
   * user-as-default-queue configuration options.
   * 通过配置文件去创建和初始化该规则
   */
  public static QueuePlacementPolicy fromConfiguration(Configuration conf,
      Map<FSQueueType, Set<String>> configuredQueues) {
    boolean create = conf.getBoolean(
        FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS,
        FairSchedulerConfiguration.DEFAULT_ALLOW_UNDECLARED_POOLS);
    boolean userAsDefaultQueue = conf.getBoolean(
        FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE,
        FairSchedulerConfiguration.DEFAULT_USER_AS_DEFAULT_QUEUE);//默认是true
    
    //最终的规则集合
    List<QueuePlacementRule> rules = new ArrayList<QueuePlacementRule>();
    
    rules.add(new QueuePlacementRule.Specified().initialize(create, null));//分配app到提交者预期想提交的队列中
    
    if (userAsDefaultQueue) {
      rules.add(new QueuePlacementRule.User().initialize(create, null));//提交app到提交者所在的user队列中
    }
    if (!userAsDefaultQueue || !create) {
      rules.add(new QueuePlacementRule.Default().initialize(true, null));//使app分配到默认的队列中
    }
    try {
      return new QueuePlacementPolicy(rules, configuredQueues, conf);
    } catch (AllocationConfigurationException ex) {
      throw new RuntimeException("Should never hit exception when loading" +
      		"placement policy from conf", ex);
    }
  }

  /**
   * Applies this rule to an app with the given requested queue and user/group
   * information.
   * 
   * @param requestedQueue
   *    The queue specified in the ApplicationSubmissionContext
   * @param user
   *    The user submitting the app
   * @return
   *    The name of the queue to assign the app to.  Or null if the app should
   *    be rejected.
   * @throws IOException
   *    If an exception is encountered while getting the user's groups
   *    分配一个app到一个队列中
   */
  public String assignAppToQueue(String requestedQueue, String user)
      throws IOException {
    for (QueuePlacementRule rule : rules) {//循环所有的规则
      String queue = rule.assignAppToQueue(requestedQueue, user, groups,
          configuredQueues);//在该规则下分配一个app到队列总
      
      /**
       * 如果该规则返回的结果是null,说明拒绝该app到任何队列,则返回
       * 如果该规则返回的结果不是"",说明该app找到了一个队列,则返回
       */
      if (queue == null || !queue.isEmpty()) {
        return queue;
      }
    }
    throw new IllegalStateException("Should have applied a rule before " +
    		"reaching here");
  }
  
  //返回所有的查找app到一个队列的规则
  public List<QueuePlacementRule> getRules() {
    return rules;
  }
}

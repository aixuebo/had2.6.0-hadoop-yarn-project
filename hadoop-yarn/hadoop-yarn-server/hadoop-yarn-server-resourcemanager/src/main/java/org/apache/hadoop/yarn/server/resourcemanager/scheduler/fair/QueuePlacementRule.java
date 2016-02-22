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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public abstract class QueuePlacementRule {
  protected boolean create;
  
  /**
   * Initializes the rule with any arguments.
   * 
   * @param args
   *    Additional attributes of the rule's xml element other than create.
   * 初始化,通过参数Map初始化
   */
  public QueuePlacementRule initialize(boolean create, Map<String, String> args) {
    this.create = create;
    return this;
  }
  
  /**
   * @param requestedQueue
   *    The queue explicitly requested. 这个队列是用户明确要申请提交到该队列中
   * @param user
   *    The user submitting the app.提交app的user
   * @param groups
   *    The groups of the user submitting the app.提交app的user所在的组集合
   * @param configuredQueues
   *    The queues specified in the scheduler configuration.调度器中每一个叶子节点以及父节点对应的queue名称集合
   * @return
   *    The queue to place the app into. An empty string indicates that we should
   *    continue to the next rule, and null indicates that the app should be rejected.
   * 分配app到一个队列中   
   * 返回值:返回值是一个队列名称,即该app要被代替到哪个队列中。
   * 如果返回值是"",则表示我们应该考虑下一个规则
   * 如果返回值是null,则表示我们应该拒绝这个app
   */
  public String assignAppToQueue(String requestedQueue, String user,
      Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
      throws IOException {
   String queue = getQueueForApp(requestedQueue, user, groups,
        configuredQueues);//返回该app要被替换到哪个队列中
   
    //如果该队列queue是存在的,则返回该队列即可
    if (create || configuredQueues.get(FSQueueType.LEAF).contains(queue)
        || configuredQueues.get(FSQueueType.PARENT).contains(queue)) {
      return queue;
    } else {//如果该queue不存在,则返回"",继续查找下一个规则
      return "";
    }
  }
  
  //从元素中初始化该规则
  public void initializeFromXml(Element el)
      throws AllocationConfigurationException {
    boolean create = true;
    NamedNodeMap attributes = el.getAttributes();
    Map<String, String> args = new HashMap<String, String>();
    for (int i = 0; i < attributes.getLength(); i++) {
      Node node = attributes.item(i);
      String key = node.getNodeName();
      String value = node.getNodeValue();
      if (key.equals("create")) {
        create = Boolean.parseBoolean(value);
      } else {
        args.put(key, value);
      }
    }
    initialize(create, args);
  }
  
  /**
   * Returns true if this rule never tells the policy to continue.
   * 返回true,表示终止了,即不会在继续查找代替的队列了
   */
  public abstract boolean isTerminal();
  
  /**
   * Applies this rule to an app with the given requested queue and user/group
   * information.
   * 
   * @param requestedQueue 这个队列是用户明确要申请提交到该队列中
   *    The queue specified in the ApplicationSubmissionContext
   * @param user 提交app的user
   *    The user submitting the app.
   * @param groups 提交app的user所在的组集合
   *    The groups of the user submitting the app.
   *    
   * @param configuredQueues 调度器中每一个叶子节点以及父节点对应的queue名称集合
   *    The queues specified in the scheduler configuration.
   *    
   * @return 
   *    The name of the queue to assign the app to, or null to empty string
   *    continue to the next rule.
   *    分配app到一个队列中   
   * 返回值:返回值是一个队列名称,即该app要被代替到哪个队列中。
   * 如果返回值是"",则表示我们应该考虑下一个规则
   * 如果返回值是null,则表示我们应该拒绝这个app
   */
  protected abstract String getQueueForApp(String requestedQueue, String user,
      Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
      throws IOException;

  /**
   * Places apps in queues by username of the submitter
   * 提交app到提交者所在的user队列中
   */
  public static class User extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      return "root." + user;
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }
  
  /**
   * Places apps in queues by primary group of the submitter
   * 提交app到提交者的第一个组队列中
   */
  public static class PrimaryGroup extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
        throws IOException {
      return "root." + groups.getGroups(user).get(0);
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }
  
  /**
   * Places apps in queues by secondary group of the submitter
   * 
   * Match will be made on first secondary group that exist in
   * queues
   * 提交app到提交者的第二个已经存在的组中
   */
  public static class SecondaryGroupExistingQueue extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
        throws IOException {
      List<String> groupNames = groups.getGroups(user);
      for (int i = 1; i < groupNames.size(); i++) {
        String group = groupNames.get(i);
        if (configuredQueues.get(FSQueueType.LEAF).contains("root." + group)
            || configuredQueues.get(FSQueueType.PARENT).contains(
                "root." + group)) {
          return "root." + groupNames.get(i);
        }
      }
      
      return "";
    }
        
    @Override
    public boolean isTerminal() {
      return false;
    }
  }

  /**
   * Places apps in queues with name of the submitter under the queue
   * returned by the nested rule.
   * 使用可以嵌套的规则语法,分配一个app到哪个队列
   */
  public static class NestedUserQueue extends QueuePlacementRule {
    @VisibleForTesting
    QueuePlacementRule nestedRule;//嵌套规则对象

    /**
     * Parse xml and instantiate the nested rule 
     * 解析xml,初始化嵌套的规则语法
     */
    @Override
    public void initializeFromXml(Element el)
        throws AllocationConfigurationException {
      NodeList elements = el.getChildNodes();//所有的子元素

      for (int i = 0; i < elements.getLength(); i++) {
        Node node = elements.item(i);
        if (node instanceof Element) {
          Element element = (Element) node;
          if ("rule".equals(element.getTagName())) {//元素必须以rule为tag
            QueuePlacementRule rule = QueuePlacementPolicy
                .createAndInitializeRule(node);//创建规则
            if (rule == null) {
              throw new AllocationConfigurationException(
                  "Unable to create nested rule in nestedUserQueue rule");
            }
            this.nestedRule = rule;
            break;
          } else {
            continue;
          }
        }
      }

      if (this.nestedRule == null) {
        throw new AllocationConfigurationException(
            "No nested rule specified in <nestedUserQueue> rule");
      }
      //初始化规则
      super.initializeFromXml(el);
    }
    
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
        throws IOException {
      // Apply the nested rule应用嵌套分配规则,分配app到一个队列
      String queueName = nestedRule.assignAppToQueue(requestedQueue, user,
          groups, configuredQueues);
      
      if (queueName != null && queueName.length() != 0) {
        if (!queueName.startsWith("root.")) {
          queueName = "root." + queueName;
        }
        
        // Verify if the queue returned by the nested rule is an configured leaf queue,
        // if yes then skip to next rule in the queue placement policy
        //校验,如果通过嵌套的规则,返回的队列是叶子队列,则我们跳过这个规则,继续寻找
        if (configuredQueues.get(FSQueueType.LEAF).contains(queueName)) {
          return "";
        }
        return queueName + "." + user;
      }
      return queueName;
    }

    @Override
    public boolean isTerminal() {
      return false;
    }
  }
  
  /**
   * Places apps in queues by requested queue of the submitter
   * 分配app到提交者预期想提交的队列中
   */
  public static class Specified extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      if (requestedQueue.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)) {
        return "";
      } else {
        if (!requestedQueue.startsWith("root.")) {
          requestedQueue = "root." + requestedQueue;
        }
        return requestedQueue;
      }
    }
    
    @Override
    public boolean isTerminal() {
      return false;
    }
  }
  
  /**
   * Places apps in the specified default queue. If no default queue is
   * specified the app is placed in root.default queue.
   * 使app分配到默认的队列中
   */
  public static class Default extends QueuePlacementRule {
    @VisibleForTesting
    String defaultQueueName;//默认队列

    @Override
    public QueuePlacementRule initialize(boolean create,
        Map<String, String> args) {
      if (defaultQueueName == null) {
        defaultQueueName = "root." + YarnConfiguration.DEFAULT_QUEUE_NAME;
      }
      return super.initialize(create, args);
    }
    
    @Override
    public void initializeFromXml(Element el)
        throws AllocationConfigurationException {
      defaultQueueName = el.getAttribute("queue");
      if (defaultQueueName != null && !defaultQueueName.isEmpty()) {
        if (!defaultQueueName.startsWith("root.")) {
          defaultQueueName = "root." + defaultQueueName;
        }
      } else {
        defaultQueueName = "root." + YarnConfiguration.DEFAULT_QUEUE_NAME;
      }
      super.initializeFromXml(el);
    }

    //分配到默认队列
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      return defaultQueueName;
    }

    @Override
    public boolean isTerminal() {
      return true;
    }
  }

  /**
   * Rejects all apps
   * 拒绝所有的app
   */
  public static class Reject extends QueuePlacementRule {
	  
	//不会在分配app到队列了
    @Override
    public String assignAppToQueue(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      return null;
    }
    
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isTerminal() {
      return true;
    }
  }
}

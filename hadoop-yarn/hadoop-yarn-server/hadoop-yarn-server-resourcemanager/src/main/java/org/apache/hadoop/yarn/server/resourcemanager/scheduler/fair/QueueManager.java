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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.xml.sax.SAXException;

/**
 * Maintains a list of queues as well as scheduling parameters for each queue,
 * such as guaranteed share allocations, from the fair scheduler config file.
 * 管理所有的配置文件中队列Queue,每一个Queue的name都是用.连接起来的全路径,以root为开头
 */
@Private
@Unstable
public class QueueManager {
  public static final Log LOG = LogFactory.getLog(
    QueueManager.class.getName());

  public static final String ROOT_QUEUE = "root";
  
  private final FairScheduler scheduler;//调度器

  //每一个Queue的name都是用.连接起来的全路径,以root为开头
  private final Collection<FSLeafQueue> leafQueues = new CopyOnWriteArrayList<FSLeafQueue>(); 
  //key是queue的name,value是FSQueue队列,每一个Queue的name都是用.连接起来的全路径,以root为开头
  private final Map<String, FSQueue> queues = new HashMap<String, FSQueue>();
  private FSParentQueue rootQueue;//根ROOT队列

  public QueueManager(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }
  
  public FSParentQueue getRootQueue() {
    return rootQueue;
  }

  /**
   * 1.创建root的父节点
   * 2.创建default的叶子节点
   */
  public void initialize(Configuration conf) throws IOException,
      SAXException, AllocationConfigurationException, ParserConfigurationException {
    rootQueue = new FSParentQueue("root", scheduler, null);
    queues.put(rootQueue.getName(), rootQueue);
    
    // Create the default queue创建default的叶子节点
    getLeafQueue(YarnConfiguration.DEFAULT_QUEUE_NAME, true);
  }
  
  /**
   * Get a leaf queue by name, creating it if the create param is true and is necessary.
   * If the queue is not or can not be a leaf queue, i.e. it already exists as a
   * parent queue, or one of the parents in its name is already a leaf queue,
   * null is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   * 创建name的叶子节点,返回值一定是FSLeafQueue节点,否则返回null
   */
  public FSLeafQueue getLeafQueue(String name, boolean create) {
    FSQueue queue = getQueue(name, create, FSQueueType.LEAF);
    if (queue instanceof FSParentQueue) {
      return null;
    }
    return (FSLeafQueue) queue;
  }
  
  /**
   * Get a parent queue by name, creating it if the create param is true and is necessary.
   * If the queue is not or can not be a parent queue, i.e. it already exists as a
   * leaf queue, or one of the parents in its name is already a leaf queue,
   * null is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   * 创建name的父节点,返回值一定是FSParentQueue节点,否则返回null
   */
  public FSParentQueue getParentQueue(String name, boolean create) {
    FSQueue queue = getQueue(name, create, FSQueueType.PARENT);
    if (queue instanceof FSLeafQueue) {
      return null;
    }
    return (FSParentQueue) queue;
  }
  
  /**
   * 创建队列name,并且将name链路上所有的队列都创建了,同时创建完队列后,调用recomputeSteadyShares方法,重新计算资源
   * @param create true则表示如果name不存在,则创建
   * @param queueType
   * 返回name对应的队列
   */
  private FSQueue getQueue(String name, boolean create, FSQueueType queueType) {
    name = ensureRootPrefix(name);
    synchronized (queues) {
      FSQueue queue = queues.get(name);
      if (queue == null && create) {
        // if the queue doesn't exist,create it and return
        queue = createQueue(name, queueType);

        // Update steady fair share for all queues 更新队列的资源信息
        if (queue != null) {
          rootQueue.recomputeSteadyShares();
        }
      }
      return queue;
    }
  }
  
  /**
   * Creates a leaf or parent queue based on what is specified in 'queueType' 
   * and places it in the tree. Creates any parents that don't already exist.
   * 
   * @return
   *    the created queue, if successful. null if not allowed (one of the parent
   *    queues in the queue name is already a leaf queue)
   *    创建一个FSQueue队列
   * 返回该name的队列
   */
  private FSQueue createQueue(String name, FSQueueType queueType) {
    List<String> newQueueNames = new ArrayList<String>();//要添加的队列
    newQueueNames.add(name);
    int sepIndex = name.length();
    FSParentQueue parent = null;

    // Move up the queue tree until we reach one that exists.
    //例如aa.bb.cc
    while (sepIndex != -1) {
      sepIndex = name.lastIndexOf('.', sepIndex-1);
      FSQueue queue;
      String curName = null;
      curName = name.substring(0, sepIndex);//第一次循环执行结果是aa.bb
      queue = queues.get(curName);

      if (queue == null) {
        newQueueNames.add(curName);//因为该父队列不存在,则添加该父队列
      } else {
        if (queue instanceof FSParentQueue) {//说明队列存在,因此必须是FSParentQueue
          parent = (FSParentQueue)queue;
          break;
        } else {//说明有异常,因此返回null
          return null;
        }
      }
    }
    
    // At this point, parent refers to the deepest existing parent of the
    // queue to create.
    // Now that we know everything worked out, make all the queues
    // and add them to the map.
    AllocationConfiguration queueConf = scheduler.getAllocationConfiguration();
    FSLeafQueue leafQueue = null;
    for (int i = newQueueNames.size()-1; i >= 0; i--) {//例如队列的name为aa.bb.cc,则从aa开始创建队列,最后一个是cc
      String queueName = newQueueNames.get(i);
      if (i == 0 && queueType != FSQueueType.PARENT) {//i=0表示已经到了参数cc了,并且该节点又不是父节点,则因此创建子节点
        leafQueue = new FSLeafQueue(name, scheduler, parent);
        try {
          leafQueue.setPolicy(queueConf.getDefaultSchedulingPolicy());
        } catch (AllocationConfigurationException ex) {
          LOG.warn("Failed to set default scheduling policy "
              + queueConf.getDefaultSchedulingPolicy() + " on new leaf queue.", ex);
        }
        parent.addChildQueue(leafQueue);
        queues.put(leafQueue.getName(), leafQueue);
        leafQueues.add(leafQueue);
        leafQueue.updatePreemptionVariables();
        return leafQueue;
      } else {
    	//创建一个FSParentQueue队列
        FSParentQueue newParent = new FSParentQueue(queueName, scheduler, parent);
        try {
          newParent.setPolicy(queueConf.getDefaultSchedulingPolicy());
        } catch (AllocationConfigurationException ex) {
          LOG.warn("Failed to set default scheduling policy "
              + queueConf.getDefaultSchedulingPolicy() + " on new parent queue.", ex);
        }
        parent.addChildQueue(newParent);
        queues.put(newParent.getName(), newParent);
        newParent.updatePreemptionVariables();
        parent = newParent;
      }
    }

    return parent;
  }

  /**
   * Make way for the given queue if possible, by removing incompatible
   * queues with no apps in them. Incompatibility could be due to
   * (1) queueToCreate being currently a parent but needs to change to leaf
   * (2) queueToCreate being currently a leaf but needs to change to parent
   * (3) an existing leaf queue in the ancestry of queueToCreate.
   * 
   * We will never remove the root queue or the default queue in this way.
   *
   * @return true if we can create queueToCreate or it already exists.
   */
  private boolean removeEmptyIncompatibleQueues(String queueToCreate,
      FSQueueType queueType) {
    queueToCreate = ensureRootPrefix(queueToCreate);

    // Ensure queueToCreate is not root and doesn't have the default queue in its
    // ancestry.
    if (queueToCreate.equals(ROOT_QUEUE) ||
        queueToCreate.startsWith(
            ROOT_QUEUE + "." + YarnConfiguration.DEFAULT_QUEUE_NAME + ".")) {
      return false;
    }

    FSQueue queue = queues.get(queueToCreate);
    // Queue exists already.
    if (queue != null) {
      if (queue instanceof FSLeafQueue) {
        if (queueType == FSQueueType.LEAF) {
          // if queue is already a leaf then return true
          return true;
        }
        // remove incompatibility since queue is a leaf currently
        // needs to change to a parent.
        return removeQueueIfEmpty(queue);//说明队列不是叶子队列,则判断是否是空的父队列
      } else {
        if (queueType == FSQueueType.PARENT) {
          return true;
        }
        // If it's an existing parent queue and needs to change to leaf, 
        // remove it if it's empty.
        return removeQueueIfEmpty(queue);
      }
    }

    // Queue doesn't exist already. Check if the new queue would be created
    // under an existing leaf queue. If so, try removing that leaf queue.
    //例如queueToCreate为aa.bb.cc
    int sepIndex = queueToCreate.length();
    sepIndex = queueToCreate.lastIndexOf('.', sepIndex-1);
    while (sepIndex != -1) {
      String prefixString = queueToCreate.substring(0, sepIndex);//第一次循环则改成aa.bb
      FSQueue prefixQueue = queues.get(prefixString);
      if (prefixQueue != null && prefixQueue instanceof FSLeafQueue) {
        return removeQueueIfEmpty(prefixQueue);
      }
      sepIndex = queueToCreate.lastIndexOf('.', sepIndex-1);
    }
    return true;
  }

  /**
   * Remove the queue if it and its descendents are all empty.
   * @param queue
   * @return true if removed, false otherwise
   * 如果队列是空的,则移除该队列
   */
  private boolean removeQueueIfEmpty(FSQueue queue) {
    if (isEmpty(queue)) {
      removeQueue(queue);//彻底移除一个队列的相关关系
      return true;
    }
    return false;
  }
  
  /**
   * 彻底移除一个队列的相关关系
   * Remove a queue and all its descendents.
   * 1.从queues缓存中移除该队列
   * 2.从该队列的父队列中,移除该队列的映射
   * 3.如果是叶子队列,直接移除即可
   * 4.如果不是叶子队列,则移除所有的子队列
   */
  private void removeQueue(FSQueue queue) {
    if (queue instanceof FSLeafQueue) {
      leafQueues.remove(queue);//如果是叶子队列,直接移除即可
    } else {//如果不是叶子队列,则移除所有的子队列
      List<FSQueue> childQueues = queue.getChildQueues();
      while (!childQueues.isEmpty()) {
        removeQueue(childQueues.get(0));
      }
    }
    queues.remove(queue.getName());//从queues缓存中移除该队列
    queue.getParent().getChildQueues().remove(queue);//从该队列的父队列中,移除该队列的映射
  }
  
  /**
   * Returns true if there are no applications, running or not, in the given
   * queue or any of its descendents.
   * true表示该队列是否是空队列
   * 1.如果是叶子节点,则是空的就是空的,即就返回true
   * 2.如果是父节点,则所有的子节点必须都是空的,如果有任意一个不是空的,则返回false,都是空的,则返回true
   */
  protected boolean isEmpty(FSQueue queue) {
    if (queue instanceof FSLeafQueue) {
      FSLeafQueue leafQueue = (FSLeafQueue)queue;
      return queue.getNumRunnableApps() == 0 &&
          leafQueue.getNonRunnableAppSchedulables().isEmpty();
    } else {
      for (FSQueue child : queue.getChildQueues()) {
        if (!isEmpty(child)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Gets a queue by name.
   * 通过名字获取队列
   */
  public FSQueue getQueue(String name) {
    name = ensureRootPrefix(name);
    synchronized (queues) {
      return queues.get(name);
    }
  }

  /**
   * Return whether a queue exists already.
   * 判断名字的队列是否存在
   */
  public boolean exists(String name) {
    name = ensureRootPrefix(name);
    synchronized (queues) {
      return queues.containsKey(name);
    }
  }
  
  /**
   * Get a collection of all leaf queues
   * 获取所有的叶子节点队列
   */
  public Collection<FSLeafQueue> getLeafQueues() {
    synchronized (queues) {
      return leafQueues;
    }
  }
  
  /**
   * Get a collection of all queues
   * 获取所有的队列,包括父队列以及叶子队列
   */
  public Collection<FSQueue> getQueues() {
    return queues.values();
  }
  
  /**
   * 将name改成root.name的方式
   */
  private String ensureRootPrefix(String name) {
    if (!name.startsWith(ROOT_QUEUE + ".") && !name.equals(ROOT_QUEUE)) {
      name = ROOT_QUEUE + "." + name;
    }
    return name;
  }
  
  public void updateAllocationConfiguration(AllocationConfiguration queueConf) {
    // Create leaf queues and the parent queues in a leaf's ancestry if they do not exist
    for (String name : queueConf.getConfiguredQueues().get(FSQueueType.LEAF)) {
      if (removeEmptyIncompatibleQueues(name, FSQueueType.LEAF)) {
        getLeafQueue(name, true);//创建name的叶子节点
      }
    }

    // At this point all leaves and 'parents with at least one child' would have been created.
    // Now create parents with no configured leaf.
    for (String name : queueConf.getConfiguredQueues().get(
        FSQueueType.PARENT)) {
      if (removeEmptyIncompatibleQueues(name, FSQueueType.PARENT)) {
        getParentQueue(name, true);//创建name的父节点
      }
    }
    
    //循环所有的父节点和叶子节点,然后调用每一个节点的SchedulingPolicy的initialize方法
    for (FSQueue queue : queues.values()) {
      // Update queue metrics
      FSQueueMetrics queueMetrics = queue.getMetrics();
      queueMetrics.setMinShare(queue.getMinShare());
      queueMetrics.setMaxShare(queue.getMaxShare());
      // Set scheduling policies
      try {
        SchedulingPolicy policy = queueConf.getSchedulingPolicy(queue.getName());
        policy.initialize(scheduler.getClusterResource());
        queue.setPolicy(policy);
      } catch (AllocationConfigurationException ex) {
        LOG.warn("Cannot apply configured scheduling policy to queue "
            + queue.getName(), ex);
      }
    }

    // Update steady fair shares for all queues
    rootQueue.recomputeSteadyShares();
    // Update the fair share preemption timeouts and preemption for all queues
    // recursively
    rootQueue.updatePreemptionVariables();
  }
}

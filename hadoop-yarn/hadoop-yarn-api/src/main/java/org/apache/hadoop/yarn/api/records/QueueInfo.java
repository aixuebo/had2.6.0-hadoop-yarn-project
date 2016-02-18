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

package org.apache.hadoop.yarn.api.records;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>QueueInfo is a report of the runtime information of the queue.</p>
 * 
 * <p>It includes information such as:
 *   <ul>
 *     <li>Queue name.</li>
 *     <li>Capacity of the queue.</li>
 *     <li>Maximum capacity of the queue.</li>
 *     <li>Current capacity of the queue.</li>
 *     <li>Child queues.</li>
 *     <li>Running applications.</li>
 *     <li>{@link QueueState} of the queue.</li>
 *   </ul>
 * </p>
 *
 * @see QueueState
 * @see ApplicationClientProtocol#getQueueInfo(org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest)
 * 
 *     <name>yarn.scheduler.capacity.$queue.capacity</name>根的时候设置为100,表示百分比,即100%
    1.capacity 就是100%,即1,因此0<capacity<1,该值仅仅表示为该队列在父队列的基础上的占比,配置文件中设置多少就是多少
    2.maximumCapacity 与capacity含义一致,就是配置文件中原始的配置元素的百分比,即如果配置为50,则该值表示50%,即0.5
    3.absoluteCapacity,表示绝对的capacity ,因此该值会根据父队列占总资源的capacity进行分配,
                 比如:设置为50,则表示占比50,比如该队列还有子队列,设置为30,则表示子队列的资源使用为总资源*50%*30%
    4.absoluteMaxCapacity 与absoluteCapacity含义一致,但是表示该队列最多允许分配的资源占比量
 */
@Public
@Stable
public abstract class QueueInfo {
  
  @Private
  @Unstable
  public static QueueInfo newInstance(String queueName, float capacity,
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression) {
    QueueInfo queueInfo = Records.newRecord(QueueInfo.class);
    queueInfo.setQueueName(queueName);
    queueInfo.setCapacity(capacity);//配置文件中配置的该队列的资源占用百分比
    queueInfo.setMaximumCapacity(maximumCapacity);//配置文件中配置的该队列的最大资源占用百分比
    queueInfo.setCurrentCapacity(currentCapacity);//该队列真正已经使用的占比,公式:usedResources/(clusterResource*childQueue.getAbsoluteCapacity(),翻译 已经使用的资源量/该队列分配的capacity量,即就是该队列的使用百分比
    queueInfo.setChildQueues(childQueues);//该队列的所有子队列
    queueInfo.setApplications(applications);//该队列的所有应用
    queueInfo.setQueueState(queueState);//该队列的状态
    queueInfo.setAccessibleNodeLabels(accessibleNodeLabels);//该队列的标签
    queueInfo.setDefaultNodeLabelExpression(defaultNodeLabelExpression);//该队列的默认标签
    return queueInfo;
  }

  /**
   * Get the <em>name</em> of the queue.
   * @return <em>name</em> of the queue
   */
  @Public
  @Stable
  public abstract String getQueueName();
  
  @Private
  @Unstable
  public abstract void setQueueName(String queueName);
  
  /**
   * Get the <em>configured capacity</em> of the queue.
   * @return <em>configured capacity</em> of the queue
   */
  @Public
  @Stable
  public abstract float getCapacity();
  
  @Private
  @Unstable
  public abstract void setCapacity(float capacity);
  
  /**
   * Get the <em>maximum capacity</em> of the queue.
   * @return <em>maximum capacity</em> of the queue
   */
  @Public
  @Stable
  public abstract float getMaximumCapacity();
  
  @Private
  @Unstable
  public abstract void setMaximumCapacity(float maximumCapacity);
  
  /**
   * Get the <em>current capacity</em> of the queue.
   * @return <em>current capacity</em> of the queue
   */
  @Public
  @Stable
  public abstract float getCurrentCapacity();
  
  @Private
  @Unstable
  public abstract void setCurrentCapacity(float currentCapacity);
  
  /**
   * Get the <em>child queues</em> of the queue.
   * @return <em>child queues</em> of the queue
   */
  @Public
  @Stable
  public abstract List<QueueInfo> getChildQueues();
  
  @Private
  @Unstable
  public abstract void setChildQueues(List<QueueInfo> childQueues);
  
  /**
   * Get the <em>running applications</em> of the queue.
   * @return <em>running applications</em> of the queue
   */
  @Public
  @Stable
  public abstract List<ApplicationReport> getApplications();
  
  @Private
  @Unstable
  public abstract void setApplications(List<ApplicationReport> applications);
  
  /**
   * Get the <code>QueueState</code> of the queue.
   * @return <code>QueueState</code> of the queue
   */
  @Public
  @Stable
  public abstract QueueState getQueueState();
  
  @Private
  @Unstable
  public abstract void setQueueState(QueueState queueState);
  
  /**
   * Get the <code>accessible node labels</code> of the queue.
   * @return <code>accessible node labels</code> of the queue
   */
  @Public
  @Stable
  public abstract Set<String> getAccessibleNodeLabels();
  
  /**
   * Set the <code>accessible node labels</code> of the queue.
   */
  @Private
  @Unstable
  public abstract void setAccessibleNodeLabels(Set<String> labels);
  
  /**
   * Get the <code>default node label expression</code> of the queue, this takes
   * affect only when the <code>ApplicationSubmissionContext</code> and
   * <code>ResourceRequest</code> don't specify their
   * <code>NodeLabelExpression</code>.
   * 
   * @return <code>default node label expression</code> of the queue
   */
  @Public
  @Stable
  public abstract String getDefaultNodeLabelExpression();
  
  @Public
  @Stable
  public abstract void setDefaultNodeLabelExpression(
      String defaultLabelExpression);
}

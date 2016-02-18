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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

class CSQueueUtils {
  
  private static final Log LOG = LogFactory.getLog(CSQueueUtils.class);

  final static float EPSILON = 0.0001f;//用于调整float的减法,使其运算准确
  
  /**
   * 检查第三个参数要在0-1之间,否则抛异常
   */
  public static void checkMaxCapacity(String queueName, 
      float capacity, float maximumCapacity) {
    if (maximumCapacity < 0.0f || maximumCapacity > 1.0f) {
      throw new IllegalArgumentException(
          "Illegal value  of maximumCapacity " + maximumCapacity + 
          " used in call to setMaxCapacity for queue " + queueName);
    }
    }

  /**
   * 校验第三个参数一定比第二个参数大,否则就会有异常
   */
  public static void checkAbsoluteCapacity(String queueName,float absCapacity, float absMaxCapacity) {
    if (absMaxCapacity < (absCapacity - EPSILON)) {
      throw new IllegalArgumentException("Illegal call to setMaxCapacity. "
          + "Queue '" + queueName + "' has " + "an absolute capacity (" + absCapacity
          + ") greater than " + "its absolute maximumCapacity (" + absMaxCapacity
          + ")");
    }
  }
  
  /**
   * 检查每一个label标签对应的资源是否大于最大值,如果大于,则抛异常
   */
  public static void checkAbsoluteCapacitiesByLabel(String queueName,
          Map<String, Float> absCapacities,
          Map<String, Float> absMaximumCapacities) {
    for (Entry<String, Float> entry : absCapacities.entrySet()) {
      String label = entry.getKey();
      float absCapacity = entry.getValue();
      float absMaxCapacity = absMaximumCapacities.get(label);
      if (absMaxCapacity < (absCapacity - EPSILON)) {
        throw new IllegalArgumentException("Illegal call to setMaxCapacity. "
            + "Queue '" + queueName + "' has " + "an absolute capacity ("
            + absCapacity + ") greater than "
            + "its absolute maximumCapacity (" + absMaxCapacity + ") of label="
            + label);
      }
    }
  }

  /**
   * 根据父类的占比,计算自己的最大占比
   */
  public static float computeAbsoluteMaximumCapacity(float maximumCapacity, CSQueue parent) {
    float parentAbsMaxCapacity = (parent == null) ? 1.0f : parent.getAbsoluteMaximumCapacity(); 
    return (parentAbsMaxCapacity * maximumCapacity);
  }
  
  /**
   * 根据父类的占比,计算自己的占比,该占比结果是为每一个label设计的
   */
  public static Map<String, Float> computeAbsoluteCapacityByNodeLabels(Map<String, Float> nodeLabelToCapacities, CSQueue parent) {
    if (parent == null) {//没有父队列,说明占比参数就是最终结果
      return nodeLabelToCapacities;
    }
    
    Map<String, Float> absoluteCapacityByNodeLabels = new HashMap<String, Float>();
    for (Entry<String, Float> entry : nodeLabelToCapacities.entrySet()) {
      String label = entry.getKey();
      float capacity = entry.getValue();
      absoluteCapacityByNodeLabels.put(label,capacity * parent.getAbsoluteCapacityByNodeLabel(label));
    }
    return absoluteCapacityByNodeLabels;
  }
  
  /**
   * 根据父类的占比,计算自己的最大占比,该占比结果是为每一个label设计的
   */
  public static Map<String, Float> computeAbsoluteMaxCapacityByNodeLabels(
      Map<String, Float> maximumNodeLabelToCapacities, CSQueue parent) {
	  
    if (parent == null) {//没有父队列,说明占比参数就是最终结果
      return maximumNodeLabelToCapacities;
    }
    Map<String, Float> absoluteMaxCapacityByNodeLabels = new HashMap<String, Float>();
    for (Entry<String, Float> entry : maximumNodeLabelToCapacities.entrySet()) {
      String label = entry.getKey();
      float maxCapacity = entry.getValue();
      absoluteMaxCapacityByNodeLabels.put(label,maxCapacity * parent.getAbsoluteMaximumCapacityByNodeLabel(label));
    }
    return absoluteMaxCapacityByNodeLabels;
  }

  /**
   * 公式 Math.ceil( (clusterResource/minimumAllocation) * absoluteMaxCapacity * maxAMResourcePercent)
   * 其中
   * clusterResource/minimumAllocation 表示该集群目前运行最小资源可以分配多少个
   * (clusterResource/minimumAllocation) * absoluteMaxCapacity 表示目前该队列运行最小资源可以分配多少个
   */
  public static int computeMaxActiveApplications(
      ResourceCalculator calculator,
      Resource clusterResource, Resource minimumAllocation, 
      float maxAMResourcePercent, float absoluteMaxCapacity) {
    return
        Math.max(
            (int)Math.ceil(
                Resources.ratio(
                    calculator, 
                    clusterResource, 
                    minimumAllocation) * 
                    maxAMResourcePercent * absoluteMaxCapacity
                ), 
            1);
  }

  /**
   * 计算在该队列上每一个用户最多可以有多少个活跃的app
   * 公式:maxActiveApplications * (userLimit / 100.0f) * userLimitFactor
   */
  public static int computeMaxActiveApplicationsPerUser(
      int maxActiveApplications, int userLimit, float userLimitFactor) {
    return Math.max(
        (int)Math.ceil(
            maxActiveApplications * (userLimit / 100.0f) * userLimitFactor),
        1);
  }
  
  /**
   * 更新使用过的资源
   */
  @Lock(CSQueue.class)
  public static void updateQueueStatistics(
      final ResourceCalculator calculator,
      final CSQueue childQueue, final CSQueue parentQueue, 
      final Resource clusterResource, final Resource minimumAllocation) {
    
    Resource queueLimit = Resources.none();//获取该队列应该允许的资源容量
    Resource usedResources = childQueue.getUsedResources();//子队列已经使用的具体资源

    float usedCapacity = 0.0f;//已经使用的资源/最大限制
    float absoluteUsedCapacity = 0.0f;

    if (Resources.greaterThan(calculator, clusterResource, clusterResource, Resources.none())) {
      
      //clusterResource*childQueue.getAbsoluteCapacity(),表示该队列分配的capacity量,即该队列能使用多少资源的限制
      queueLimit = Resources.multiply(clusterResource, childQueue.getAbsoluteCapacity()); //乘法,获取该队列应该允许的资源容量
      
      //usedResources/clusterResource,即该队列已经使用的资源占总资源的比例
      absoluteUsedCapacity = Resources.divide(calculator, clusterResource,usedResources, clusterResource);//除法 计算已经使用的资源比例
      
      /**
       * 即usedResources/(clusterResource*childQueue.getAbsoluteCapacity())
       * 翻译 已经使用的资源量/该队列分配的capacity量,即就是该队列的使用百分比
       */
      usedCapacity = 
          Resources.equals(queueLimit, Resources.none()) ? 0 :
          Resources.divide(calculator, clusterResource, 
              usedResources, queueLimit);
    }

    childQueue.setUsedCapacity(usedCapacity);
    childQueue.setAbsoluteUsedCapacity(absoluteUsedCapacity);
    
    Resource available = Resources.subtract(queueLimit, usedResources);
    childQueue.getMetrics().setAvailableResourcesToQueue(
        Resources.max(
            calculator, 
            clusterResource, 
            available, 
            Resources.none()
            )
        );
   }

  /**
   * 计算该队列可用资源绝对占比
   * @param clusterResource 集群资源
   * @param queue 等待计算的队列
   * @return 返回该队列可用的资源使用量绝对占比
   */
   public static float getAbsoluteMaxAvailCapacity(ResourceCalculator resourceCalculator, Resource clusterResource, CSQueue queue) {
      CSQueue parent = queue.getParent();
      if (parent == null) {
        return queue.getAbsoluteMaximumCapacity();
      }

      //Get my parent's max avail, needed to determine my own 计算父类允许最大使用资源量占比
      float parentMaxAvail = getAbsoluteMaxAvailCapacity(
        resourceCalculator, clusterResource, parent);
      //...and as a resource
      Resource parentResource = Resources.multiply(clusterResource, parentMaxAvail);//计算父类允许最大使用资源量

      //check for no resources parent before dividing, if so, max avail is none
      if (Resources.isInvalidDivisor(resourceCalculator, parentResource)) {//确保parentResource使用资源不是0
        return 0.0f;
      }
      //sibling used is parent used - my used...
      //计算兄弟姐妹使用占比,公式是(父亲已经使用的资源量-自己已经使用的资源量)/父类允许最大使用资源量 = 兄弟已经使用的资源量/父类允许最大使用资源量 = 兄弟使用资源占比
      float siblingUsedCapacity = Resources.ratio(
                 resourceCalculator,
                 Resources.subtract(parent.getUsedResources(), queue.getUsedResources()),
                 parentResource);
      //my max avail is the lesser of my max capacity and what is unused from my parent
      //by my siblings (if they are beyond their base capacity)
      //计算该队列目前允许使用的最大占比,可以是配置文件配置的最大占比,也可能是该节点剩余的最大占比
      float maxAvail = Math.min(
        queue.getMaximumCapacity(),
        1.0f - siblingUsedCapacity);
      //and, mutiply by parent to get absolute (cluster relative) value
      float absoluteMaxAvail = maxAvail * parentMaxAvail;//本队列的最终最大占比maxAvail * 父类允许最大使用资源量占比 = 绝对值占比

      if (LOG.isDebugEnabled()) {
        LOG.debug("qpath " + queue.getQueuePath());
        LOG.debug("parentMaxAvail " + parentMaxAvail);
        LOG.debug("siblingUsedCapacity " + siblingUsedCapacity);
        LOG.debug("getAbsoluteMaximumCapacity " + queue.getAbsoluteMaximumCapacity());
        LOG.debug("maxAvail " + maxAvail);
        LOG.debug("absoluteMaxAvail " + absoluteMaxAvail);
      }

      //格式化最终占比
      if (absoluteMaxAvail < 0.0f) {
        absoluteMaxAvail = 0.0f;
      } else if (absoluteMaxAvail > 1.0f) {
        absoluteMaxAvail = 1.0f;
      }

      return absoluteMaxAvail;
   }
}

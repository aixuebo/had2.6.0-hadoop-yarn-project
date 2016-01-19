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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

public class FiCaSchedulerNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerNode.class);

  public FiCaSchedulerNode(RMNode node, boolean usePortForNodeName) {
    super(node, usePortForNodeName);
  }

  /**
   * 申请预留资源
   * 为任务的某一个优先级预留一个容器
   */
  @Override
  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, Priority priority,
      RMContainer container) {
    // Check if it's already reserved
    RMContainer reservedContainer = getReservedContainer();
    if (reservedContainer != null) {//如果该节点的域名容器位置不是null,说明已经被占用
      // Sanity check
      if (!container.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container +
            " on node " + container.getReservedNode() + 
            " when currently" + " reserved resource " + reservedContainer +
            " on node " + reservedContainer.getReservedNode());
      }
      
      // Cannot reserve more than one application attempt on a given node!
      // Reservation is still against attempt.一个node节点必须只能为一个任务留容器
      if (!reservedContainer.getContainer().getId().getApplicationAttemptId()
          .equals(container.getContainer().getId().getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container + 
            " for application " + application.getApplicationAttemptId() + 
            " when currently" +
            " reserved container " + reservedContainer +
            " on node " + this);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated reserved container "
            + container.getContainer().getId() + " on node " + this
            + " for application attempt "
            + application.getApplicationAttemptId());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reserved container "
            + container.getContainer().getId() + " on node " + this
            + " for application attempt "
            + application.getApplicationAttemptId());
      }
    }
    //真正的分配预留容器
    setReservedContainer(container);
  }

  /**
   * 取消预留资源
   */
  @Override
  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application) {

    // adding NP checks as this can now be called for preemption
    if (getReservedContainer() != null
        && getReservedContainer().getContainer() != null
        && getReservedContainer().getContainer().getId() != null
        && getReservedContainer().getContainer().getId()
          .getApplicationAttemptId() != null) {

      // Cannot unreserve for wrong application...
      ApplicationAttemptId reservedApplication =
          getReservedContainer().getContainer().getId()
            .getApplicationAttemptId();
      if (!reservedApplication.equals(
          application.getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to unreserve " +
            " for application " + application.getApplicationAttemptId() +
            " when currently reserved " +
            " for application " + reservedApplication.getApplicationId() +
            " on node " + this);
      }
    }
    setReservedContainer(null);
  }
}

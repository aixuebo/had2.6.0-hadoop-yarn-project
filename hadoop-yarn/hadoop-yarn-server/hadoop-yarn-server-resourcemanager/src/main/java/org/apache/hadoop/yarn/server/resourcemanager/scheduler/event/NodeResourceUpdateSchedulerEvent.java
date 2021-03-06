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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * 一个节点重新上报他的总资源数量
 * 因此要更改集群资源数量
 * 即集群中先减去老的资源，再加上新的资源
 */
public class NodeResourceUpdateSchedulerEvent extends SchedulerEvent {

  private final RMNode rmNode;
  private final ResourceOption resourceOption;
  
  public NodeResourceUpdateSchedulerEvent(RMNode rmNode,
      ResourceOption resourceOption) {
    super(SchedulerEventType.NODE_RESOURCE_UPDATE);
    this.rmNode = rmNode;
    this.resourceOption = resourceOption;
  }

  public RMNode getRMNode() {
    return rmNode;
  }

  public ResourceOption getResourceOption() {
    return resourceOption;
  }

}

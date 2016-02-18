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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;

//表示最终分配多少资源去在一个容器中执行一个任务,以及任务Node选择的节点类型
@Private
@Unstable
public class CSAssignment {
  final private Resource resource;//该任务要分配多少资源去执行
  private NodeType type;//节点类型
  private final RMContainer excessReservation;//执行的容器
  private final FiCaSchedulerApp application;//容器执行的任务,代表一个任务,即代表一个ApplicationAttempt
  private final boolean skipped;
  
  public CSAssignment(Resource resource, NodeType type) {
    this.resource = resource;
    this.type = type;
    this.application = null;
    this.excessReservation = null;
    this.skipped = false;
  }
  
  public CSAssignment(FiCaSchedulerApp application, RMContainer excessReservation) {
    this.resource = excessReservation.getContainer().getResource();
    this.type = NodeType.NODE_LOCAL;
    this.application = application;
    this.excessReservation = excessReservation;
    this.skipped = false;
  }
  
  public CSAssignment(boolean skipped) {
    this.resource = Resources.createResource(0, 0);
    this.type = NodeType.NODE_LOCAL;
    this.application = null;
    this.excessReservation = null;
    this.skipped = skipped;
  }

  public Resource getResource() {
    return resource;
  }

  public NodeType getType() {
    return type;
  }
  
  public void setType(NodeType type) {
    this.type = type;
  }
  
  public FiCaSchedulerApp getApplication() {
    return application;
  }

  public RMContainer getExcessReservation() {
    return excessReservation;
  }

  public boolean getSkipped() {
    return skipped;
  }
  
  @Override
  public String toString() {
    return resource.getMemory() + ":" + type;
  }
}
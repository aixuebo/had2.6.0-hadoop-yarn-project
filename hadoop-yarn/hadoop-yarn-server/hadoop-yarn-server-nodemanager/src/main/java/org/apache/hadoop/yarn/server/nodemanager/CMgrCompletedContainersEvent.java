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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 一组容器完成,需要清理容器
 */
public class CMgrCompletedContainersEvent extends ContainerManagerEvent {

  private final List<ContainerId> containerToCleanup;//需要清理的容器
  private final Reason reason;

  public CMgrCompletedContainersEvent(List<ContainerId> containersToCleanup,Reason reason) {
    super(ContainerManagerEventType.FINISH_CONTAINERS);
    this.containerToCleanup = containersToCleanup;
    this.reason = reason;
  }

  public List<ContainerId> getContainersToCleanup() {
    return this.containerToCleanup;
  }

  public Reason getReason() {
    return reason;
  }

  public static enum Reason {
    /**
     * Container is killed as NodeManager is shutting down
     */
    ON_SHUTDOWN,

    /**
     * Container is killed as the Nodemanager is re-syncing with the
     * ResourceManager
     */
    ON_NODEMANAGER_RESYNC,

    /**
     * Container is killed on request by the ResourceManager
     * ResourceManager发布要清理这些容器
     */
    BY_RESOURCEMANAGER
  }

}

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
package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;

public interface SchedulingEditPolicy {

  /**
   * 初始化方法
   */
  public void init(Configuration config,
      EventHandler<ContainerPreemptEvent> dispatcher,
      PreemptableResourceScheduler scheduler);

  /**
   * 该方法每个一定时间就执行一次
   * This method is invoked at regular intervals. Internally the policy is
   * allowed to track containers and affect the scheduler. The "actions"
   * performed are passed back through an EventHandler.
   * 每次调度器执行都执行该方法
   */
  public void editSchedule();

  /**
   * 获取调度器的执行时间间隔
   */
  public long getMonitoringInterval();

  /**
   * 名称
   */
  public String getPolicyName();

}

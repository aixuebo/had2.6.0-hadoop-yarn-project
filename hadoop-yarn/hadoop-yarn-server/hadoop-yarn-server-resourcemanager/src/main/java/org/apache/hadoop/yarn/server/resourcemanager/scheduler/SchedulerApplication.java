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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

/**
 * 代表一个调度的应用,一个app对应一个该对象
 */
@Private
@Unstable
public class SchedulerApplication<T extends SchedulerApplicationAttempt> {

  private Queue queue;//该尝试任务对应的队列
  private final String user;//该尝试任务对应的提交者
  private T currentAttempt;//当前调度的app尝试任务

  public SchedulerApplication(Queue queue, String user) {
    this.queue = queue;
    this.user = user;
  }

  public Queue getQueue() {
    return queue;
  }
  
  public void setQueue(Queue queue) {
    this.queue = queue;
  }

  public String getUser() {
    return user;
  }

  public T getCurrentAppAttempt() {
    return currentAttempt;
  }

  public void setCurrentAppAttempt(T currentAttempt) {
    this.currentAttempt = currentAttempt;
  }

  public void stop(RMAppState rmAppFinalState) {
    queue.getMetrics().finishApp(user, rmAppFinalState);
  }

}

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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 应用日志聚合
 */
public interface AppLogAggregator extends Runnable {

  /**
   * 当收到CONTAINER_FINISHED, //容器完成事件时,调用该方法,
   * 参数wasContainerSuccessful如果为true表示容器正常成功完成的
   */
  void startContainerLogAggregation(ContainerId containerId,
      boolean wasContainerSuccessful);

  //当一个应用中途终止了,则调用该方法
  void abortLogAggregation();

  /**
   * 一个应用全部完成的时候调用该方法
   */
  void finishLogAggregation();
}

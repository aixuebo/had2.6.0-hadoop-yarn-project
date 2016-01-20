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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;

/**
 * The class which provides functionality of checking the health of the node and
 * reporting back to the service for which the health checker has been asked to
 * report.
 * 从脚本和磁盘两个角度去检查是否健康
 * 脚本是自己写的，磁盘是程序默认的
 * 
 * Node节点的健康检查服务,是一个组合服务,提供检查磁盘的服务以及可能提供脚本检查的服务
 */
public class NodeHealthCheckerService extends CompositeService {

  private NodeHealthScriptRunner nodeHealthScriptRunner;//脚本检查
  private LocalDirsHandlerService dirsHandler;

  static final String SEPARATOR = ";";

  public NodeHealthCheckerService() {
    super(NodeHealthCheckerService.class.getName());
    dirsHandler = new LocalDirsHandlerService();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (NodeHealthScriptRunner.shouldRun(conf)) {//说明有脚本文件,则启动脚本文件
      nodeHealthScriptRunner = new NodeHealthScriptRunner();
      addService(nodeHealthScriptRunner);
    }
    addService(dirsHandler);
    super.serviceInit(conf);
  }

  /**
   * @return the reporting string of health of the node
   * 打印健康状况信息
   */
  String getHealthReport() {
    String scriptReport = (nodeHealthScriptRunner == null) ? ""
        : nodeHealthScriptRunner.getHealthReport();
    if (scriptReport.equals("")) {//说明健康,没有输出信息
      return dirsHandler.getDisksHealthReport(false);
    } else {//说明不健康
      return scriptReport.concat(SEPARATOR + dirsHandler.getDisksHealthReport(false));
    }
  }

  /**
   * @return <em>true</em> if the node is healthy
   * 是否健康 
   */
  boolean isHealthy() {
    boolean scriptHealthStatus = (nodeHealthScriptRunner == null) ? true
        : nodeHealthScriptRunner.isHealthy();
    return scriptHealthStatus && dirsHandler.areDisksHealthy();
  }

  /**
   * @return when the last time the node health status is reported
   * 获取最后一次检查时间
   */
  long getLastHealthReportTime() {
    long diskCheckTime = dirsHandler.getLastDisksCheckTime();
    long lastReportTime = (nodeHealthScriptRunner == null)
        ? diskCheckTime
        : Math.max(nodeHealthScriptRunner.getLastReportedTime(), diskCheckTime);
    return lastReportTime;
  }

  /**
   * @return the disk handler
   * 返回磁盘检查服务
   */
  public LocalDirsHandlerService getDiskHandler() {
    return dirsHandler;
  }

  /**
   * @return the node health script runner
   * 返回脚本检查服务
   */
  NodeHealthScriptRunner getNodeHealthScriptRunner() {
    return nodeHealthScriptRunner;
  }
}

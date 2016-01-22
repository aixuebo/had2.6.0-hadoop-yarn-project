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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event;

import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;

/**
 * 日志记录系统
 * ApplicationImpl.AppInitTransition方法调用该事件
 */
public class LogHandlerAppStartedEvent extends LogHandlerEvent {

  private final ApplicationId applicationId;//记录日志的应用
  private final ContainerLogsRetentionPolicy retentionPolicy;//记录什么级别的日志
  private final String user;//所属user
  private final Credentials credentials;//权限
  private final Map<ApplicationAccessType, String> appAcls;//权限
  private final LogAggregationContext logAggregationContext;//日志系统上下文

  public LogHandlerAppStartedEvent(ApplicationId appId, String user,
      Credentials credentials, ContainerLogsRetentionPolicy retentionPolicy,
      Map<ApplicationAccessType, String> appAcls) {
    this(appId, user, credentials, retentionPolicy, appAcls, null);
  }

  public LogHandlerAppStartedEvent(ApplicationId appId, String user,
      Credentials credentials, ContainerLogsRetentionPolicy retentionPolicy,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext) {
    super(LogHandlerEventType.APPLICATION_STARTED);
    this.applicationId = appId;
    this.user = user;
    this.credentials = credentials;
    this.retentionPolicy = retentionPolicy;
    this.appAcls = appAcls;
    this.logAggregationContext = logAggregationContext;
  }

  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  public Credentials getCredentials() {
    return this.credentials;
  }

  public ContainerLogsRetentionPolicy getLogRetentionPolicy() {
    return this.retentionPolicy;
  }

  public String getUser() {
    return this.user;
  }

  public Map<ApplicationAccessType, String> getApplicationAcls() {
    return this.appAcls;
  }

  public LogAggregationContext getLogAggregationContext() {
    return this.logAggregationContext;
  }
}
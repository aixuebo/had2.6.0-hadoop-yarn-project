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

package org.apache.hadoop.yarn.api.records;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>ApplicationSubmissionContext</code> represents all of the
 * information needed by the <code>ResourceManager</code> to launch 
 * the <code>ApplicationMaster</code> for an application.</p>
 * 该对象表示ResourceManager去为一个应用启动一个ApplicationMaster时候,需要的全部信息
 * 
 * 包含以下信息:
 * 1.appId
 * 2.app的提交者user
 * 3.app的name名称
 * 4.app执行的优先级
 * 5.ContainerLaunchContext,ApplicationMaster中要去执行如何启动容器的上下文
 * 6.maxAppAttempts,表示最多允许该app创建多少个尝试任务
 *   该值不会大于yarn全局的最大值。
 * 7.attemptFailuresValidityInterval
 * 8.application-specific,例如LogAggregationContext
 * 
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ApplicationId} of the application.</li>
 *     <li>Application user.</li>
 *     <li>Application name.</li>
 *     <li>{@link Priority} of the application.</li>
 *     <li>
 *       {@link ContainerLaunchContext} of the container in which the 
 *       <code>ApplicationMaster</code> is executed.
 *     </li>
 *     <li>maxAppAttempts. The maximum number of application attempts.
 *     It should be no larger than the global number of max attempts in the
 *     Yarn configuration.
 *     </li>
 *     <li>attemptFailuresValidityInterval. The default value is -1.
 *     when attemptFailuresValidityInterval in milliseconds is set to > 0,
 *     the failure number will no take failures which happen out of the
 *     validityInterval into failure count. If failure count reaches to
 *     maxAppAttempts, the application will be failed.
 *     </li>
 *   <li>Optional, application-specific {@link LogAggregationContext}</li>
 *   </ul>
 * </p>
 * 
 * @see ContainerLaunchContext
 * @see ApplicationClientProtocol#submitApplication(org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest)
 * 应用提交的上下文，基本上都是job设置的参数
 */
@Public
@Stable
public abstract class ApplicationSubmissionContext {

  /**
   * ContainerLaunchContext 每一个应用在提交的时候会设置该内容,表示容器启动的参数
   */
  @Public
  @Stable
  public static ApplicationSubmissionContext newInstance(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource, String applicationType,
      boolean keepContainers, String appLabelExpression,
      String amContainerLabelExpression) {
    ApplicationSubmissionContext context =
        Records.newRecord(ApplicationSubmissionContext.class);
    context.setApplicationId(applicationId);//appId
    context.setApplicationName(applicationName);//名称
    context.setQueue(queue);
    context.setPriority(priority);
    context.setAMContainerSpec(amContainer);//容器在app任务上任意一个任务需要的环境
    context.setUnmanagedAM(isUnmanagedAM);
    context.setCancelTokensWhenComplete(cancelTokensWhenComplete);
    context.setMaxAppAttempts(maxAppAttempts);
    context.setApplicationType(applicationType);
    context.setKeepContainersAcrossApplicationAttempts(keepContainers);
    context.setNodeLabelExpression(appLabelExpression);
    context.setResource(resource);
    
    //为AM本身添加有一个容器,去执行AM
    ResourceRequest amReq = Records.newRecord(ResourceRequest.class);
    amReq.setResourceName(ResourceRequest.ANY);//表示任何节点都可以执行该AM
    amReq.setCapability(resource);//AM的资源与容器的资源相同即可
    amReq.setNumContainers(1);//1个容器即可
    amReq.setRelaxLocality(true);
    amReq.setNodeLabelExpression(amContainerLabelExpression);
    context.setAMContainerResourceRequest(amReq);//am本身需要的资源
    return context;
  }
  
  public static ApplicationSubmissionContext newInstance(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource, String applicationType,
      boolean keepContainers) {
    return newInstance(applicationId, applicationName, queue, priority,
        amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts,
        resource, applicationType, keepContainers, null, null);
  }

  @Public
  @Stable
  public static ApplicationSubmissionContext newInstance(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource, String applicationType) {
    return newInstance(applicationId, applicationName, queue, priority,
      amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts,
      resource, applicationType, false, null, null);
  }

  @Public
  @Stable
  public static ApplicationSubmissionContext newInstance(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource) {
    return newInstance(applicationId, applicationName, queue, priority,
      amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts,
      resource, null);
  }
  
  @Public
  @Stable
  public static ApplicationSubmissionContext newInstance(
      ApplicationId applicationId, String applicationName, String queue,
      ContainerLaunchContext amContainer, boolean isUnmanagedAM,
      boolean cancelTokensWhenComplete, int maxAppAttempts,
      String applicationType, boolean keepContainers,
      String appLabelExpression, ResourceRequest resourceRequest) {
    ApplicationSubmissionContext context =
        Records.newRecord(ApplicationSubmissionContext.class);
    context.setApplicationId(applicationId);
    context.setApplicationName(applicationName);
    context.setQueue(queue);
    context.setAMContainerSpec(amContainer);
    context.setUnmanagedAM(isUnmanagedAM);
    context.setCancelTokensWhenComplete(cancelTokensWhenComplete);
    context.setMaxAppAttempts(maxAppAttempts);
    context.setApplicationType(applicationType);
    context.setKeepContainersAcrossApplicationAttempts(keepContainers);
    context.setAMContainerResourceRequest(resourceRequest);
    return context;
  }

  @Public
  @Stable
  public static ApplicationSubmissionContext newInstance(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource, String applicationType,
      boolean keepContainers, long attemptFailuresValidityInterval) {
    ApplicationSubmissionContext context =
        newInstance(applicationId, applicationName, queue, priority,
          amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts,
          resource, applicationType, keepContainers);
    context.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
    return context;
  }

  @Public
  @Stable
  public static ApplicationSubmissionContext newInstance(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource, String applicationType,
      boolean keepContainers, LogAggregationContext logAggregationContext) {
    ApplicationSubmissionContext context =
        newInstance(applicationId, applicationName, queue, priority,
          amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts,
          resource, applicationType, keepContainers);
    context.setLogAggregationContext(logAggregationContext);
    return context;
  }
  /**
   * Get the <code>ApplicationId</code> of the submitted application.
   * @return <code>ApplicationId</code> of the submitted application
   */
  @Public
  @Stable
  public abstract ApplicationId getApplicationId();
  
  /**
   * Set the <code>ApplicationId</code> of the submitted application.
   * @param applicationId <code>ApplicationId</code> of the submitted
   *                      application
   */
  @Public
  @Stable
  public abstract void setApplicationId(ApplicationId applicationId);

  /**
   * Get the application <em>name</em>.
   * @return application name
   */
  @Public
  @Stable
  public abstract String getApplicationName();
  
  /**
   * Set the application <em>name</em>.
   * @param applicationName application name
   */
  @Public
  @Stable
  public abstract void setApplicationName(String applicationName);
  
  /**
   * Get the <em>queue</em> to which the application is being submitted.
   * @return <em>queue</em> to which the application is being submitted
   */
  @Public
  @Stable
  public abstract String getQueue();
  
  /**
   * Set the <em>queue</em> to which the application is being submitted
   * @param queue <em>queue</em> to which the application is being submitted
   */
  @Public
  @Stable
  public abstract void setQueue(String queue);
  
  /**
   * Get the <code>Priority</code> of the application.
   * @return <code>Priority</code> of the application
   */
  @Public
  @Stable
  public abstract Priority getPriority();

  /**
   * Set the <code>Priority</code> of the application.
   * @param priority <code>Priority</code> of the application
   */
  @Private
  @Unstable
  public abstract void setPriority(Priority priority);

  /**
   * Get the <code>ContainerLaunchContext</code> to describe the 
   * <code>Container</code> with which the <code>ApplicationMaster</code> is
   * launched.
   * @return <code>ContainerLaunchContext</code> for the 
   *         <code>ApplicationMaster</code> container
   */
  @Public
  @Stable
  public abstract ContainerLaunchContext getAMContainerSpec();
  
  /**
   * Set the <code>ContainerLaunchContext</code> to describe the 
   * <code>Container</code> with which the <code>ApplicationMaster</code> is
   * launched.
   * @param amContainer <code>ContainerLaunchContext</code> for the 
   *                    <code>ApplicationMaster</code> container
   */
  @Public
  @Stable
  public abstract void setAMContainerSpec(ContainerLaunchContext amContainer);
  
  /**
   * Get if the RM should manage the execution of the AM. 
   * If true, then the RM 
   * will not allocate a container for the AM and start it. It will expect the 
   * AM to be launched and connect to the RM within the AM liveliness period and 
   * fail the app otherwise. The client should launch the AM only after the RM 
   * has ACCEPTED the application and changed the <code>YarnApplicationState</code>.
   * Such apps will not be retried by the RM on app attempt failure.
   * The default value is false.
   * @return true if the AM is not managed by the RM
   * 
   * 在YARN中，一个ApplicationMaster需要占用一个container，该container可能位于任意一个NodeManager上，
   * 这给ApplicationMaster测试带来很大麻烦，为了解决该问题，YARN引入了一种新的ApplicationMaster—Unmanaged AM（具体参考：MAPREDUCE-4427），
   * 这种AM运行在客户端，不再由ResourceManager启动和销毁。用户只需稍微修改一下客户端即可将分布式环境下的AM运行在客户端的一个单独进程中。
   * 
   * 即true表示RM不在管理AM了,AM在客户端的进程上运行
   */
  @Public
  @Stable
  public abstract boolean getUnmanagedAM();
  
  /**
   * @param value true if RM should not manage the AM
   * true表示RM不在管理AM了,AM在客户端的进程上运行
   */
  @Public
  @Stable
  public abstract void setUnmanagedAM(boolean value);

  /**
   * @return true if tokens should be canceled when the app completes.
   */
  @LimitedPrivate("mapreduce")
  @Unstable
  public abstract boolean getCancelTokensWhenComplete();
  
  /**
   * Set to false if tokens should not be canceled when the app finished else
   * false.  WARNING: this is not recommended unless you want your single job
   * tokens to be reused by others jobs.
   * @param cancel true if tokens should be canceled when the app finishes. 
   */
  @LimitedPrivate("mapreduce")
  @Unstable
  public abstract void setCancelTokensWhenComplete(boolean cancel);

  /**
   * @return the number of max attempts of the application to be submitted
   */
  @Public
  @Stable
  public abstract int getMaxAppAttempts();

  /**
   * Set the number of max attempts of the application to be submitted. WARNING:
   * it should be no larger than the global number of max attempts in the Yarn
   * configuration.
   * @param maxAppAttempts the number of max attempts of the application
   * to be submitted.
   */
  @Public
  @Stable
  public abstract void setMaxAppAttempts(int maxAppAttempts);

  /**
   * Get the resource required by the <code>ApplicationMaster</code> for this
   * application. Please note this will be DEPRECATED, use <em>getResource</em>
   * in <em>getAMContainerResourceRequest</em> instead.
   * 
   * @return the resource required by the <code>ApplicationMaster</code> for
   *         this application.
   */
  @Public
  public abstract Resource getResource();

  /**
   * Set the resource required by the <code>ApplicationMaster</code> for this
   * application.
   *
   * @param resource the resource required by the <code>ApplicationMaster</code>
   * for this application.
   */
  @Public
  public abstract void setResource(Resource resource);
  
  /**
   * Get the application type
   * 
   * @return the application type
   */
  @Public
  @Stable
  public abstract String getApplicationType();

  /**
   * Set the application type
   * 
   * @param applicationType the application type
   */
  @Public
  @Stable
  public abstract void setApplicationType(String applicationType);

  /**
   * Get the flag which indicates whether to keep containers across application
   * attempts or not.
   * 
   * @return the flag which indicates whether to keep containers across
   *         application attempts or not.
   */
  @Public
  @Stable
  public abstract boolean getKeepContainersAcrossApplicationAttempts();

  /**
   * Set the flag which indicates whether to keep containers across application
   * attempts.
   * <p>
   * If the flag is true, running containers will not be killed when application
   * attempt fails and these containers will be retrieved by the new application
   * attempt on registration via
   * {@link ApplicationMasterProtocol#registerApplicationMaster(RegisterApplicationMasterRequest)}.
   * </p>
   * 
   * @param keepContainers
   *          the flag which indicates whether to keep containers across
   *          application attempts.
   */
  @Public
  @Stable
  public abstract void setKeepContainersAcrossApplicationAttempts(
      boolean keepContainers);

  /**
   * Get tags for the application
   *
   * @return the application tags
   */
  @Public
  @Stable
  public abstract Set<String> getApplicationTags();

  /**
   * Set tags for the application. A maximum of
   * {@link YarnConfiguration#APPLICATION_MAX_TAGS} are allowed
   * per application. Each tag can be at most
   * {@link YarnConfiguration#APPLICATION_MAX_TAG_LENGTH}
   * characters, and can contain only ASCII characters.
   *
   * @param tags tags to set
   */
  @Public
  @Stable
  public abstract void setApplicationTags(Set<String> tags);
  
  /**
   * Get node-label-expression for this app. If this is set, all containers of
   * this application without setting node-label-expression in ResurceRequest
   * will get allocated resources on only those nodes that satisfy this
   * node-label-expression.
   * 
   * If different node-label-expression of this app and ResourceRequest are set
   * at the same time, the one set in ResourceRequest will be used when
   * allocating container
   * 
   * @return node-label-expression for this app
   */
  @Public
  @Evolving
  public abstract String getNodeLabelExpression();
  
  /**
   * Set node-label-expression for this app
   * @param nodeLabelExpression node-label-expression of this app
   */
  @Public
  @Evolving
  public abstract void setNodeLabelExpression(String nodeLabelExpression);
  
  /**
   * Get ResourceRequest of AM container, if this is not null, scheduler will
   * use this to acquire resource for AM container.
   * 
   * If this is null, scheduler will assemble a ResourceRequest by using
   * <em>getResource</em> and <em>getPriority</em> of
   * <em>ApplicationSubmissionContext</em>.
   * 
   * Number of containers and Priority will be ignore.
   * 
   * @return ResourceRequest of AM container
   */
  @Public
  @Evolving
  public abstract ResourceRequest getAMContainerResourceRequest();
  
  /**
   * Set ResourceRequest of AM container
   * @param request of AM container
   */
  @Public
  @Evolving
  public abstract void setAMContainerResourceRequest(ResourceRequest request);

  /**
   * Get the attemptFailuresValidityInterval in milliseconds for the application
   *
   * @return the attemptFailuresValidityInterval
   */
  @Public
  @Stable
  public abstract long getAttemptFailuresValidityInterval();

  /**
   * Set the attemptFailuresValidityInterval in milliseconds for the application
   * @param attemptFailuresValidityInterval
   */
  @Public
  @Stable
  public abstract void setAttemptFailuresValidityInterval(
      long attemptFailuresValidityInterval);

  /**
   * Get <code>LogAggregationContext</code> of the application
   *
   * @return <code>LogAggregationContext</code> of the application
   */
  @Public
  @Stable
  public abstract LogAggregationContext getLogAggregationContext();

  /**
   * Set <code>LogAggregationContext</code> for the application
   *
   * @param logAggregationContext
   *          for the application
   */
  @Public
  @Stable
  public abstract void setLogAggregationContext(
      LogAggregationContext logAggregationContext);

  /**
   * Get the reservation id, that corresponds to a valid resource allocation in
   * the scheduler (between start and end time of the corresponding reservation)
   * 
   * @return the reservation id representing the unique id of the corresponding
   *         reserved resource allocation in the scheduler
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationID();

  /**
   * Set the reservation id, that correspond to a valid resource allocation in
   * the scheduler (between start and end time of the corresponding reservation)
   * 
   * @param reservationID representing the unique id of the
   *          corresponding reserved resource allocation in the scheduler
   */
  @Public
  @Unstable
  public abstract void setReservationID(ReservationId reservationID);
}

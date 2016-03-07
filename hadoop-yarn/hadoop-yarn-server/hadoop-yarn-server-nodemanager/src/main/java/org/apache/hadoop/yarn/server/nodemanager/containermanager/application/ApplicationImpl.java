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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * The state machine for the representation of an Application
 * within the NodeManager.
 * 该类表示在该节点上处理的应用对象,以及处理应用的一些事件
 * 
 * 
App流程ApplicationEventType
1.INIT_APPLICATION 进行app的初始化工作
a.添加该app的权限信息
b.发送LogHandlerEventType.APPLICATION_STARTED
2.APPLICATION_LOG_HANDLING_INITED,//应用的日志初始化完成
a.发送LocalizationEventType.INIT_APPLICATION_RESOURCES,初始化该应用资源信息
3.APPLICATION_INITED 表示已经app初始化结束了,开始执行容器了
a.ContainerEventType.INIT_CONTAINER 循环所有该app的容器,进行容器初始化
4.APPLICATION_CONTAINER_FINISHED
 容器完成的时候触发该函数
a.app会删除该容器的映射关系
5.FINISH_APPLICATION 完成该app
 resourceManager发来的信息,表示该应用完成,该完成可能是正常的完成,也可以应用异常导致的完成
a.如果此时该app内的容器集合为空,说明没有容器了,因此执行该app的清理工作,返回ApplicationState.APPLICATION_RESOURCES_CLEANINGUP状态
发送LocalizationEventType.DESTROY_APPLICATION_RESOURCES事件,清理该app的资源
发送AuxServicesEventType.APPLICATION_STOP事件,说明该app已经结束了,第三方扩展包也要结束
b.如果此时该app的容器集合有内容,则对每一个容器进行kill处理,返回ApplicationState.FINISHING_CONTAINERS_WAIT状态
 执行ContainerEventType.KILL_CONTAINER事件
6.APPLICATION_CONTAINER_FINISHED,说明该应用的容器也完成了
在ApplicationState.FINISHING_CONTAINERS_WAIT状态下,可以转换成ApplicationState.FINISHING_CONTAINERS_WAIT或者ApplicationState.APPLICATION_RESOURCES_CLEANINGUP状态
a.从该app的容器集合中删除这个完成的容器
b.如果容器集合依然不是空,则还是返回ApplicationState.FINISHING_CONTAINERS_WAIT状态,说明有容器正在被杀死过程中
c.如果容器集合已经是空了,则返回ApplicationState.APPLICATION_RESOURCES_CLEANINGUP状态
发送LocalizationEventType.DESTROY_APPLICATION_RESOURCES事件,清理该app的资源
发送AuxServicesEventType.APPLICATION_STOP事件,说明该app已经结束了,第三方扩展包也要结束
7.APPLICATION_RESOURCES_CLEANEDUP 清理该app下的所有资源
ApplicationState.APPLICATION_RESOURCES_CLEANINGUP状态下,遇到该事件,则进行如下处理
a.发送LogHandlerEventType.APPLICATION_FINISHED事件,清理该app下的日志资源
8.APPLICATION_LOG_HANDLING_FINISHED 该app资源下的日志资源已经清理完成
a.该app从context上下文中彻底移除
b.移除该app的权限西西
c.删除该app的所有记录,即因此该app彻底结束
 */
public class ApplicationImpl implements Application {

  final Dispatcher dispatcher;
  final String user;//该应用所属user
  final ApplicationId appId;//该应用ID
  final Credentials credentials;
  Map<ApplicationAccessType, String> applicationACLs;//访问权限
  final ApplicationACLsManager aclsManager;//访问权限
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Context context;//nodemanager上下文对象

  private static final Log LOG = LogFactory.getLog(Application.class);

  private LogAggregationContext logAggregationContext;
  
  //应用启动的容器,即被resourceManager分配到该节点的容器
  Map<ContainerId, Container> containers = new HashMap<ContainerId, Container>();

  public ApplicationImpl(Dispatcher dispatcher, String user, ApplicationId appId,Credentials credentials, Context context) {
    this.dispatcher = dispatcher;
    this.user = user;
    this.appId = appId;
    this.credentials = credentials;
    this.aclsManager = context.getApplicationACLsManager();
    this.context = context;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public String getUser() {
    return user.toString();
  }

  @Override
  public ApplicationId getAppId() {
    return appId;
  }

  @Override
  public ApplicationState getApplicationState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<ContainerId, Container> getContainers() {
    this.readLock.lock();
    try {
      return this.containers;
    } finally {
      this.readLock.unlock();
    }
  }

  private static final ContainerDoneTransition CONTAINER_DONE_TRANSITION = new ContainerDoneTransition();

  /**
   * 参见hadoop根目录下面的hadoop2.6.0事件转换.xlsx
   */
  private static StateMachineFactory<ApplicationImpl, ApplicationState,
          ApplicationEventType, ApplicationEvent> stateMachineFactory =
      new StateMachineFactory<ApplicationImpl, ApplicationState,
          ApplicationEventType, ApplicationEvent>(ApplicationState.NEW)

           // Transitions from NEW state
           .addTransition(ApplicationState.NEW, ApplicationState.INITING,
               ApplicationEventType.INIT_APPLICATION, new AppInitTransition())
           .addTransition(ApplicationState.NEW, ApplicationState.NEW,
               ApplicationEventType.INIT_CONTAINER,
               new InitContainerTransition())

           // Transitions from INITING state
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.INIT_CONTAINER,
               new InitContainerTransition())
           .addTransition(ApplicationState.INITING,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.FINISH_APPLICATION,
               new AppFinishTriggeredTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               CONTAINER_DONE_TRANSITION)
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
               new AppLogInitDoneTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
               new AppLogInitFailTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.RUNNING,
               ApplicationEventType.APPLICATION_INITED,
               new AppInitDoneTransition())

           // Transitions from RUNNING state
           .addTransition(ApplicationState.RUNNING,
               ApplicationState.RUNNING,
               ApplicationEventType.INIT_CONTAINER,
               new InitContainerTransition())
           .addTransition(ApplicationState.RUNNING,
               ApplicationState.RUNNING,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               CONTAINER_DONE_TRANSITION)
           .addTransition(
               ApplicationState.RUNNING,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.FINISH_APPLICATION,
               new AppFinishTriggeredTransition())

           // Transitions from FINISHING_CONTAINERS_WAIT state.
           .addTransition(
               ApplicationState.FINISHING_CONTAINERS_WAIT,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               new AppFinishTransition())
          .addTransition(ApplicationState.FINISHING_CONTAINERS_WAIT,
              ApplicationState.FINISHING_CONTAINERS_WAIT,
              EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
                  ApplicationEventType.APPLICATION_INITED,
                  ApplicationEventType.FINISH_APPLICATION))

           // Transitions from APPLICATION_RESOURCES_CLEANINGUP state
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED)
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.FINISHED,
               ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP,
               new AppCompletelyDoneTransition())
          .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED,
                  ApplicationEventType.APPLICATION_INITED,
                  ApplicationEventType.FINISH_APPLICATION))
           
           // Transitions from FINISHED state
           .addTransition(ApplicationState.FINISHED,
               ApplicationState.FINISHED,
               ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED,
               new AppLogsAggregatedTransition())
           .addTransition(ApplicationState.FINISHED, ApplicationState.FINISHED,
               EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
                  ApplicationEventType.FINISH_APPLICATION))
               
           // create the topology tables
           .installTopology();

  private final StateMachine<ApplicationState, ApplicationEventType, ApplicationEvent> stateMachine;

  /**
   * Notify services of new application.
   * 
   * In particular, this initializes the {@link LogAggregationService}
   * 初始化一些应用需要的属性,比如权限,启动日志服务等初始化过程,然后调用日志系统,通知该应用已经开始运行
   */
  @SuppressWarnings("unchecked")
  static class AppInitTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent)event;
      app.applicationACLs = initEvent.getApplicationACLs();
      app.aclsManager.addApplication(app.getAppId(), app.applicationACLs);
      // Inform the logAggregator
      app.logAggregationContext = initEvent.getLogAggregationContext();
      app.dispatcher.getEventHandler().handle(
          new LogHandlerAppStartedEvent(app.appId, app.user,
              app.credentials, ContainerLogsRetentionPolicy.ALL_CONTAINERS,
              app.applicationACLs, app.logAggregationContext)); 
    }
  }

  /**
   * Handles the APPLICATION_LOG_HANDLING_INITED event that occurs after
   * {@link LogAggregationService} has created the directories for the app
   * and started the aggregation thread for the app.
   * 
   * In particular, this requests that the {@link ResourceLocalizationService}
   * localize the application-scoped resources.
   * 初始化日志服务器完成
   */
  @SuppressWarnings("unchecked")
  static class AppLogInitDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizationEvent(
              LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
    }
  }

  /**
   * Handles the APPLICATION_LOG_HANDLING_FAILED event that occurs after
   * {@link LogAggregationService} has failed to initialize the log 
   * aggregation service
   * 
   * In particular, this requests that the {@link ResourceLocalizationService}
   * localize the application-scoped resources.
   * 日志服务器初始化失败
   */
  @SuppressWarnings("unchecked")
  static class AppLogInitFailTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      LOG.warn("Log Aggregation service failed to initialize, there will " + 
               "be no logs for this application");
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizationEvent(
              LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
    }
  }
  /**
   * Handles INIT_CONTAINER events which request that we launch a new
   * container. When we're still in the INITTING state, we simply
   * queue these up. When we're in the RUNNING state, we pass along
   * an ContainerInitEvent to the appropriate ContainerImpl.
   * 在运行状态下,初始化一个容器,即为该任务在该节点上分配了一个容器,则需要调动容器的init事件
   * 真正去初始化一个容器,此时该容器所在的应用已经处于运行中了,即所需要的资源已经加载完毕
   *调用new ContainerInitEvent(container.getContainerId())
   */
  @SuppressWarnings("unchecked")
  static class InitContainerTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationContainerInitEvent initEvent = (ApplicationContainerInitEvent) event;
      Container container = initEvent.getContainer();
      //添加映射关系
      app.containers.put(container.getContainerId(), container);
      LOG.info("Adding " + container.getContainerId()
          + " to application " + app.toString());
      
      switch (app.getApplicationState()) {
      case RUNNING://如果应用已经在运行中了,则调用容器初始化事件
        app.dispatcher.getEventHandler().handle(new ContainerInitEvent(container.getContainerId()));
        break;
      case INITING:
      case NEW:
        // these get queued up and sent out in AppInitDoneTransition
        break;
      default:
        assert false : "Invalid state for InitContainerTransition: " +
            app.getApplicationState();
      }
    }
  }

  /**
   * 应用全部初始化完成,开始到run状态
   * 初始化容器
   */
  @SuppressWarnings("unchecked")
  static class AppInitDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      // Start all the containers waiting for ApplicationInit
      for (Container container : app.containers.values()) {
        app.dispatcher.getEventHandler().handle(new ContainerInitEvent(container.getContainerId()));
      }
    }
  }

  /**
   * 当应用完成了一个容器时触发的事件
   * 移除该容器
   */
  static final class ContainerDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationContainerFinishedEvent containerEvent = (ApplicationContainerFinishedEvent) event;
      if (null == app.containers.remove(containerEvent.getContainerID())) {//真正从应用中移除该容器
        LOG.warn("Removing unknown " + containerEvent.getContainerID() + " from application " + app.toString());
      } else {
        LOG.info("Removing " + containerEvent.getContainerID() + " from application " + app.toString());
      }
    }
  }

  /**
   * 应用已经完成了,容器也没有了,因此调用该方法
   */
  @SuppressWarnings("unchecked")
  void handleAppFinishWithContainersCleanedup() {
    // Delete Application level resources 由于该应用已经被完成,因此要清理该应用加载的资源
    this.dispatcher.getEventHandler().handle(new ApplicationLocalizationEvent(LocalizationEventType.DESTROY_APPLICATION_RESOURCES, this));

    // tell any auxiliary services that the app is done 发送应用已经停止
    this.dispatcher.getEventHandler().handle(new AuxServicesEvent(AuxServicesEventType.APPLICATION_STOP, appId));
    // TODO: Trigger the LogsManager
  }

  /**
   * 接收到应用完成事件,该事件是resourceManager发过来的,即该应用确实已经完成了
   * 如果没有容器了,则清理应用
   * 如意有容器,则需要对容器进行kill
   */
  @SuppressWarnings("unchecked")
  static class AppFinishTriggeredTransition
      implements
      MultipleArcTransition<ApplicationImpl, ApplicationEvent, ApplicationState> {
    @Override
    public ApplicationState transition(ApplicationImpl app,ApplicationEvent event) {
      ApplicationFinishEvent appEvent = (ApplicationFinishEvent)event;
      if (app.containers.isEmpty()) {//如果没有容器了,则清理应用
        // No container to cleanup. Cleanup app level resources.
        app.handleAppFinishWithContainersCleanedup();
        return ApplicationState.APPLICATION_RESOURCES_CLEANINGUP;
      }

      // Send event to ContainersLauncher to finish all the containers of this
      // application.
      for (ContainerId containerID : app.containers.keySet()) {
        app.dispatcher.getEventHandler().handle(
            new ContainerKillEvent(containerID,
                ContainerExitStatus.KILLED_AFTER_APP_COMPLETION,
                "Container killed on application-finish event: " + appEvent.getDiagnostic()));
      }
      return ApplicationState.FINISHING_CONTAINERS_WAIT;
    }
  }

  /**
   * 一个容器完成之后,调用该方法,删除该容器的映射,
   * 如果没有容器了,则设置为APPLICATION_RESOURCES_CLEANINGUP状态
   * 如果还有容器,则设置为FINISHING_CONTAINERS_WAIT
   */
  static class AppFinishTransition implements
    MultipleArcTransition<ApplicationImpl, ApplicationEvent, ApplicationState> {

    @Override
    public ApplicationState transition(ApplicationImpl app,
        ApplicationEvent event) {

      ApplicationContainerFinishedEvent containerFinishEvent =
          (ApplicationContainerFinishedEvent) event;
      LOG.info("Removing " + containerFinishEvent.getContainerID()
          + " from application " + app.toString());
      //删除该容器的映射
      app.containers.remove(containerFinishEvent.getContainerID());

      if (app.containers.isEmpty()) {
        // All containers are cleanedup.
        app.handleAppFinishWithContainersCleanedup();
        return ApplicationState.APPLICATION_RESOURCES_CLEANINGUP;
      }

      return ApplicationState.FINISHING_CONTAINERS_WAIT;
    }

  }

  /**
   * app全部完成,即从"清理应用程序的资源"到"FINISHED"状态时,触发该函数,该函数即表示该应用彻底被完成了
   */
  @SuppressWarnings("unchecked")
  static class AppCompletelyDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {

      // Inform the logService 向日志服务器发送应用彻底完成了
      app.dispatcher.getEventHandler().handle(new LogHandlerAppFinishedEvent(app.appId));
      //向token服务器发送该应用彻底完成了
      app.context.getNMTokenSecretManager().appFinished(app.getAppId());
    }
  }

  /**
   * 当日志服务器完成该应用的日志服务时,调用该方法
   */
  static class AppLogsAggregatedTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationId appId = event.getApplicationID();
      //从全局的上下文中删除该应用,因为该应用已经全部完成,以后不需要该应用了,因此从应用的全局上下文中删除该应用
      app.context.getApplications().remove(appId);
      //删除该应用的权限
      app.aclsManager.removeApplication(appId);
      try {
        //删除该应用的存储
        app.context.getNMStateStore().removeApplication(appId);
      } catch (IOException e) {
        LOG.error("Unable to remove application from state store", e);
      }
    }
  }

  //根据状态机变化进行函数处理
  @Override
  public void handle(ApplicationEvent event) {

    this.writeLock.lock();

    try {
      ApplicationId applicationID = event.getApplicationID();
      LOG.debug("Processing " + applicationID + " of type " + event.getType());

      ApplicationState oldState = stateMachine.getCurrentState();
      ApplicationState newState = null;
      try {
        // queue event requesting init of the same app
        newState = stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.warn("Can't handle this event at current state", e);
      }
      if (oldState != newState) {
        LOG.info("Application " + applicationID + " transitioned from "
            + oldState + " to " + newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    return appId.toString();
  }

  @VisibleForTesting
  public LogAggregationContext getLogAggregationContext() {
    try {
      this.readLock.lock();
      return this.logAggregationContext;
    } finally {
      this.readLock.unlock();
    }
  }
}

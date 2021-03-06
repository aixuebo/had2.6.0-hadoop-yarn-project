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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 日志聚合服务
 */
public class LogAggregationService extends AbstractService implements
    LogHandler {

  private static final Log LOG = LogFactory.getLog(LogAggregationService.class);

  /*
   * Expected deployment TLD will be 1777, owner=<NMOwner>, group=<NMGroup -
   * Group to which NMOwner belongs> App dirs will be created as 770,
   * owner=<AppOwner>, group=<NMGroup>: so that the owner and <NMOwner> can
   * access / modify the files.
   * <NMGroup> should obviously be a limited access group.
   */
  /**
   * Permissions for the top level directory under which app directories will be
   * created.
   */
  private static final FsPermission TLDIR_PERMISSIONS = FsPermission
      .createImmutable((short) 01777);//远程根节点remoteRootLogDir的权限
  /**
   * Permissions for the Application directory.
   */
  private static final FsPermission APP_DIR_PERMISSIONS = FsPermission
      .createImmutable((short) 0770);//每一个应用app对应的目录权限

  private final Context context;//节点的上下文对象
  private final DeletionService deletionService;//删除服务
  private final Dispatcher dispatcher;

  private LocalDirsHandlerService dirsHandler;
  Path remoteRootLogDir;//聚合后的日志,存储在HDFS的哪个目录上
  String remoteRootLogDirSuffix;
  private NodeId nodeId;

  /**
   * key是appId,value是该app的日志聚合类,当应用完成的时候,要对该应用的日志进行聚合
   */
  private final ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;

  private final ExecutorService threadPool;
  
  public LogAggregationService(Dispatcher dispatcher, Context context,
      DeletionService deletionService, LocalDirsHandlerService dirsHandler) {
    super(LogAggregationService.class.getName());
    this.dispatcher = dispatcher;
    this.context = context;
    this.deletionService = deletionService;
    this.dirsHandler = dirsHandler;
    this.appLogAggregators = new ConcurrentHashMap<ApplicationId, AppLogAggregator>();
    this.threadPool = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat("LogAggregationService #%d")
          .build());
  }

  protected void serviceInit(Configuration conf) throws Exception {
    this.remoteRootLogDir =
        new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    this.remoteRootLogDirSuffix =
        conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // NodeId is only available during start, the following cannot be moved
    // anywhere else.
    this.nodeId = this.context.getNodeId();
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info(this.getName() + " waiting for pending aggregation during exit");
    stopAggregators();
    super.serviceStop();
  }
   
  private void stopAggregators() {
    threadPool.shutdown();
    // if recovery on restart is supported then leave outstanding aggregations
    // to the next restart
    boolean shouldAbort = context.getNMStateStore().canRecover()
        && !context.getDecommissioned();
    // politely ask to finish
    for (AppLogAggregator aggregator : appLogAggregators.values()) {
      if (shouldAbort) {
        aggregator.abortLogAggregation();
      } else {
        aggregator.finishLogAggregation();
      }
    }
    while (!threadPool.isTerminated()) { // wait for all threads to finish
      for (ApplicationId appId : appLogAggregators.keySet()) {
        LOG.info("Waiting for aggregation to complete for " + appId);
      }
      try {
        if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
          threadPool.shutdownNow(); // send interrupt to hurry them along
        }
      } catch (InterruptedException e) {
        LOG.warn("Aggregation stop interrupted!");
        break;
      }
    }
    for (ApplicationId appId : appLogAggregators.keySet()) {
      LOG.warn("Some logs may not have been aggregated for " + appId);
    }
  }

  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }

  /**
   * 校验现有目录的权限
   */
  void verifyAndCreateRemoteLogDir(Configuration conf) {
    // Checking the existence of the TLD
    FileSystem remoteFS = null;
    try {
      remoteFS = getFileSystem(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Unable to get Remote FileSystem instance", e);
    }
    boolean remoteExists = true;//远程根节点是否存在,true表示存在
    try {
      FsPermission perms =
          remoteFS.getFileStatus(this.remoteRootLogDir).getPermission();
      if (!perms.equals(TLDIR_PERMISSIONS)) {
        LOG.warn("Remote Root Log Dir [" + this.remoteRootLogDir
            + "] already exist, but with incorrect permissions. "
            + "Expected: [" + TLDIR_PERMISSIONS + "], Found: [" + perms
            + "]." + " The cluster may have problems with multiple users.");
      }
    } catch (FileNotFoundException e) {
      remoteExists = false;
    } catch (IOException e) {
      throw new YarnRuntimeException(
          "Failed to check permissions for dir ["
              + this.remoteRootLogDir + "]", e);
    }
    if (!remoteExists) {//远程节点不存在,则创建该节点
      LOG.warn("Remote Root Log Dir [" + this.remoteRootLogDir
          + "] does not exist. Attempting to create it.");
      try {
        Path qualified =
            this.remoteRootLogDir.makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());
        remoteFS.mkdirs(qualified, new FsPermission(TLDIR_PERMISSIONS));
        remoteFS.setPermission(qualified, new FsPermission(TLDIR_PERMISSIONS));
      } catch (IOException e) {
        throw new YarnRuntimeException("Failed to create remoteLogDir ["
            + this.remoteRootLogDir + "]", e);
      }
    }
  }

  //返回路径/remoteRootLogDir/user/suffix/appId/nodeId 远程该应用在该节点的目录
  Path getRemoteNodeLogFileForApp(ApplicationId appId, String user) {
    return LogAggregationUtils.getRemoteNodeLogFileForApp(
        this.remoteRootLogDir, appId, user, this.nodeId,
        this.remoteRootLogDirSuffix);
  }

  //返回/remoteRootLogDir/user/suffix/appId 远程该应用的目录
  Path getRemoteAppLogDir(ApplicationId appId, String user) {
    return LogAggregationUtils.getRemoteAppLogDir(this.remoteRootLogDir, appId,
        user, this.remoteRootLogDirSuffix);
  }

  /**
   * 创建目录,并且附加权限
   */
  private void createDir(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    FsPermission dirPerm = new FsPermission(fsPerm);
    fs.mkdirs(path, dirPerm);
    FsPermission umask = FsPermission.getUMask(fs.getConf());
    if (!dirPerm.equals(dirPerm.applyUMask(umask))) {
      fs.setPermission(path, new FsPermission(fsPerm));
    }
  }

  /**
   * 检查是否该路径存在,并且赋予该路径一个权限
   */
  private boolean checkExists(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    boolean exists = true;//true表示该path存在
    try {
      FileStatus appDirStatus = fs.getFileStatus(path);
      if (!APP_DIR_PERMISSIONS.equals(appDirStatus.getPermission())) {
        fs.setPermission(path, APP_DIR_PERMISSIONS);
      }
    } catch (FileNotFoundException fnfe) {
      exists = false;
    }
    return exists;
  }

  /**
   * 为一个app创建一系列目录
   */
  protected void createAppDir(final String user, final ApplicationId appId,
      UserGroupInformation userUgi) {
    try {
      userUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          try {
            // TODO: Reuse FS for user?
            FileSystem remoteFS = getFileSystem(getConfig());

            // Only creating directories if they are missing to avoid
            // unnecessary load on the filesystem from all of the nodes 创建远程应用目录/remoteRootLogDir/user/suffix/appId
            Path appDir = LogAggregationUtils.getRemoteAppLogDir(
                LogAggregationService.this.remoteRootLogDir, appId, user,
                LogAggregationService.this.remoteRootLogDirSuffix);
            appDir = appDir.makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());

            if (!checkExists(remoteFS, appDir, APP_DIR_PERMISSIONS)) {//该/remoteRootLogDir/user/suffix/appId目录不存在
              Path suffixDir = LogAggregationUtils.getRemoteLogSuffixedDir(
                  LogAggregationService.this.remoteRootLogDir, user,
                  LogAggregationService.this.remoteRootLogDirSuffix);//remoteRootLogDir/user/suffix
              suffixDir = suffixDir.makeQualified(remoteFS.getUri(),
                  remoteFS.getWorkingDirectory());

              if (!checkExists(remoteFS, suffixDir, APP_DIR_PERMISSIONS)) {//如果remoteRootLogDir/user/suffix目录不存在
                Path userDir = LogAggregationUtils.getRemoteLogUserDir(
                    LogAggregationService.this.remoteRootLogDir, user);///remoteRootLogDir/user
                userDir = userDir.makeQualified(remoteFS.getUri(),
                    remoteFS.getWorkingDirectory());

                if (!checkExists(remoteFS, userDir, APP_DIR_PERMISSIONS)) {//如果/remoteRootLogDir/user目录不存在
                  createDir(remoteFS, userDir, APP_DIR_PERMISSIONS);//创建目录/remoteRootLogDir/user
                }

                createDir(remoteFS, suffixDir, APP_DIR_PERMISSIONS);//创建目录remoteRootLogDir/user/suffix
              }

              createDir(remoteFS, appDir, APP_DIR_PERMISSIONS);//创建目录/remoteRootLogDir/user/suffix/appId
            }
          } catch (IOException e) {
            LOG.error("Failed to setup application log directory for "
                + appId, e);
            throw e;
          }
          return null;
        }
      });
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * 初始化一个应用的聚合处理
   */
  @SuppressWarnings("unchecked")
  private void initApp(final ApplicationId appId, String user,
      Credentials credentials, ContainerLogsRetentionPolicy logRetentionPolicy,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext) {
    ApplicationEvent eventResponse;
    try {
      verifyAndCreateRemoteLogDir(getConfig());
      initAppAggregator(appId, user, credentials, logRetentionPolicy, appAcls,
          logAggregationContext);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED);
    } catch (YarnRuntimeException e) {
      LOG.warn("Application failed to init aggregation", e);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED);
    }
    this.dispatcher.getEventHandler().handle(eventResponse);
  }
  
  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }


  /**
   * 1.创建app远程目录
   * 2.生成AppLogAggregatorImpl对象
   */
  protected void initAppAggregator(final ApplicationId appId, String user,
      Credentials credentials, ContainerLogsRetentionPolicy logRetentionPolicy,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext) {

    // Get user's FileSystem credentials 校验user的权限
    final UserGroupInformation userUgi =
        UserGroupInformation.createRemoteUser(user);
    if (credentials != null) {
      userUgi.addCredentials(credentials);
    }

    // New application
    final AppLogAggregator appLogAggregator =
        new AppLogAggregatorImpl(this.dispatcher, this.deletionService,
            getConfig(), appId, userUgi, this.nodeId, dirsHandler,
            getRemoteNodeLogFileForApp(appId, user), logRetentionPolicy,
            appAcls, logAggregationContext, this.context,
            getLocalFileContext(getConfig()));
    if (this.appLogAggregators.putIfAbsent(appId, appLogAggregator) != null) {
      throw new YarnRuntimeException("Duplicate initApp for " + appId);
    }
    // wait until check for existing aggregator to create dirs
    try {
      // Create the app dir
      createAppDir(user, appId, userUgi);
    } catch (Exception e) {
      appLogAggregators.remove(appId);
      closeFileSystems(userUgi);
      if (!(e instanceof YarnRuntimeException)) {
        e = new YarnRuntimeException(e);
      }
      throw (YarnRuntimeException)e;
    }


    // TODO Get the user configuration for the list of containers that need log
    // aggregation.

    // Schedule the aggregator.
    Runnable aggregatorWrapper = new Runnable() {
      public void run() {
        try {
          appLogAggregator.run();
        } finally {
          appLogAggregators.remove(appId);
          closeFileSystems(userUgi);
        }
      }
    };
    this.threadPool.execute(aggregatorWrapper);
  }

  protected void closeFileSystems(final UserGroupInformation userUgi) {
    try {
      FileSystem.closeAllForUGI(userUgi);
    } catch (IOException e) {
      LOG.warn("Failed to close filesystems: ", e);
    }
  }

  // for testing only 获取等待聚合的应用的数量
  @Private
  int getNumAggregators() {
    return this.appLogAggregators.size();
  }

  //一个容器完成了,要对该容器如何聚合处理
  private void stopContainer(ContainerId containerId, int exitCode) {

    // A container is complete. Put this containers' logs up for aggregation if
    // this containers' logs are needed.

    AppLogAggregator aggregator = this.appLogAggregators.get(containerId.getApplicationAttemptId().getApplicationId());
    if (aggregator == null) {
      LOG.warn("Log aggregation is not initialized for " + containerId
          + ", did it fail to start?");
      return;
    }
    aggregator.startContainerLogAggregation(containerId, exitCode == 0);
  }

  //一个app完成了,要对该app的日志进行聚合处理
  private void stopApp(ApplicationId appId) {

    // App is complete. Finish up any containers' pending log aggregation and
    // close the application specific logFile.

    AppLogAggregator aggregator = this.appLogAggregators.get(appId);
    if (aggregator == null) {
      LOG.warn("Log aggregation is not initialized for " + appId
          + ", did it fail to start?");
      return;
    }
    aggregator.finishLogAggregation();
  }

  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        LogHandlerAppStartedEvent appStartEvent = (LogHandlerAppStartedEvent) event;
        initApp(appStartEvent.getApplicationId(), appStartEvent.getUser(),
            appStartEvent.getCredentials(),
            appStartEvent.getLogRetentionPolicy(),
            appStartEvent.getApplicationAcls(),
            appStartEvent.getLogAggregationContext());
        break;
      case CONTAINER_FINISHED:
        LogHandlerContainerFinishedEvent containerFinishEvent = (LogHandlerContainerFinishedEvent) event;
        stopContainer(containerFinishEvent.getContainerId(),containerFinishEvent.getExitCode());
        break;
      case APPLICATION_FINISHED:
        LogHandlerAppFinishedEvent appFinishedEvent = (LogHandlerAppFinishedEvent) event;
        stopApp(appFinishedEvent.getApplicationId());
        break;
      default:
        ; // Ignore
    }

  }

  @VisibleForTesting
  public ConcurrentMap<ApplicationId, AppLogAggregator> getAppLogAggregators() {
    return this.appLogAggregators;
  }

  @VisibleForTesting
  public NodeId getNodeId() {
    return this.nodeId;
  }
}

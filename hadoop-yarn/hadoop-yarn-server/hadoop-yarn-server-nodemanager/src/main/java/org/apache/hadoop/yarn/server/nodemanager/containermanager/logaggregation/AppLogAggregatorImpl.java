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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


/**
 * 应用日志聚合
 * 每一个应用拥有一个该对象,用于存储日志
 */
public class AppLogAggregatorImpl implements AppLogAggregator {

  private static final Log LOG = LogFactory
      .getLog(AppLogAggregatorImpl.class);
  private static final int THREAD_SLEEP_TIME = 1000;
  // This is temporary solution. The configuration will be deleted once
  // we find a more scalable method to only write a single log file per LRS.
  private static final String NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP//每个app最多允许聚合多少个日志
      = YarnConfiguration.NM_PREFIX + "log-aggregation.num-log-files-per-app";
  private static final int
      DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP = 30;
  
  // This configuration is for debug and test purpose. By setting
  // this configuration as true. We can break the lower bound of
  // NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS.
  private static final String NM_LOG_AGGREGATION_DEBUG_ENABLED = YarnConfiguration.NM_PREFIX + "log-aggregation.debug-enabled";
  private static final boolean DEFAULT_NM_LOG_AGGREGATION_DEBUG_ENABLED = false;

  private static final long NM_LOG_AGGREGATION_MIN_ROLL_MONITORING_INTERVAL_SECONDS = 3600;

  private final LocalDirsHandlerService dirsHandler;
  private final Dispatcher dispatcher;
  private final ApplicationId appId;
  private final String applicationId;
  private boolean logAggregationDisabled = false;
  private final Configuration conf;
  private final DeletionService delService;
  private final UserGroupInformation userUgi;
  private final Path remoteNodeLogFileForApp;///remoteRootLogDir/user/suffix/appId/nodeId 远程该应用在该节点的目录
  private final Path remoteNodeTmpLogFileForApp;
  private final ContainerLogsRetentionPolicy retentionPolicy;

  private final BlockingQueue<ContainerId> pendingContainers;//已经完成的容器,等待上传
  private final AtomicBoolean appFinishing = new AtomicBoolean();//引用是否完成
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();
  private final AtomicBoolean aborted = new AtomicBoolean();//是否出异常,导致中断
  private final Map<ApplicationAccessType, String> appAcls;
  private final FileContext lfs;
  private final LogAggregationContext logAggregationContext;
  private final Context context;
  private final int retentionSize;//每个app最多允许聚合多少个日志
  private final long rollingMonitorInterval;//每次上传日志的时间间隔
  private final NodeId nodeId;

  /**
   * 每个容器与该容器对应的对象映射
   */
  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators = new HashMap<ContainerId, ContainerLogAggregator>();

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      ContainerLogsRetentionPolicy retentionPolicy,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs) {
    this.dispatcher = dispatcher;
    this.conf = conf;
    this.delService = deletionService;
    this.appId = appId;
    this.applicationId = ConverterUtils.toString(appId);
    this.userUgi = userUgi;
    this.dirsHandler = dirsHandler;
    this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
    this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    this.retentionPolicy = retentionPolicy;
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
    this.appAcls = appAcls;
    this.lfs = lfs;
    this.logAggregationContext = logAggregationContext;
    this.context = context;
    this.nodeId = nodeId;
    int configuredRentionSize =
        conf.getInt(NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP,
            DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP);
    if (configuredRentionSize <= 0) {
      this.retentionSize =
          DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP;
    } else {
      this.retentionSize = configuredRentionSize;
    }
    long configuredRollingMonitorInterval = conf.getLong(
      YarnConfiguration
        .NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS,
      YarnConfiguration
        .DEFAULT_NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS);
    boolean debug_mode =
        conf.getBoolean(NM_LOG_AGGREGATION_DEBUG_ENABLED,
          DEFAULT_NM_LOG_AGGREGATION_DEBUG_ENABLED);
    if (configuredRollingMonitorInterval > 0
        && configuredRollingMonitorInterval <
          NM_LOG_AGGREGATION_MIN_ROLL_MONITORING_INTERVAL_SECONDS) {
      if (debug_mode) {
        this.rollingMonitorInterval = configuredRollingMonitorInterval;
      } else {
        LOG.warn(
            "rollingMonitorIntervall should be more than or equal to "
            + NM_LOG_AGGREGATION_MIN_ROLL_MONITORING_INTERVAL_SECONDS
            + " seconds. Using "
            + NM_LOG_AGGREGATION_MIN_ROLL_MONITORING_INTERVAL_SECONDS
            + " seconds instead.");
        this.rollingMonitorInterval =
            NM_LOG_AGGREGATION_MIN_ROLL_MONITORING_INTERVAL_SECONDS;
      }
    } else {
      if (configuredRollingMonitorInterval <= 0) {
        LOG.warn("rollingMonitorInterval is set as "
            + configuredRollingMonitorInterval + ". "
            + "The log rolling mornitoring interval is disabled. "
            + "The logs will be aggregated after this application is finished.");
      } else {
        LOG.warn("rollingMonitorInterval is set as "
            + configuredRollingMonitorInterval + ". "
            + "The logs will be aggregated every "
            + configuredRollingMonitorInterval + " seconds");
      }
      this.rollingMonitorInterval = configuredRollingMonitorInterval;
    }
  }

  /**
   * 上传本地属于该app的日志文件,到HDFS上,即聚合,是该类的核心方法
   */
  private void uploadLogsForContainers() {
    if (this.logAggregationDisabled) {
      return;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials systemCredentials = context.getSystemCredentialsForApps().get(appId);
      if (systemCredentials != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding new framework-token for " + appId
              + " for log-aggregation: " + systemCredentials.getAllTokens()
              + "; userUgi=" + userUgi);
        }
        // this will replace old token
        userUgi.addCredentials(systemCredentials);
      }
    }

    // Create a set of Containers whose logs will be uploaded in this cycle.
    // It includes:
    // a) all containers in pendingContainers: those containers are finished
    //    and satisfy the retentionPolicy.
    // b) some set of running containers: For all the Running containers,
    // we have ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY,
    // so simply set wasContainerSuccessful as true to
    // bypass FAILED_CONTAINERS check and find the running containers 
    // which satisfy the retentionPolicy.
    Set<ContainerId> pendingContainerInThisCycle = new HashSet<ContainerId>();
    this.pendingContainers.drainTo(pendingContainerInThisCycle);//将pendingContainers的内容存放到pendingContainerInThisCycle集合中
    
    Set<ContainerId> finishedContainers = new HashSet<ContainerId>(pendingContainerInThisCycle);
    //循环该应用所有的容器,将符合标准的容器添加到pendingContainerInThisCycle集合中
    if (this.context.getApplications().get(this.appId) != null) {
      for (ContainerId container : this.context.getApplications().get(this.appId).getContainers().keySet()) {
        if (shouldUploadLogs(container, true)) {
          pendingContainerInThisCycle.add(container);
        }
      }
    }

    /**
     * 生成一个输出流
     */
    LogWriter writer = null;
    try {
      try {
        writer =
            new LogWriter(this.conf, this.remoteNodeTmpLogFileForApp,
              this.userUgi);
        // Write ACLs once when the writer is created.
        writer.writeApplicationACLs(appAcls);
        writer.writeApplicationOwner(this.userUgi.getShortUserName());

      } catch (IOException e1) {
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Skip log upload this time. ");
        return;
      }

      boolean uploadedLogsInThisCycle = false;
      for (ContainerId container : pendingContainerInThisCycle) {
        ContainerLogAggregator aggregator = null;//获取该容器对应的ContainerLogAggregator对象
        if (containerLogAggregators.containsKey(container)) {
          aggregator = containerLogAggregators.get(container);
        } else {
          aggregator = new ContainerLogAggregator(container);
          containerLogAggregators.put(container, aggregator);
        }
        Set<Path> uploadedFilePathsInThisCycle = aggregator.doContainerLogAggregation(writer);
        if (uploadedFilePathsInThisCycle.size() > 0) {
          uploadedLogsInThisCycle = true;
        }
        this.delService.delete(this.userUgi.getShortUserName(), null,
          uploadedFilePathsInThisCycle
            .toArray(new Path[uploadedFilePathsInThisCycle.size()]));

        // This container is finished, and all its logs have been uploaded,
        // remove it from containerLogAggregators.
        if (finishedContainers.contains(container)) {
          containerLogAggregators.remove(container);
        }
      }

      // Before upload logs, make sure the number of existing logs
      // is smaller than the configured NM log aggregation retention size.
      if (uploadedLogsInThisCycle) {
        cleanOldLogs();
      }

      if (writer != null) {
        writer.close();
      }

      final Path renamedPath = this.rollingMonitorInterval <= 0
              ? remoteNodeLogFileForApp : new Path(
                remoteNodeLogFileForApp.getParent(),
                remoteNodeLogFileForApp.getName() + "_"
                    + System.currentTimeMillis());

      final boolean rename = uploadedLogsInThisCycle;
      try {
        userUgi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            FileSystem remoteFS = FileSystem.get(conf);
            if (remoteFS.exists(remoteNodeTmpLogFileForApp)) {
              if (rename) {
                remoteFS.rename(remoteNodeTmpLogFileForApp, renamedPath);
              } else {
                remoteFS.delete(remoteNodeTmpLogFileForApp, false);
              }
            }
            return null;
          }
        });
      } catch (Exception e) {
        LOG.error(
          "Failed to move temporary log file to final location: ["
              + remoteNodeTmpLogFileForApp + "] to ["
              + renamedPath + "]", e);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private void cleanOldLogs() {
    try {
      final FileSystem remoteFS = this.remoteNodeLogFileForApp.getFileSystem(conf);
      Path appDir = this.remoteNodeLogFileForApp.getParent().makeQualified(remoteFS.getUri(), remoteFS.getWorkingDirectory());
            
      Set<FileStatus> status = new HashSet<FileStatus>(Arrays.asList(remoteFS.listStatus(appDir)));

      Iterable<FileStatus> mask =
          Iterables.filter(status, new Predicate<FileStatus>() {
            @Override
            public boolean apply(FileStatus next) {
              return next.getPath().getName().contains(LogAggregationUtils.getNodeString(nodeId))
                && !next.getPath().getName().endsWith(LogAggregationUtils.TMP_FILE_SUFFIX);
            }
          });
      status = Sets.newHashSet(mask);
      // Normally, we just need to delete one oldest log
      // before we upload a new log.
      // If we can not delete the older logs in this cycle,
      // we will delete them in next cycle.
      if (status.size() >= this.retentionSize) {
        // sort by the lastModificationTime ascending
        List<FileStatus> statusList = new ArrayList<FileStatus>(status);
        Collections.sort(statusList, new Comparator<FileStatus>() {
          public int compare(FileStatus s1, FileStatus s2) {
            return s1.getModificationTime() < s2.getModificationTime() ? -1
                : s1.getModificationTime() > s2.getModificationTime() ? 1 : 0;
          }
        });
        for (int i = 0 ; i <= statusList.size() - this.retentionSize; i++) {
          final FileStatus remove = statusList.get(i);
          try {
            userUgi.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                remoteFS.delete(remove.getPath(), false);
                return null;
              }
            });
          } catch (Exception e) {
            LOG.error("Failed to delete " + remove.getPath(), e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to clean old logs", e);
    }
  }

  @Override
  public void run() {
    try {
      doAppLogAggregation();
    } finally {
      if (!this.appAggregationFinished.get()) {
        LOG.warn("Aggregation did not complete for application " + appId);
      }
      this.appAggregationFinished.set(true);
    }
  }

  @SuppressWarnings("unchecked")
  private void doAppLogAggregation() {
    while (!this.appFinishing.get() && !this.aborted.get()) {//只要是该app没有终止,也没有完成,则不断循环
      synchronized(this) {
        try {
          if (this.rollingMonitorInterval > 0) {
            wait(this.rollingMonitorInterval * 1000);
            if (this.appFinishing.get() || this.aborted.get()) {
              break;
            }
            //上传本地属于该app的日志文件,到HDFS上,即聚合,是该类的核心方法
            uploadLogsForContainers();
          } else {
            wait(THREAD_SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          LOG.warn("PendingContainers queue is interrupted");
          this.appFinishing.set(true);
        }
      }
    }

    //说明已经应用完成,或者中断,才能走到以下流程中
    
    if (this.aborted.get()) {
      return;
    }

    //上传本地属于该app的日志文件,到HDFS上,即聚合,是该类的核心方法
    // App is finished, upload the container logs.
    uploadLogsForContainers();

    // Remove the local app-log-dirs 删除本地属于该app的日志文件
    List<Path> localAppLogDirs = new ArrayList<Path>();
    for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
      Path logPath = new Path(rootLogDir, applicationId);
      try {
        // check if log dir exists
        lfs.getFileStatus(logPath);
        localAppLogDirs.add(logPath);
      } catch (UnsupportedFileSystemException ue) {
        LOG.warn("Log dir " + rootLogDir + "is an unsupported file system", ue);
        continue;
      } catch (IOException fe) {
        continue;
      }
    }

    if (localAppLogDirs.size() > 0) {
      this.delService.delete(this.userUgi.getShortUserName(), null,
        localAppLogDirs.toArray(new Path[localAppLogDirs.size()]));
    }
    
    this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
    this.appAggregationFinished.set(true);    
  }

  private Path getRemoteNodeTmpLogFileForApp() {
    return new Path(remoteNodeLogFileForApp.getParent(),
      (remoteNodeLogFileForApp.getName() + LogAggregationUtils.TMP_FILE_SUFFIX));
  }

  // TODO: The condition: containerId.getId() == 1 to determine an AM container
  // is not always true. 判断该容器是否是要保存的容器
  private boolean shouldUploadLogs(ContainerId containerId,boolean wasContainerSuccessful) {
      
    // All containers 保留所有日志
    if (this.retentionPolicy.equals(ContainerLogsRetentionPolicy.ALL_CONTAINERS)) {
      return true;
    }

    // AM Container only 仅仅保留master容器
    if (this.retentionPolicy.equals(ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY)) {
      if ((containerId.getContainerId() & ContainerId.CONTAINER_ID_BITMASK)== 1) {//证明该容器是否是master容器
        return true;
      }
      return false;
    }

    // AM + Failing containers 仅仅保留失败的容器
    if (this.retentionPolicy.equals(ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY)) {
      if ((containerId.getContainerId() & ContainerId.CONTAINER_ID_BITMASK) == 1) {
        return true;
      } else if(!wasContainerSuccessful) {
        return true;
      }
      return false;
    }
    return false;
  }

  /**
   * 如果一个容器完成了,并且符合上传的要求,则存放到pendingContainers队列中
   */
  @Override
  public void startContainerLogAggregation(ContainerId containerId,boolean wasContainerSuccessful) {
    if (shouldUploadLogs(containerId, wasContainerSuccessful)) {
      LOG.info("Considering container " + containerId
          + " for log-aggregation");
      this.pendingContainers.add(containerId);
    }
  }

  @Override
  public synchronized void finishLogAggregation() {
    LOG.info("Application just finished : " + this.applicationId);
    this.appFinishing.set(true);
    this.notifyAll();
  }

  @Override
  public synchronized void abortLogAggregation() {
    LOG.info("Aborting log aggregation for " + this.applicationId);
    this.aborted.set(true);
    this.notifyAll();
  }

  @Private
  @VisibleForTesting
  public synchronized void doLogAggregationOutOfBand() {
    LOG.info("Do OutOfBand log aggregation");
    this.notifyAll();
  }

  /**
   * 一个容器拥有一个该对象
   */
  private class ContainerLogAggregator {
    private final ContainerId containerId;
    private Set<String> uploadedFileMeta = new HashSet<String>();
    
    public ContainerLogAggregator(ContainerId containerId) {
      this.containerId = containerId;
    }

    public Set<Path> doContainerLogAggregation(LogWriter writer) {
      LOG.info("Uploading logs for container " + containerId
          + ". Current good log dirs are "
          + StringUtils.join(",", dirsHandler.getLogDirs()));
      final LogKey logKey = new LogKey(containerId);
      final LogValue logValue =
          new LogValue(dirsHandler.getLogDirs(), containerId,
            userUgi.getShortUserName(), logAggregationContext,
            this.uploadedFileMeta);
      try {
        writer.append(logKey, logValue);
      } catch (Exception e) {
        LOG.error("Couldn't upload logs for " + containerId
            + ". Skipping this container.");
        return new HashSet<Path>();
      }
      this.uploadedFileMeta.addAll(logValue.getCurrentUpLoadedFileMeta());
      // if any of the previous uploaded logs have been deleted,
      // we need to remove them from alreadyUploadedLogs
      Iterable<String> mask =
          Iterables.filter(uploadedFileMeta, new Predicate<String>() {
            @Override
            public boolean apply(String next) {
              return logValue.getAllExistingFilesMeta().contains(next);
            }
          });

      this.uploadedFileMeta = Sets.newHashSet(mask);
      return logValue.getCurrentUpLoadedFilesPath();
    }
  }

  // only for test
  @VisibleForTesting
  public UserGroupInformation getUgi() {
    return this.userUgi;
  }
}

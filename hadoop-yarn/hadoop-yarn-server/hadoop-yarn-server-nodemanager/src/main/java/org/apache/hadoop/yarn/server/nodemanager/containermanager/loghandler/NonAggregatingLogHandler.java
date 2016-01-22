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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Log Handler which schedules deletion of log files based on the configured log
 * retention time.
 * 不用聚合应用产生的文件，而死调度器在保留一段时间后,将应用的日志删除
 */
public class NonAggregatingLogHandler extends AbstractService implements
    LogHandler {

  private static final Log LOG = LogFactory.getLog(NonAggregatingLogHandler.class);
  private final Dispatcher dispatcher;
  private final DeletionService delService;
  /**
   * key是应用id,value是该应用的user
   */
  private final Map<ApplicationId, String> appOwners;

  private final LocalDirsHandlerService dirsHandler;
  private long deleteDelaySeconds;//延迟多少秒后执行
  private ScheduledThreadPoolExecutor sched;//生成线程池

  public NonAggregatingLogHandler(Dispatcher dispatcher,
      DeletionService delService, LocalDirsHandlerService dirsHandler) {
    super(NonAggregatingLogHandler.class.getName());
    this.dispatcher = dispatcher;
    this.delService = delService;
    this.dirsHandler = dirsHandler;
    this.appOwners = new ConcurrentHashMap<ApplicationId, String>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // Default 3 hours.
    this.deleteDelaySeconds =
        conf.getLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS,
                YarnConfiguration.DEFAULT_NM_LOG_RETAIN_SECONDS);
    sched = createScheduledThreadPoolExecutor(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (sched != null) {
      sched.shutdown();
      boolean isShutdown = false;
      try {
        isShutdown = sched.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        sched.shutdownNow();
        isShutdown = true;
      }
      if (!isShutdown) {
        sched.shutdownNow();
      }
    }
    super.serviceStop();
  }
  
  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }


  @SuppressWarnings("unchecked")
  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED://应用开始事件,添加该应用以及该应用的所属者
        LogHandlerAppStartedEvent appStartedEvent = (LogHandlerAppStartedEvent) event;
        this.appOwners.put(appStartedEvent.getApplicationId(),appStartedEvent.getUser());
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(appStartedEvent.getApplicationId(),ApplicationEventType.APPLICATION_LOG_HANDLING_INITED));
        break;
      case CONTAINER_FINISHED://容器完成事件
        // Ignore
        break;
      case APPLICATION_FINISHED://应用完成事件,多少小时之后删除该应用所对应的文件夹
        LogHandlerAppFinishedEvent appFinishedEvent = (LogHandlerAppFinishedEvent) event;
        // Schedule - so that logs are available on the UI till they're deleted.
        LOG.info("Scheduling Log Deletion for application: "
            + appFinishedEvent.getApplicationId() + ", with delay of "
            + this.deleteDelaySeconds + " seconds");
        LogDeleterRunnable logDeleter = new LogDeleterRunnable(appOwners.remove(appFinishedEvent.getApplicationId()), appFinishedEvent.getApplicationId());
        try {
          sched.schedule(logDeleter, this.deleteDelaySeconds,TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
          // Handling this event in local thread before starting threads
          // or after calling sched.shutdownNow().
          logDeleter.run();
        }
        break;
      default:
        ; // Ignore
    }
  }

  /**
   * 生成线程池
   */
  ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor(Configuration conf) {
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("LogDeleter #%d").build();
    sched =
        new ScheduledThreadPoolExecutor(conf.getInt(
            YarnConfiguration.NM_LOG_DELETION_THREADS_COUNT,
            YarnConfiguration.DEFAULT_NM_LOG_DELETE_THREAD_COUNT), tf);
    return sched;
  }

  /**
   * 真正的删除应用的文件夹,并且调度"应用的日志完成"事件
   */
  class LogDeleterRunnable implements Runnable {
    private String user;//该应用的所属者
    private ApplicationId applicationId;//应用

    public LogDeleterRunnable(String user, ApplicationId applicationId) {
      this.user = user;
      this.applicationId = applicationId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      List<Path> localAppLogDirs = new ArrayList<Path>();
      FileContext lfs = getLocalFileContext(getConfig());
      for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
        /**
         * 真正的删除应用的文件夹
         */
        Path logDir = new Path(rootLogDir, applicationId.toString());
        try {
          lfs.getFileStatus(logDir);
          localAppLogDirs.add(logDir);
        } catch (UnsupportedFileSystemException ue) {
          LOG.warn("Unsupported file system used for log dir " + logDir, ue);
          continue;
        } catch (IOException ie) {
          continue;
        }
      }

      // Inform the application before the actual delete itself, so that links
      // to logs will no longer be there on NM web-UI.
      NonAggregatingLogHandler.this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.applicationId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
      if (localAppLogDirs.size() > 0) {
        NonAggregatingLogHandler.this.delService.delete(user, null,
          (Path[]) localAppLogDirs.toArray(new Path[localAppLogDirs.size()]));
      }
    }

    @Override
    public String toString() {
      return "LogDeleter for AppId " + this.applicationId.toString()
          + ", owned by " + user;
    }
  }
}
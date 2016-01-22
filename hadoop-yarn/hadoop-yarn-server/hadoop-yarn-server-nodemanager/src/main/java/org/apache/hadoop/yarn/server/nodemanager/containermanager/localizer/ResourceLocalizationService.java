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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRecoveredEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.LocalResourceTrackerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLocalizationState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredUserResources;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerBuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.FSDownload;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 资源本地化服务
 * 是本地化的入口类
 * 
 * 处理LocalizationEventType类型的事件
 */
public class ResourceLocalizationService extends CompositeService
    implements EventHandler<LocalizationEvent>, LocalizationProtocol {

  private static final Log LOG = LogFactory.getLog(ResourceLocalizationService.class);
  public static final String NM_PRIVATE_DIR = "nmPrivate";
  public static final FsPermission NM_PRIVATE_PERM = new FsPermission((short) 0700);

  private Server server;//实现LocalizationProtocol的RPC接口,服务器的地址是localizationServerAddress
  private InetSocketAddress localizationServerAddress;//本节点的服务
  private long cacheTargetSize;//本地化缓存的目标大小,单位M,
  private long cacheCleanupPeriod;//清理缓存的周期时间

  private final ContainerExecutor exec;
  protected final Dispatcher dispatcher;
  private final DeletionService delService;
  private LocalizerTracker localizerTracker;
  private RecordFactory recordFactory;
  private final ScheduledExecutorService cacheCleanup;//定期执行CacheCleanup,清理内存空间
  private LocalizerTokenSecretManager secretManager;
  private NMStateStoreService stateStore;//存储日志行为,便于恢复

  private LocalDirsHandlerService dirsHandler;
  private Context nmContext;//NodeManager的上下问

  /**
   * 公开的资源下载器,所有用户和应用共享该资源管理器
   */
  private LocalResourcesTracker publicRsrc;
  
  /**
   * Map of LocalResourceTrackers keyed by username, for private
   * resources.
   * key:user,value:LocalResourcesTrackerImpl
   * 私有的资源下载器,每一个userName对应一个该下载器
   * key是userName,value是对应的下载器
   */
  private final ConcurrentMap<String,LocalResourcesTracker> privateRsrc = new ConcurrentHashMap<String,LocalResourcesTracker>();

  /**
   * Map of LocalResourceTrackers keyed by appid, for application
   * resources.
   * key:appId,value:LocalResourcesTrackerImpl
   * 应用的资源下载器,每一个应用对应一个该下载器
   * key是应用id,格式是application_1439549102823_0906,value是资源下载器
   */
  private final ConcurrentMap<String,LocalResourcesTracker> appRsrc = new ConcurrentHashMap<String,LocalResourcesTracker>();
  
  FileContext lfs;

  public ResourceLocalizationService(Dispatcher dispatcher,
      ContainerExecutor exec, DeletionService delService,
      LocalDirsHandlerService dirsHandler, Context context) {

    super(ResourceLocalizationService.class.getName());
    this.exec = exec;
    this.dispatcher = dispatcher;
    this.delService = delService;
    this.dirsHandler = dirsHandler;

    this.cacheCleanup = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder()
          .setNameFormat("ResourceLocalizationService Cache Cleanup")
          .build());
    this.stateStore = context.getNMStateStore();
    this.nmContext = context;
  }

  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  /**
   * 校验每个目录的文件极限不能少于36个
   */
  private void validateConf(Configuration conf) {
    int perDirFileLimit =
        conf.getInt(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY,
          YarnConfiguration.DEFAULT_NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY);
    if (perDirFileLimit <= 36) {
      LOG.error(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
          + " parameter is configured with very low value.");
      throw new YarnRuntimeException(
        YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
            + " parameter is configured with a value less than 37.");
    } else {
      LOG.info("per directory file limit = " + perDirFileLimit);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.validateConf(conf);//校验每个目录的文件极限不能少于36个
    this.publicRsrc = new LocalResourcesTrackerImpl(null, null, dispatcher,true, conf, stateStore);
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);

    try {
      lfs = getLocalFileContext(conf);
      lfs.setUMask(new FsPermission((short) FsPermission.DEFAULT_UMASK));

      //不是恢复操作,或者这次操作是第一次操作,则要清空目录
      if (!stateStore.canRecover()|| stateStore.isNewlyCreated()) {
        //初始化,清理本地目录
        cleanUpLocalDirs(lfs, delService);//异步删除现有目录,先改名,在删除
        //初始化本地目录
        initializeLocalDirs(lfs);//创建目录并且赋予权限
        //初始化日志目录
        initializeLogDirs(lfs);//创建目录并且赋予权限
      }
    } catch (Exception e) {
      throw new YarnRuntimeException(
        "Failed to initialize LocalizationService", e);
    }

    cacheTargetSize =
      conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_TARGET_SIZE_MB, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_TARGET_SIZE_MB) << 20;
    
    //清理缓存的周期时间
    cacheCleanupPeriod =
      conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS);
    
    localizationServerAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_PORT);

    localizerTracker = createLocalizerTracker(conf);
    addService(localizerTracker);
    dispatcher.register(LocalizerEventType.class, localizerTracker);
    super.serviceInit(conf);
  }

  //Recover localized resources after an NM restart
  public void recoverLocalizedResources(RecoveredLocalizationState state)
      throws URISyntaxException {
    LocalResourceTrackerState trackerState = state.getPublicTrackerState();
    recoverTrackerResources(publicRsrc, trackerState);

    for (Map.Entry<String, RecoveredUserResources> userEntry :
         state.getUserResources().entrySet()) {
      String user = userEntry.getKey();
      RecoveredUserResources userResources = userEntry.getValue();
      trackerState = userResources.getPrivateTrackerState();
      if (!trackerState.isEmpty()) {
        LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user,
            null, dispatcher, true, super.getConfig(), stateStore);
        LocalResourcesTracker oldTracker = privateRsrc.putIfAbsent(user,
            tracker);
        if (oldTracker != null) {
          tracker = oldTracker;
        }
        recoverTrackerResources(tracker, trackerState);
      }

      for (Map.Entry<ApplicationId, LocalResourceTrackerState> appEntry :
           userResources.getAppTrackerStates().entrySet()) {
        trackerState = appEntry.getValue();
        if (!trackerState.isEmpty()) {
          ApplicationId appId = appEntry.getKey();
          String appIdStr = ConverterUtils.toString(appId);
          LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user,
              appId, dispatcher, false, super.getConfig(), stateStore);
          LocalResourcesTracker oldTracker = appRsrc.putIfAbsent(appIdStr,
              tracker);
          if (oldTracker != null) {
            tracker = oldTracker;
          }
          recoverTrackerResources(tracker, trackerState);
        }
      }
    }
  }

  private void recoverTrackerResources(LocalResourcesTracker tracker,
      LocalResourceTrackerState state) throws URISyntaxException {
    for (LocalizedResourceProto proto : state.getLocalizedResources()) {
      LocalResource rsrc = new LocalResourcePBImpl(proto.getResource());
      LocalResourceRequest req = new LocalResourceRequest(rsrc);
      LOG.info("Recovering localized resource " + req + " at "
          + proto.getLocalPath());
      tracker.handle(new ResourceRecoveredEvent(req,
          new Path(proto.getLocalPath()), proto.getSize()));
    }

    for (Map.Entry<LocalResourceProto, Path> entry :
         state.getInProgressResources().entrySet()) {
      LocalResource rsrc = new LocalResourcePBImpl(entry.getKey());
      LocalResourceRequest req = new LocalResourceRequest(rsrc);
      Path localPath = entry.getValue();
      tracker.handle(new ResourceRecoveredEvent(req, localPath, 0));

      // delete any in-progress localizations, containers will request again
      LOG.info("Deleting in-progress localization for " + req + " at "
          + localPath);
      tracker.remove(tracker.getLocalizedResource(req), delService);
    }

    // TODO: remove untracked directories in local filesystem
  }

  /**
   * 处理该容器发过来的心跳信息
   */
  @Override
  public LocalizerHeartbeatResponse heartbeat(LocalizerStatus status) {
    return localizerTracker.processHeartbeat(status);
  }

  @Override
  public void serviceStart() throws Exception {
	  //定期执行清理内存空间线程
    cacheCleanup.scheduleWithFixedDelay(new CacheCleanup(dispatcher),
        cacheCleanupPeriod, cacheCleanupPeriod, TimeUnit.MILLISECONDS);
    
    //创建LocalizationProtocol服务
    server = createServer();
    server.start();
    
    localizationServerAddress =
        getConfig().updateConnectAddr(YarnConfiguration.NM_BIND_HOST,
                                      YarnConfiguration.NM_LOCALIZER_ADDRESS,
                                      YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
                                      server.getListenerAddress());
    LOG.info("Localizer started on port " + server.getPort());
    super.serviceStart();
  }

  LocalizerTracker createLocalizerTracker(Configuration conf) {
    return new LocalizerTracker(conf);
  }

  Server createServer() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      secretManager = new LocalizerTokenSecretManager();      
    }
    
    Server server = rpc.getServer(LocalizationProtocol.class, this,
        localizationServerAddress, conf, secretManager, 
        conf.getInt(YarnConfiguration.NM_LOCALIZER_CLIENT_THREAD_COUNT, 
            YarnConfiguration.DEFAULT_NM_LOCALIZER_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      server.refreshServiceAcl(conf, new NMPolicyProvider());
    }
    
    return server;
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    cacheCleanup.shutdown();
    super.serviceStop();
  }

  /**
   * 对资源的初始化事件的处理
   */
  @Override
  public void handle(LocalizationEvent event) {
    // TODO: create log dir as $logdir/$user/$appId
    switch (event.getType()) {
    case INIT_APPLICATION_RESOURCES://一个应用要在本地进行初始化
      handleInitApplicationResources(((ApplicationLocalizationEvent)event).getApplication());
      break;
    case INIT_CONTAINER_RESOURCES:
      handleInitContainerResources((ContainerLocalizationRequestEvent) event);
      break;
    case CACHE_CLEANUP://CacheCleanup线程周期性执行
      handleCacheCleanup(event);
      break;
    case CLEANUP_CONTAINER_RESOURCES:
      handleCleanupContainerResources((ContainerLocalizationCleanupEvent)event);
      break;
    case DESTROY_APPLICATION_RESOURCES:
      handleDestroyApplicationResources(((ApplicationLocalizationEvent)event).getApplication());
      break;
    default:
      throw new YarnRuntimeException("Unknown localization event: " + event);
    }
  }
  
  /**
   * Handle event received the first time any container is scheduled by a given application.
   * 该事件表示一个应用第一次接收到任何容器的时候会触发该事件
   * 一个应用要在本地进行初始化
   */
  @SuppressWarnings("unchecked")
  private void handleInitApplicationResources(Application app) {
    // 0) Create application tracking structs
    String userName = app.getUser();
    //私有的资源下载器,每一个userName对应一个该下载器
    privateRsrc.putIfAbsent(userName, new LocalResourcesTrackerImpl(userName,null, dispatcher, true, super.getConfig(), stateStore));
    
    //应用的资源下载器,每一个应用对应一个该下载器
    String appIdStr = ConverterUtils.toString(app.getAppId());
    appRsrc.putIfAbsent(appIdStr, new LocalResourcesTrackerImpl(app.getUser(),app.getAppId(), dispatcher, false, super.getConfig(), stateStore));
    // 1) Signal container init
    //
    // This is handled by the ApplicationImpl state machine and allows
    // containers to proceed with launching.
    //发送事件,表示已经完成应用的初始化任务
    dispatcher.getEventHandler().handle(new ApplicationInitedEvent(app.getAppId()));
  }
  
  /**
   * For each of the requested resources for a container, determines the
   * appropriate {@link LocalResourcesTracker} and forwards a 
   * {@link LocalResourceRequest} to that tracker.
   * 循环该容器需要的每一个资源请求
   * 
   * 发送下载资源请求即可
   */
  private void handleInitContainerResources(ContainerLocalizationRequestEvent rsrcReqs) {
    Container c = rsrcReqs.getContainer();
    // create a loading cache for the file statuses
    /**
     * Future模式获取该path路径对应的FileStatus对象
     * 调用方法举例:
     * CacheLoader aa = createStatusCacheLoader(conf);
     * aa.load(path).get()
     */
    LoadingCache<Path,Future<FileStatus>> statCache = CacheBuilder.newBuilder().build(FSDownload.createStatusCacheLoader(getConfig()));
    
    //为该容器创建LocalizerContext对象
    LocalizerContext ctxt = new LocalizerContext(c.getUser(), c.getContainerId(), c.getCredentials(), statCache);
    
    //该容器所需要的资源,已经按照可见性分组了
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs = rsrcReqs.getRequestedResources();
      
    for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> e : rsrcs.entrySet()) {
      //获取该可见性的LocalResourcesTracker对象,可见性可以根据是否是public以及user、application来区分
      LocalResourcesTracker tracker = getLocalResourcesTracker(e.getKey(), c.getUser(),
              c.getContainerId().getApplicationAttemptId()
                  .getApplicationId());
      for (LocalResourceRequest req : e.getValue()) {//发送下载资源请求
        tracker.handle(new ResourceRequestEvent(req, e.getKey(), ctxt));
      }
    }
  }

  /**
   * 周期的执行该方法,进行清理缓存目录
   * 
   * 清理公共空间和user私人空间,应用级别的空间不会被清理
   */
  private void handleCacheCleanup(LocalizationEvent event) {
    ResourceRetentionSet retain = new ResourceRetentionSet(delService, cacheTargetSize);
    retain.addResources(publicRsrc);
    LOG.debug("Resource cleanup (public) " + retain);
    for (LocalResourcesTracker t : privateRsrc.values()) {
      retain.addResources(t);
      LOG.debug("Resource cleanup " + t.getUser() + ":" + retain);
    }
    //TODO Check if appRsrcs should also be added to the retention set.
  }


  /**
   * 清理该容器下的所有资源
   */
  @SuppressWarnings("unchecked")
  private void handleCleanupContainerResources(ContainerLocalizationCleanupEvent rsrcCleanup) {
    Container c = rsrcCleanup.getContainer();
    //该容器下的所有资源,即每个可见性下面有资源集合
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs = rsrcCleanup.getResources();
      
    for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> e : rsrcs.entrySet()) {
      //查询该资源所在的跟踪器
      LocalResourcesTracker tracker = getLocalResourcesTracker(e.getKey(), c.getUser(), 
          c.getContainerId().getApplicationAttemptId()
          .getApplicationId());
      for (LocalResourceRequest req : e.getValue()) {//获取该容器在该资源管理器中所以对应的等待下载的资源
    	  //发送释放该资源的事件
        tracker.handle(new ResourceReleaseEvent(req,c.getContainerId()));
      }
    }
    
    String locId = ConverterUtils.toString(c.getContainerId());
    localizerTracker.cleanupPrivLocalizers(locId);
    
    // Delete the container directories
    String userName = c.getUser();
    String containerIDStr = c.toString();
    String appIDStr = ConverterUtils.toString(c.getContainerId().getApplicationAttemptId().getApplicationId());
    
    // Try deleting from good local dirs and full local dirs because a dir might
    // have gone bad while the app was running(disk full). In addition
    // a dir might have become good while the app was running.
    // Check if the container dir exists and if it does, try to delete it

    /**
     * 1.删除该容器目录,即/localDir/usercache/user/appcache/appId/containerID
     * 2.删除该容器所对应的私有文件夹,--该appId--该容器Id对应的文件夹内容,localDir/nmPrivate/appID/containerID
     */
    for (String localDir : dirsHandler.getLocalDirsForCleanup()) {
      // Delete the user-owned container-dir
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);//localDir/usercache
      Path userdir = new Path(usersdir, userName);//localDir/usercache/user
      Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);//localDir/usercache/user/appcache
      Path appDir = new Path(allAppsdir, appIDStr);//localDir/usercache/user/appcache/appId
      Path containerDir = new Path(appDir, containerIDStr);//localDir/usercache/user/appcache/appId/containerID
      
      //删除属于该user--该appId--该容器Id对应的文件夹内容
      submitDirForDeletion(userName, containerDir);//删除该容器目录,即/localDir/usercache/user/appcache/appId/containerID

      // Delete the nmPrivate container-dir

      Path sysDir = new Path(localDir, NM_PRIVATE_DIR);//localDir/nmPrivate
      Path appSysDir = new Path(sysDir, appIDStr);//localDir/nmPrivate/appID
      Path containerSysDir = new Path(appSysDir, containerIDStr);//localDir/nmPrivate/appID/containerID
      //删除该容器所对应的私有文件夹,--该appId--该容器Id对应的文件夹内容
      submitDirForDeletion(null, containerSysDir);//删除该容器的私有目录
    }

    //发送容器资源已经清理完成
    dispatcher.getEventHandler().handle(
        new ContainerEvent(c.getContainerId(),
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
  }
  
  /**
   * 删除目录
   */
  private void submitDirForDeletion(String userName, Path dir) {
    try {
      lfs.getFileStatus(dir);
      delService.delete(userName, dir, new Path[] {});
    } catch (UnsupportedFileSystemException ue) {
      LOG.warn("Local dir " + dir + " is an unsupported filesystem", ue);
    } catch (IOException ie) {
      // ignore
      return;
    }
  }


  /**
   * 当一个应用彻底完成后,调用该函数
   */
  @SuppressWarnings({"unchecked"})
  private void handleDestroyApplicationResources(Application application) {
    String userName = application.getUser();
    ApplicationId appId = application.getAppId();
    String appIDStr = application.toString();
    
    //查看app级别的资源管理器
    LocalResourcesTracker appLocalRsrcsTracker =
      appRsrc.remove(ConverterUtils.toString(appId));
    if (appLocalRsrcsTracker != null) {
      for (LocalizedResource rsrc : appLocalRsrcsTracker ) {//循环app级别的资源管理器下所有的资源
        Path localPath = rsrc.getLocalPath();
        if (localPath != null) {
          try {
        	  //记录日志
            stateStore.removeLocalizedResource(userName, appId, localPath);
          } catch (IOException e) {
            LOG.error("Unable to remove resource " + rsrc + " for " + appIDStr
                + " from state store", e);
          }
        }
      }
    } else {
      LOG.warn("Removing uninitialized application " + application);
    }

    // Delete the application directories
    userName = application.getUser();
    appIDStr = application.toString();

    for (String localDir : dirsHandler.getLocalDirsForCleanup()) {

      // Delete the user-owned app-dir
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, userName);
      Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(allAppsdir, appIDStr);//$localDir/usercache/$user/appcache/$appid
      //删除$localDir/usercache/$user/appcache/$appid下所有文件
      submitDirForDeletion(userName, appDir);

      // Delete the nmPrivate app-dir
      Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
      Path appSysDir = new Path(sysDir, appIDStr);//$localDir/nmPrivate/$appid
      //删除$localDir/nmPrivate/$appid下所有数据
      submitDirForDeletion(null, appSysDir);
    }

    // TODO: decrement reference counts of all resources associated with this
    // app

    dispatcher.getEventHandler().handle(new ApplicationEvent(
          application.getAppId(),
          ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
  }


  /**
   * 查询该资源所在的跟踪器
   */
  LocalResourcesTracker getLocalResourcesTracker(LocalResourceVisibility visibility, String user, ApplicationId appId) {
    switch (visibility) {
      default:
      case PUBLIC:
        return publicRsrc;
      case PRIVATE:
        return privateRsrc.get(user);//获取该user的资源下载器
      case APPLICATION:
        return appRsrc.get(ConverterUtils.toString(appId));//获取该应用的资源下载器
    }
  }

  //./usercache/user/filecache
  private String getUserFileCachePath(String user) {
    return StringUtils.join(Path.SEPARATOR, Arrays.asList(".",
      ContainerLocalizer.USERCACHE, user, ContainerLocalizer.FILECACHE));

  }

  //.usercache/user/appcache/appId/filecache
  private String getAppFileCachePath(String user, String appId) {
    return StringUtils.join(Path.SEPARATOR, Arrays.asList(".",
        ContainerLocalizer.USERCACHE, user, ContainerLocalizer.APPCACHE, appId,
        ContainerLocalizer.FILECACHE));
  }
  
  @VisibleForTesting
  @Private
  public PublicLocalizer getPublicLocalizer() {
    return localizerTracker.publicLocalizer;
  }

  @VisibleForTesting
  @Private
  public LocalizerRunner getLocalizerRunner(String locId) {
    return localizerTracker.privLocalizers.get(locId);
  }
  
  @VisibleForTesting
  @Private
  public Map<String, LocalizerRunner> getPrivateLocalizers() {
    return localizerTracker.privLocalizers;
  }
  
  /**
   * Sub-component handling the spawning of {@link ContainerLocalizer}
   */
  class LocalizerTracker extends AbstractService implements EventHandler<LocalizerEvent>  {

    private final PublicLocalizer publicLocalizer;
    //每一个loc,对应一个LocalizerRunner对象,对应的包含PRIVATE、APPLICATION两大私有类
    private final Map<String,LocalizerRunner> privLocalizers;

    LocalizerTracker(Configuration conf) {
      this(conf, new HashMap<String,LocalizerRunner>());
    }

    LocalizerTracker(Configuration conf,Map<String,LocalizerRunner> privLocalizers) {
      super(LocalizerTracker.class.getName());
      this.publicLocalizer = new PublicLocalizer(conf);
      this.privLocalizers = privLocalizers;
    }
    
    @Override
    public synchronized void serviceStart() throws Exception {
      publicLocalizer.start();
      super.serviceStart();
    }

    /**
     * 处理该容器发过来的心跳信息
     */
    public LocalizerHeartbeatResponse processHeartbeat(LocalizerStatus status) {
      String locId = status.getLocalizerId();
      synchronized (privLocalizers) {
        LocalizerRunner localizer = privLocalizers.get(locId);//找到对应的容器下载类
        if (null == localizer) {
          // TODO process resources anyway
          LOG.info("Unknown localizer with localizerId " + locId
              + " is sending heartbeat. Ordering it to DIE");
          LocalizerHeartbeatResponse response = recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
          response.setLocalizerAction(LocalizerAction.DIE);
          return response;
        }
        return localizer.update(status.getResources());
      }
    }
    
    @Override
    public void serviceStop() throws Exception {
      for (LocalizerRunner localizer : privLocalizers.values()) {
        localizer.interrupt();
      }
      publicLocalizer.interrupt();
      super.serviceStop();
    }

    /**
     * 当容器需要被下载资源的时候触发该函数
     */
    @Override
    public void handle(LocalizerEvent event) {
      String locId = event.getLocalizerId();
      switch (event.getType()) {
      case REQUEST_RESOURCE_LOCALIZATION:
        // 0) find running localizer or start new thread
        LocalizerResourceRequestEvent req = (LocalizerResourceRequestEvent)event;
        switch (req.getVisibility()) {
        case PUBLIC:
          publicLocalizer.addResource(req);
          break;
        case PRIVATE:
        case APPLICATION:
          synchronized (privLocalizers) {
            LocalizerRunner localizer = privLocalizers.get(locId);
            if (null == localizer) {
              LOG.info("Created localizer for " + locId);
              localizer = new LocalizerRunner(req.getContext(), locId);
              privLocalizers.put(locId, localizer);
              localizer.start();
            }
            // 1) propagate event
            localizer.addResource(req);
          }
          break;
        }
        break;
      }
    }

    /**
     * 清理该locId对应的私有本地化信息
     */
    public void cleanupPrivLocalizers(String locId) {
      synchronized (privLocalizers) {
        LocalizerRunner localizer = privLocalizers.get(locId);
        if (null == localizer) {
          return; // ignore; already gone
        }
        privLocalizers.remove(locId);
        localizer.interrupt();
      }
    }
  }
  

  private static ExecutorService createLocalizerExecutor(Configuration conf) {
    int nThreads = conf.getInt(
        YarnConfiguration.NM_LOCALIZER_FETCH_THREAD_COUNT,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_FETCH_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("PublicLocalizer #%d")
      .build();
    return Executors.newFixedThreadPool(nThreads, tf);
  }

/**
 * 公共类去循环下载文件 
 * 负责下载文件
 */
  class PublicLocalizer extends Thread {

    final FileContext lfs;
    final Configuration conf;
    final ExecutorService threadPool;
    final CompletionService<Path> queue;//该对象返回path,真正去执行下载的多线程任务,一旦该队列Future返回path,则说明该文件已经下载完成,返回下载完成后的本地路径
    // Its shared between public localizer and dispatcher thread.
    //正在等待或者下载中的文件,key是最终下载完成的本地路径,value是待下载资源
    final Map<Future<Path>,LocalizerResourceRequestEvent> pending;

    PublicLocalizer(Configuration conf) {
      super("Public Localizer");
      this.lfs = getLocalFileContext(conf);
      this.conf = conf;
      this.pending = Collections.synchronizedMap(new HashMap<Future<Path>, LocalizerResourceRequestEvent>());
      this.threadPool = createLocalizerExecutor(conf);
      this.queue = new ExecutorCompletionService<Path>(threadPool);
    }

    /**
     * 添加一个待下载任务,并且去真正的下载该文件
     */
    public void addResource(LocalizerResourceRequestEvent request) {
      // TODO handle failures, cancellation, requests by other containers
      LocalizedResource rsrc = request.getResource();
      LocalResourceRequest key = rsrc.getRequest();//待下载的原始文件
      LOG.info("Downloading public rsrc:" + key);
      /*
       * Here multiple containers may request the same resource. So we need
       * to start downloading only when
       * 1) ResourceState == DOWNLOADING
       * 2) We are able to acquire non blocking semaphore lock.
       * If not we will skip this resource as either it is getting downloaded
       * or it FAILED / LOCALIZED.
       */
      if (rsrc.tryAcquire()) {
        if (rsrc.getState().equals(ResourceState.DOWNLOADING)) {
          LocalResource resource = request.getResource().getRequest();//待下载的原始文件
          try {
        	  //申请size大小的资源空间,让下载后的资源存放进去
            Path publicRootPath =
                dirsHandler.getLocalPathForWrite("." + Path.SEPARATOR
                    + ContainerLocalizer.FILECACHE,
                  ContainerLocalizer.getEstimatedSize(resource), true);//./filecache/目录下找到一定大小可以写的目录
            
            //为res参数设置本地存储路径
            Path publicDirDestPath = publicRsrc.getPathForLocalization(key, publicRootPath);
            if (!publicDirDestPath.getParent().equals(publicRootPath)) {
              DiskChecker.checkDir(new File(publicDirDestPath.toUri().getPath()));
            }
            // explicitly synchronize pending here to avoid future task
            // completing and being dequeued before pending updated
            //真正去执行下载资源任务,返回该资源最后的本地路径
            synchronized (pending) {
              pending.put(queue.submit(new FSDownload(lfs, null, conf,
                  publicDirDestPath, resource, request.getContext().getStatCache())),
                  request);
            }
          } catch (IOException e) {
            rsrc.unlock();
            publicRsrc.handle(new ResourceFailedLocalizationEvent(request
              .getResource().getRequest(), e.getMessage()));
            LOG.error("Local path for public localization is not found. "
                + " May be disks failed.", e);
          } catch (RejectedExecutionException re) {
            rsrc.unlock();
            publicRsrc.handle(new ResourceFailedLocalizationEvent(request
              .getResource().getRequest(), re.getMessage()));
            LOG.error("Failed to submit rsrc " + rsrc + " for download."
                + " Either queue is full or threadpool is shutdown.", re);
          }
        } else {
          rsrc.unlock();
        }
      }
    }

    /**
     * 轮训下载资源,一旦 queue.take();返回值,说明已经下载完成
     * 当下载完成后,线程退出,因此触发文件下载完成事件
     */
    @Override
    public void run() {
      try {
        // TODO shutdown, better error handling esp. DU
        while (!Thread.currentThread().isInterrupted()) {
          try {
            //说明已经下载完成,该方法是阻塞的
            Future<Path> completed = queue.take();
            //下载完成,则从等待下载队列中删除该文件
            LocalizerResourceRequestEvent assoc = pending.remove(completed);
            try {
              //获取已经下载完成的,存储在本地的文件路径
              Path local = completed.get();
              if (null == assoc) {
                LOG.error("Localized unknown resource to " + completed);
                // TODO delete
                return;
              }
              //等待下载信息
              LocalResourceRequest key = assoc.getResource().getRequest();
              //触发下载完成事件,等待下载的资源、下载完成后的本地路径,本地所占用磁盘大小
              publicRsrc.handle(new ResourceLocalizedEvent(key, local, FileUtil.getDU(new File(local.toUri()))));
              assoc.getResource().unlock();
            } catch (ExecutionException e) {
              LOG.info("Failed to download resource " + assoc.getResource(),e.getCause());
              LocalResourceRequest req = assoc.getResource().getRequest();
              publicRsrc.handle(new ResourceFailedLocalizationEvent(req,e.getMessage()));
              assoc.getResource().unlock();
            } catch (CancellationException e) {
              // ignore; shutting down
            }
          } catch (InterruptedException e) {
            return;
          }
        }
      } catch(Throwable t) {
        LOG.fatal("Error: Shutting down", t);
      } finally {
        LOG.info("Public cache exiting");
        threadPool.shutdownNow();
      }
    }

  }

  /**
   * Runs the {@link ContainerLocalizer} itself in a separate process with
   * access to user's credentials. One {@link LocalizerRunner} per localizerId.
   * 私有资源下载服务
   * 每一个user或者app应用都持有一个该对象
   */
  class LocalizerRunner extends Thread {

    final LocalizerContext context;
    final String localizerId;
    //key待下载资源,value待下载资源事件,已经进入调度的资源下载集合
    final Map<LocalResourceRequest,LocalizerResourceRequestEvent> scheduled;
    // Its a shared list between Private Localizer and dispatcher thread.
    final List<LocalizerResourceRequestEvent> pending;//等待下载的资源事件,还没有进入下载调度队列中

    // TODO: threadsafe, use outer?
    private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(getConfig());

    LocalizerRunner(LocalizerContext context, String localizerId) {
      super("LocalizerRunner for " + localizerId);
      this.context = context;
      this.localizerId = localizerId;
      this.pending = Collections.synchronizedList(new ArrayList<LocalizerResourceRequestEvent>());
      this.scheduled = new HashMap<LocalResourceRequest, LocalizerResourceRequestEvent>();
    }

    public void addResource(LocalizerResourceRequestEvent request) {
      pending.add(request);
    }

    /**
     * Find next resource to be given to a spawned localizer.
     * 
     * @return the next resource to be localized
     * 返回下一个要下载的资源
     * 在等待队列中遍历寻找
     */
    private LocalResource findNextResource() {
      synchronized (pending) {
        for (Iterator<LocalizerResourceRequestEvent> i = pending.iterator();i.hasNext();) {
         LocalizerResourceRequestEvent evt = i.next();//待下载资源事件
         LocalizedResource nRsrc = evt.getResource();//本地资源
         // Resource download should take place ONLY if resource is in
         // Downloading state
         if (!ResourceState.DOWNLOADING.equals(nRsrc.getState())) {
           i.remove();
           continue;
         }
         /*
          * Multiple containers will try to download the same resource. So the
          * resource download should start only if
          * 1) We can acquire a non blocking semaphore lock on resource
          * 2) Resource is still in DOWNLOADING state
          */
         if (nRsrc.tryAcquire()) {
           if (nRsrc.getState().equals(ResourceState.DOWNLOADING)) {
             LocalResourceRequest nextRsrc = nRsrc.getRequest();
             //下一个要下载的资源
             LocalResource next = recordFactory.newRecordInstance(LocalResource.class);
             next.setResource(ConverterUtils.getYarnUrlFromPath(nextRsrc.getPath()));
             next.setTimestamp(nextRsrc.getTimestamp());
             next.setType(nextRsrc.getType());
             next.setVisibility(evt.getVisibility());
             next.setPattern(evt.getPattern());
             scheduled.put(nextRsrc, evt);
             return next;
           } else {
             // Need to release acquired lock
             nRsrc.unlock();
           }
         }
       }
       return null;
      }
    }

    /**
     * 处理该容器发过来的心跳信息
     * 此时容器发过来该容器已经下载的资源信息
     */
    LocalizerHeartbeatResponse update(List<LocalResourceStatus> remoteResourceStatuses) {
        
      LocalizerHeartbeatResponse response = recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);

      String user = context.getUser();
      ApplicationId applicationId = context.getContainerId().getApplicationAttemptId().getApplicationId();
      // The localizer has just spawned. Start giving it resources for
      // remote-fetching.
      if (remoteResourceStatuses.isEmpty()) {
        LocalResource next = findNextResource();
        if (next != null) {
          response.setLocalizerAction(LocalizerAction.LIVE);
          try {
            ArrayList<ResourceLocalizationSpec> rsrcs = new ArrayList<ResourceLocalizationSpec>();
            //在以上两个目录中寻找一个空间用于存放下载的文件,存储私有资源下载到本地的对象信息
            ResourceLocalizationSpec rsrc = NodeManagerBuilderUtils.newResourceLocalizationSpec(next,getPathForLocalization(next)); 
            rsrcs.add(rsrc);
            response.setResourceSpecs(rsrcs);
          } catch (IOException e) {
            LOG.error("local path for PRIVATE localization could not be found."
                + "Disks might have failed.", e);
          } catch (URISyntaxException e) {
            // TODO fail? Already translated several times...
          }
        } else if (pending.isEmpty()) {
          // TODO: Synchronization
          response.setLocalizerAction(LocalizerAction.DIE);
        } else {
          response.setLocalizerAction(LocalizerAction.LIVE);
        }
        return response;
      }
      ArrayList<ResourceLocalizationSpec> rsrcs = new ArrayList<ResourceLocalizationSpec>();
       /*
        * TODO : It doesn't support multiple downloads per ContainerLocalizer
        * at the same time. We need to think whether we should support this.
        * 循环组装等待下载的资源
        */
      for (LocalResourceStatus stat : remoteResourceStatuses) {
        LocalResource rsrc = stat.getResource();//等待下载的资源
        LocalResourceRequest req = null;//等待下载的资源请求
        try {
          req = new LocalResourceRequest(rsrc);
        } catch (URISyntaxException e) {
          // TODO fail? Already translated several times...
        }
        //如果该资源不存在,则说明有问题,返回即可
        LocalizerResourceRequestEvent assoc = scheduled.get(req);
        if (assoc == null) {
          // internal error
          LOG.error("Unknown resource reported: " + req);
          continue;
        }
        //如果报告的资源已经完成抓取
        switch (stat.getStatus()) {
          case FETCH_SUCCESS:
            // notify resource
            try {
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceLocalizedEvent(req, ConverterUtils.getPathFromYarnURL(stat.getLocalPath()), stat.getLocalSize()));
            } catch (URISyntaxException e) { }

            // unlocking the resource and removing it from scheduled resource
            // list
            assoc.getResource().unlock();
            scheduled.remove(req);
            
            if (pending.isEmpty()) {
              // TODO: Synchronization
              response.setLocalizerAction(LocalizerAction.DIE);
              break;
            }
            response.setLocalizerAction(LocalizerAction.LIVE);
            LocalResource next = findNextResource();
            if (next != null) {
              try {
                ResourceLocalizationSpec resource =
                    NodeManagerBuilderUtils.newResourceLocalizationSpec(next,
                      getPathForLocalization(next));
                rsrcs.add(resource);
              } catch (IOException e) {
                LOG.error("local path for PRIVATE localization could not be " +
                  "found. Disks might have failed.", e);
              } catch (URISyntaxException e) {
                  //TODO fail? Already translated several times...
              }
            }
            break;
          case FETCH_PENDING:
            response.setLocalizerAction(LocalizerAction.LIVE);
            break;
          case FETCH_FAILURE:
            final String diagnostics = stat.getException().toString();
            LOG.warn(req + " failed: " + diagnostics);
            response.setLocalizerAction(LocalizerAction.DIE);
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceFailedLocalizationEvent(
                  req, diagnostics));

            // unlocking the resource and removing it from scheduled resource
            // list
            assoc.getResource().unlock();
            scheduled.remove(req);
            
            break;
          default:
            LOG.info("Unknown status: " + stat.getStatus());
            response.setLocalizerAction(LocalizerAction.DIE);
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceFailedLocalizationEvent(
                  req, stat.getException().getMessage()));
            break;
        }
      }
      response.setResourceSpecs(rsrcs);
      return response;
    }

    /**
     * .usercache/user/filecache
     * .usercache/user/appcache/appId/filecache
     * 在以上两个目录中寻找一个空间用于存放下载的文件
     */
    private Path getPathForLocalization(LocalResource rsrc) throws IOException,URISyntaxException {
      String user = context.getUser();
      ApplicationId appId = context.getContainerId().getApplicationAttemptId().getApplicationId();
      LocalResourceVisibility vis = rsrc.getVisibility();
      LocalResourcesTracker tracker = getLocalResourcesTracker(vis, user, appId);
      String cacheDirectory = null;
      if (vis == LocalResourceVisibility.PRIVATE) {// PRIVATE Only .usercache/user/filecache
        cacheDirectory = getUserFileCachePath(user);
      } else {// APPLICATION ONLY .usercache/user/appcache/appId/filecache
        cacheDirectory = getAppFileCachePath(user, appId.toString());
      }
      
      //找到能写入该文件大小的文件夹
      Path dirPath =
          dirsHandler.getLocalPathForWrite(cacheDirectory,
            ContainerLocalizer.getEstimatedSize(rsrc), false);
      return tracker.getPathForLocalization(new LocalResourceRequest(rsrc),dirPath);
    }

    @Override
    @SuppressWarnings("unchecked") // dispatcher not typed
    public void run() {
      Path nmPrivateCTokensPath = null;
      try {
        // Get nmPrivateDir 路径:nmPrivate/localizerId.tokens
        nmPrivateCTokensPath =
          dirsHandler.getLocalPathForWrite(
                NM_PRIVATE_DIR + Path.SEPARATOR
                    + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,
                        localizerId));

        // 0) init queue, etc.
        // 1) write credentials to private dir 向nmPrivate/localizerId.tokens文件写入creden信息
        writeCredentials(nmPrivateCTokensPath);
        // 2) exec initApplication and wait
        if (dirsHandler.areDisksHealthy()) {
          exec.startLocalizer(nmPrivateCTokensPath, localizationServerAddress,
              context.getUser(),
              ConverterUtils.toString(
                  context.getContainerId().
                  getApplicationAttemptId().getApplicationId()),
              localizerId,
              dirsHandler);
        } else {
          throw new IOException("All disks failed. "
              + dirsHandler.getDisksHealthReport(false));
        }
      // TODO handle ExitCodeException separately?
      } catch (Exception e) {
        LOG.info("Localizer failed", e);
        // 3) on error, report failure to Container and signal ABORT
        // 3.1) notify resource of failed localization
        ContainerId cId = context.getContainerId();
        dispatcher.getEventHandler().handle(new ContainerResourceFailedEvent(cId, null, e.getMessage()));
      } finally {
        for (LocalizerResourceRequestEvent event : scheduled.values()) {
          event.getResource().unlock();
        }
        delService.delete(null, nmPrivateCTokensPath, new Path[] {});
      }
    }

    private Credentials getSystemCredentialsSentFromRM(
        LocalizerContext localizerContext) throws IOException {
      ApplicationId appId =
          localizerContext.getContainerId().getApplicationAttemptId()
            .getApplicationId();
      Credentials systemCredentials =
          nmContext.getSystemCredentialsForApps().get(appId);
      if (systemCredentials == null) {
        return null;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding new framework-token for " + appId
            + " for localization: " + systemCredentials.getAllTokens());
      }
      return systemCredentials;
    }
    
    /**
     * 写入credien信息
     */
    private void writeCredentials(Path nmPrivateCTokensPath)
        throws IOException {
      DataOutputStream tokenOut = null;
      try {
        Credentials credentials = context.getCredentials();
        if (UserGroupInformation.isSecurityEnabled()) {
          Credentials systemCredentials = getSystemCredentialsSentFromRM(context);
          if (systemCredentials != null) {
            credentials = systemCredentials;
          }
        }

        FileContext lfs = getLocalFileContext(getConfig());
        tokenOut = lfs.create(nmPrivateCTokensPath, EnumSet.of(CREATE, OVERWRITE));
        LOG.info("Writing credentials to the nmPrivate file "
            + nmPrivateCTokensPath.toString() + ". Credentials list: ");
        if (LOG.isDebugEnabled()) {
          for (Token<? extends TokenIdentifier> tk : credentials
              .getAllTokens()) {
            LOG.debug(tk.getService() + " : " + tk.encodeToUrlString());
          }
        }
        if (UserGroupInformation.isSecurityEnabled()) {
          credentials = new Credentials(credentials);
          LocalizerTokenIdentifier id = secretManager.createIdentifier();
          Token<LocalizerTokenIdentifier> localizerToken =
              new Token<LocalizerTokenIdentifier>(id, secretManager);
          credentials.addToken(id.getKind(), localizerToken);
        }
        credentials.writeTokenStorageToStream(tokenOut);
      } finally {
        if (tokenOut != null) {
          tokenOut.close();
        }
      }
    }

  }

  /**
   * cacheCleanupPeriod周期内,定期清理内存空间
   */
  static class CacheCleanup extends Thread {

    private final Dispatcher dispatcher;

    public CacheCleanup(Dispatcher dispatcher) {
      super("CacheCleanup");
      this.dispatcher = dispatcher;
    }

    @Override
    @SuppressWarnings("unchecked") // dispatcher not typed
    public void run() {
      dispatcher.getEventHandler().handle(new LocalizationEvent(LocalizationEventType.CACHE_CLEANUP));
    }

  }

  /**
   * 初始化本地目录
   * 创建以下三个目录,并且赋予一定权限
   * localDir/usercache
   * localDir/filecache
   * localDir/nmPrivate
   */
  private void initializeLocalDirs(FileContext lfs) {
    List<String> localDirs = dirsHandler.getLocalDirs();
    for (String localDir : localDirs) {
      initializeLocalDir(lfs, localDir);
    }
  }

  /**
   * 创建以下三个目录,并且赋予一定权限
   * localDir/usercache
   * localDir/filecache
   * localDir/nmPrivate
   * @param lfs
   * @param localDir
   */
  private void initializeLocalDir(FileContext lfs, String localDir) {

    /**
     * 为每一个目录设置权限,并且返回
     * localDir/usercache FsPermission
     * localDir/filecache FsPermission
     * localDir/nmPrivate FsPermission
     */
    Map<Path, FsPermission> pathPermissionMap = getLocalDirsPathPermissionsMap(localDir);
    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      try {
        status = lfs.getFileStatus(entry.getKey());
      }
      catch(FileNotFoundException fs) {
        status = null;
      }
      catch(IOException ie) {
        String msg = "Could not get file status for local dir " + entry.getKey();
        LOG.warn(msg, ie);
        throw new YarnRuntimeException(msg, ie);
      }
      
      //如果目录不存在,则创建该目录
      if(status == null) {
        try {
          lfs.mkdir(entry.getKey(), entry.getValue(), true);
          status = lfs.getFileStatus(entry.getKey());
        } catch (IOException e) {
          String msg = "Could not initialize local dir " + entry.getKey();
          LOG.warn(msg, e);
          throw new YarnRuntimeException(msg, e);
        }
      }
      FsPermission perms = status.getPermission();
      if(!perms.equals(entry.getValue())) {
        try {
          lfs.setPermission(entry.getKey(), entry.getValue());
        }
        catch(IOException ie) {
          String msg = "Could not set permissions for local dir " + entry.getKey();
          LOG.warn(msg, ie);
          throw new YarnRuntimeException(msg, ie);
        }
      }
    }
  }

  /**
   * 创建该日志目录集合 
   */
  private void initializeLogDirs(FileContext lfs) {
    List<String> logDirs = dirsHandler.getLogDirs();
    for (String logDir : logDirs) {
      initializeLogDir(lfs, logDir);
    }
  }

  /**
   * 创建该logDir目录
   */
  private void initializeLogDir(FileContext lfs, String logDir) {
    try {
      lfs.mkdir(new Path(logDir), null, true);
    } catch (FileAlreadyExistsException fe) {
      // do nothing
    } catch (IOException e) {
      String msg = "Could not initialize log dir " + logDir;
      LOG.warn(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  /**
   * 删除localDir下文件,初始化的时候调用该方法
   */
  private void cleanUpLocalDirs(FileContext lfs, DeletionService del) {
    for (String localDir : dirsHandler.getLocalDirs()) {
      cleanUpLocalDir(lfs, del, localDir);
    }
  }

  /**
   * 删除localDir下以下文件
$localDir/usercache
$localDir/filecache
$localDir/nmPrivate

实现逻辑:
1.对文件进行改名字
$localDir/usercache_DEL_时间戳
$localDir/filecache_DEL_时间戳
$localDir/nmPrivate_DEL_时间戳
2.删除_DEL_.*文件

   */
  private void cleanUpLocalDir(FileContext lfs, DeletionService del,String localDir) {
    long currentTimeStamp = System.currentTimeMillis();
    renameLocalDir(lfs, localDir, ContainerLocalizer.USERCACHE,currentTimeStamp);
    renameLocalDir(lfs, localDir, ContainerLocalizer.FILECACHE,currentTimeStamp);
    renameLocalDir(lfs, localDir, ResourceLocalizationService.NM_PRIVATE_DIR,currentTimeStamp);
    try {
      deleteLocalDir(lfs, del, localDir);
    } catch (IOException e) {
      // Do nothing, just give the warning
      LOG.warn("Failed to delete localDir: " + localDir);
    }
  }

  /**
对文件进行改名字
$localDir/usercache_DEL_时间戳
$localDir/filecache_DEL_时间戳
$localDir/nmPrivate_DEL_时间戳
   */
  private void renameLocalDir(FileContext lfs, String localDir,String localSubDir, long currentTimeStamp) {
    try {
      lfs.rename(new Path(localDir, localSubDir), new Path(localDir, localSubDir + "_DEL_" + currentTimeStamp));
    } catch (FileNotFoundException ex) {
      // No need to handle this exception
      // localSubDir may not be exist
    } catch (Exception ex) {
      // Do nothing, just give the warning
      LOG.warn("Failed to rename the local file under " +
          localDir + "/" + localSubDir);
    }
  }

  /**
   * 删除_DEL_.*文件
   */
  private void deleteLocalDir(FileContext lfs, DeletionService del,String localDir) throws IOException {
    RemoteIterator<FileStatus> fileStatus = lfs.listStatus(new Path(localDir));
    if (fileStatus != null) {
      while (fileStatus.hasNext()) {
        FileStatus status = fileStatus.next();
        try {
          //.*usercache_DEL_.*
          if (status.getPath().getName().matches(".*" +
              ContainerLocalizer.USERCACHE + "_DEL_.*")) {
            LOG.info("usercache path : " + status.getPath().toString());
            cleanUpFilesPerUserDir(lfs, del, status.getPath());
          } else if (status.getPath().getName()
              .matches(".*" + NM_PRIVATE_DIR + "_DEL_.*")
              ||
              status.getPath().getName()
                  .matches(".*" + ContainerLocalizer.FILECACHE + "_DEL_.*")) {
            //.*nmPrivate_DEL_.* || .*filecache_DEL_.*
            del.delete(null, status.getPath(), new Path[] {});
          }
        } catch (IOException ex) {
          // Do nothing, just give the warning
          LOG.warn("Failed to delete this local Directory: " +
              status.getPath().getName());
        }
      }
    }
  }

  /**
   * 删除userDirPath下文件,该文件夹下仅仅删除跟当前用户有关的文件
   */
  private void cleanUpFilesPerUserDir(FileContext lfs, DeletionService del,Path userDirPath) throws IOException {
    RemoteIterator<FileStatus> userDirStatus = lfs.listStatus(userDirPath);//查找所有子目录
    //建立异步删除文件对象任务,采用绝对路径方式删除userDirPath下文件
    FileDeletionTask dependentDeletionTask = del.createFileDeletionTask(null, userDirPath, new Path[] {});
    
    if (userDirStatus != null && userDirStatus.hasNext()) {//有子目录,则查找指定user的目录,然后删除掉
      List<FileDeletionTask> deletionTasks = new ArrayList<FileDeletionTask>();//创建子任务
      while (userDirStatus.hasNext()) {//遍历所有子目录
        FileStatus status = userDirStatus.next();
        String owner = status.getOwner();//该文件的所有者
        FileDeletionTask deletionTask =
            del.createFileDeletionTask(owner, null,
              new Path[] { status.getPath() });
        deletionTask.addFileDeletionTaskDependency(dependentDeletionTask);//该语法在此处没任何意义
        deletionTasks.add(deletionTask);
      }
      for (FileDeletionTask task : deletionTasks) {
        del.scheduleFileDeletionTask(task);
      }
    } else {//没有子目录,则将userDirPath目录删除
      del.scheduleFileDeletionTask(dependentDeletionTask);
    }
  }
  
  /**
   * Synchronized method to get a list of initialized local dirs. Method will
   * check each local dir to ensure it has been setup correctly and will attempt
   * to fix any issues it finds.
   * 
   * @return list of initialized local dirs
   * 初始化本地目录
   */
  synchronized private List<String> getInitializedLocalDirs() {
    List<String> dirs = dirsHandler.getLocalDirs();
    List<String> checkFailedDirs = new ArrayList<String>();
    for (String dir : dirs) {
      try {
        checkLocalDir(dir);
      } catch (YarnRuntimeException e) {
        checkFailedDirs.add(dir);
      }
    }
    
    //失败了,重新创建目录在进行校验,如果还不行,则抛异常
    for (String dir : checkFailedDirs) {
      LOG.info("Attempting to initialize " + dir);
      initializeLocalDir(lfs, dir);
      try {
        checkLocalDir(dir);
      } catch (YarnRuntimeException e) {
        String msg = "Failed to setup local dir " + dir + ", which was marked as good.";
        LOG.warn(msg, e);
        throw new YarnRuntimeException(msg, e);
      }
    }
    return dirs;
  }

  /**
   * 校验每一个必须存在,并且权限也必须符合标准
   */
  private boolean checkLocalDir(String localDir) {
    /**
     * 为每一个目录设置权限,并且返回
     */
    Map<Path, FsPermission> pathPermissionMap = getLocalDirsPathPermissionsMap(localDir);

    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      try {
        status = lfs.getFileStatus(entry.getKey());
      } catch (Exception e) {
        String msg =
            "Could not carry out resource dir checks for " + localDir
                + ", which was marked as good";
        LOG.warn(msg, e);
        throw new YarnRuntimeException(msg, e);
      }

      if (!status.getPermission().equals(entry.getValue())) {
        String msg =
            "Permissions incorrectly set for dir " + entry.getKey()
                + ", should be " + entry.getValue() + ", actual value = "
                + status.getPermission();
        LOG.warn(msg);
        throw new YarnRuntimeException(msg);
      }
    }
    return true;
  }

  /**
   * 为每一个目录设置权限,并且返回
   * 为以下三个目录设置权限
   * localDir/usercache FsPermission
   * localDir/filecache FsPermission
   * localDir/nmPrivate FsPermission
   */
  private Map<Path, FsPermission> getLocalDirsPathPermissionsMap(String localDir) {
    Map<Path, FsPermission> localDirPathFsPermissionsMap = new HashMap<Path, FsPermission>();

    FsPermission defaultPermission = FsPermission.getDirDefault().applyUMask(lfs.getUMask());
    FsPermission nmPrivatePermission = NM_PRIVATE_PERM.applyUMask(lfs.getUMask());
        

    Path userDir = new Path(localDir, ContainerLocalizer.USERCACHE);//localDir/usercache
    Path fileDir = new Path(localDir, ContainerLocalizer.FILECACHE);//localDir/filecache
    Path sysDir = new Path(localDir, NM_PRIVATE_DIR);//localDir/nmPrivate

    localDirPathFsPermissionsMap.put(userDir, defaultPermission);
    localDirPathFsPermissionsMap.put(fileDir, defaultPermission);
    localDirPathFsPermissionsMap.put(sysDir, nmPrivatePermission);
    return localDirPathFsPermissionsMap;
  }
  
  /**
   * Synchronized method to get a list of initialized log dirs. Method will
   * check each local dir to ensure it has been setup correctly and will attempt
   * to fix any issues it finds.
   * 
   * @return list of initialized log dirs
   */
  synchronized private List<String> getInitializedLogDirs() {
    List<String> dirs = dirsHandler.getLogDirs();
    initializeLogDirs(lfs);
    return dirs;
  }
}

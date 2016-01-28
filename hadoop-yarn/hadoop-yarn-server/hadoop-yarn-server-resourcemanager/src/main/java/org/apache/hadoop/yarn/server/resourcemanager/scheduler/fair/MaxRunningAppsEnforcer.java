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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Handles tracking and enforcement for user and queue maxRunningApps
 * constraints
 * 跟踪和实施,为user和队列最大运行app数量的约束
 */
public class MaxRunningAppsEnforcer {
  private static final Log LOG = LogFactory.getLog(FairScheduler.class);
  
  private final FairScheduler scheduler;//核心调度器

  // Tracks the number of running applications by user.
  //每一个user运行了多少个应用
  private final Map<String, Integer> usersNumRunnableApps;
  
  //缓存每一个user对应的尚未运行的app尝试任务映射关系
  //注意key是String,value是List<FSAppAttempt>
  @VisibleForTesting
  final ListMultimap<String, FSAppAttempt> usersNonRunnableApps;

  public MaxRunningAppsEnforcer(FairScheduler scheduler) {
    this.scheduler = scheduler;
    this.usersNumRunnableApps = new HashMap<String, Integer>();
    this.usersNonRunnableApps = ArrayListMultimap.create();
  }

  /**
   * Checks whether making the application runnable would exceed any
   * maxRunningApps limits.
   * false表示不能运行
   * true表示可以运行
   */
  public boolean canAppBeRunnable(FSQueue queue, String user) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    Integer userNumRunnable = usersNumRunnableApps.get(user);//当前该user运行了多少个app
    if (userNumRunnable == null) {
      userNumRunnable = 0;
    }
    if (userNumRunnable >= allocConf.getUserMaxApps(user)) {//如果当前user运行的app数量超过了限制,返回false
      return false;
    }
    // Check queue and all parent queues
    //校验该队列以及该队列的所有父队列
    //也就是说虽然该队列允许,但是他的父队列已经满了,达到上限了,因此也是不能放下这个应用的
    while (queue != null) {
      int queueMaxApps = allocConf.getQueueMaxApps(queue.getName());//该队列允许最多运行多少个应用
      if (queue.getNumRunnableApps() >= queueMaxApps) {//该队列正常运行的已经超过了最大值,则返回false
        return false;
      }
      queue = queue.getParent();
    }

    return true;
  }

  /**
   * Tracks the given new runnable app for purposes of maintaining max running
   * app limits.
   * 跟踪运行的app与user的关系
   */
  public void trackRunnableApp(FSAppAttempt app) {
    String user = app.getUser();
    FSLeafQueue queue = app.getQueue();
    // Increment running counts for all parent queues
    //累加该队列以及父队列运行的app数量
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.incrementRunnableApps();
      parent = parent.getParent();
    }

    //添加该user对应的运行的app数量
    Integer userNumRunnable = usersNumRunnableApps.get(user);
    usersNumRunnableApps.put(user, (userNumRunnable == null ? 0
        : userNumRunnable) + 1);
  }

  /**
   * Tracks the given new non runnable app so that it can be made runnable when
   * it would not violate max running app limits.
   * 跟踪该尚未运行的app与user的关系
   */
  public void trackNonRunnableApp(FSAppAttempt app) {
    String user = app.getUser();
    usersNonRunnableApps.put(user, app);
  }

  /**
   * Checks to see whether any other applications runnable now that the given
   * application has been removed from the given queue.  And makes them so.
   * 
   * Runs in O(n log(n)) where n is the number of queues that are under the
   * highest queue that went from having no slack to having slack.
   */
  public void updateRunnabilityOnAppRemoval(FSAppAttempt app, FSLeafQueue queue) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    // childqueueX might have no pending apps itself, but if a queue higher up
    // in the hierarchy parentqueueY has a maxRunningApps set, an app completion
    // in childqueueX could allow an app in some other distant child of
    // parentqueueY to become runnable.
    // An app removal will only possibly allow another app to become runnable if
    // the queue was already at its max before the removal.
    // Thus we find the ancestor queue highest in the tree for which the app
    // that was at its maxRunningApps before the removal.
    FSQueue highestQueueWithAppsNowRunnable = (queue.getNumRunnableApps() ==
        allocConf.getQueueMaxApps(queue.getName()) - 1) ? queue : null;
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      if (parent.getNumRunnableApps() == allocConf.getQueueMaxApps(parent
          .getName()) - 1) {
        highestQueueWithAppsNowRunnable = parent;
      }
      parent = parent.getParent();
    }

    List<List<FSAppAttempt>> appsNowMaybeRunnable =
        new ArrayList<List<FSAppAttempt>>();

    // Compile lists of apps which may now be runnable
    // We gather lists instead of building a set of all non-runnable apps so
    // that this whole operation can be O(number of queues) instead of
    // O(number of apps)
    if (highestQueueWithAppsNowRunnable != null) {
      gatherPossiblyRunnableAppLists(highestQueueWithAppsNowRunnable,
          appsNowMaybeRunnable);
    }
    String user = app.getUser();
    Integer userNumRunning = usersNumRunnableApps.get(user);
    if (userNumRunning == null) {
      userNumRunning = 0;
    }
    if (userNumRunning == allocConf.getUserMaxApps(user) - 1) {
      List<FSAppAttempt> userWaitingApps = usersNonRunnableApps.get(user);
      if (userWaitingApps != null) {
        appsNowMaybeRunnable.add(userWaitingApps);
      }
    }

    // Scan through and check whether this means that any apps are now runnable
    Iterator<FSAppAttempt> iter = new MultiListStartTimeIterator(
        appsNowMaybeRunnable);
    FSAppAttempt prev = null;
    //不在等待的app集合
    List<FSAppAttempt> noLongerPendingApps = new ArrayList<FSAppAttempt>();
    while (iter.hasNext()) {
      FSAppAttempt next = iter.next();
      if (next == prev) {
        continue;
      }

      if (canAppBeRunnable(next.getQueue(), next.getUser())) {//该user在该队列上可以运行
        trackRunnableApp(next);
        FSAppAttempt appSched = next;
        next.getQueue().getRunnableAppSchedulables().add(appSched);
        noLongerPendingApps.add(appSched);

        // No more than one app per list will be able to be made runnable, so
        // we can stop looking after we've found that many
        if (noLongerPendingApps.size() >= appsNowMaybeRunnable.size()) {
          break;
        }
      }

      prev = next;
    }
    
    // We remove the apps from their pending lists afterwards so that we don't
    // pull them out from under the iterator.  If they are not in these lists
    // in the first place, there is a bug.
    for (FSAppAttempt appSched : noLongerPendingApps) {
      if (!appSched.getQueue().getNonRunnableAppSchedulables()
          .remove(appSched)) {
        LOG.error("Can't make app runnable that does not already exist in queue"
            + " as non-runnable: " + appSched + ". This should never happen.");
      }
      
      if (!usersNonRunnableApps.remove(appSched.getUser(), appSched)) {
        LOG.error("Waiting app " + appSched + " expected to be in "
        		+ "usersNonRunnableApps, but was not. This should never happen.");
      }
    }
  }
  
  /**
   * Updates the relevant tracking variables after a runnable app with the given
   * queue and user has been removed.
   * 不跟踪一个运行的app
   */
  public void untrackRunnableApp(FSAppAttempt app) {
    // Update usersRunnableApps
    String user = app.getUser();
    //删除user与app之间的关联关系
    int newUserNumRunning = usersNumRunnableApps.get(user) - 1;
    if (newUserNumRunning == 0) {
      usersNumRunnableApps.remove(user);
    } else {
      usersNumRunnableApps.put(user, newUserNumRunning);
    }
    
    // Update runnable app bookkeeping for queues减少该队列与父队列中app运行的数量
    FSLeafQueue queue = app.getQueue();
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.decrementRunnableApps();
      parent = parent.getParent();
    }
  }
  
  /**
   * Stops tracking the given non-runnable app
   * 不跟踪未运行的app
   */
  public void untrackNonRunnableApp(FSAppAttempt app) {
    usersNonRunnableApps.remove(app.getUser(), app);
  }

  /**
   * Traverses the queue hierarchy under the given queue to gather all lists
   * of non-runnable applications.
   * 获取该队列以及所有的子队列中未运行的应用集合,添加到参数appLists中
   * List<List<FSAppAttempt>> appLists中第一层List表示每一个队列,里面的List<FSAppAttempt>表示每一个队列中未运行的应用集合
   */
  private void gatherPossiblyRunnableAppLists(FSQueue queue,
      List<List<FSAppAttempt>> appLists) {
	  
	  //如果当前队列还有空间可以运行应用,则进行处理
    if (queue.getNumRunnableApps() < scheduler.getAllocationConfiguration()
        .getQueueMaxApps(queue.getName())) {
      if (queue instanceof FSLeafQueue) {
        appLists.add(((FSLeafQueue)queue).getNonRunnableAppSchedulables());
      } else {
        for (FSQueue child : queue.getChildQueues()) {
          gatherPossiblyRunnableAppLists(child, appLists);
        }
      }
    }
  }

  /**
   * Takes a list of lists, each of which is ordered by start time, and returns
   * their elements in order of start time.
   * 
   * We maintain positions in each of the lists.  Each next() call advances
   * the position in one of the lists.  We maintain a heap that orders lists
   * by the start time of the app in the current position in that list.
   * This allows us to pick which list to advance in O(log(num lists)) instead
   * of O(num lists) time.
   * List<List<FSAppAttempt>> appLists中第一层List表示每一个队列,里面的List<FSAppAttempt>表示每一个队列中未运行的应用集合
   * 所有的队列中未运行的任务进行比较,按照开始时间排序,一个一个迭代出来
   */
  static class MultiListStartTimeIterator implements
      Iterator<FSAppAttempt> {

    private List<FSAppAttempt>[] appLists;//List<FSAppAttempt>表示每一个队列中未运行的应用集合,每一个数组表示每一个队列
    private int[] curPositionsInAppLists;//每一个队列有一个int值,存储运行了到该队列的第几个应用了
    private PriorityQueue<IndexAndTime> appListsByCurStartTime;//优先比较队列

    @SuppressWarnings("unchecked")
    public MultiListStartTimeIterator(List<List<FSAppAttempt>> appListList) {
      appLists = appListList.toArray(new List[appListList.size()]);//转换成数组
      curPositionsInAppLists = new int[appLists.length];//每一个数组有一个int值
      appListsByCurStartTime = new PriorityQueue<IndexAndTime>();
      for (int i = 0; i < appLists.length; i++) {
        long time = appLists[i].isEmpty() ? Long.MAX_VALUE : appLists[i].get(0)
            .getStartTime();
        appListsByCurStartTime.add(new IndexAndTime(i, time));
      }
    }

    @Override
    public boolean hasNext() {
      return !appListsByCurStartTime.isEmpty()
          && appListsByCurStartTime.peek().time != Long.MAX_VALUE;
    }

    @Override
    public FSAppAttempt next() {
      IndexAndTime indexAndTime = appListsByCurStartTime.remove();
      int nextListIndex = indexAndTime.index;
      FSAppAttempt next = appLists[nextListIndex]
          .get(curPositionsInAppLists[nextListIndex]);//获取下一个应用任务
      curPositionsInAppLists[nextListIndex]++;//因为数组是在内存中的,因此++,将会改变数组的值

      if (curPositionsInAppLists[nextListIndex] < appLists[nextListIndex].size()) {//当前数组的大小还没有循环完
        indexAndTime.time = appLists[nextListIndex]
            .get(curPositionsInAppLists[nextListIndex]).getStartTime();
      } else {//当前数组的大小已经循环完了
        indexAndTime.time = Long.MAX_VALUE;
      }
      appListsByCurStartTime.add(indexAndTime);

      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }

    private static class IndexAndTime implements Comparable<IndexAndTime> {
      public int index;
      public long time;

      public IndexAndTime(int index, long time) {
        this.index = index;//第几个数组
        this.time = time;//当前数组中第一个任务的开始时间
      }

      //比较时间即可
      @Override
      public int compareTo(IndexAndTime o) {
        return time < o.time ? -1 : (time > o.time ? 1 : 0);
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof IndexAndTime)) {
          return false;
        }
        IndexAndTime other = (IndexAndTime)o;
        return other.time == time;
      }

      @Override
      public int hashCode() {
        return (int)time;
      }
    }
  }
}

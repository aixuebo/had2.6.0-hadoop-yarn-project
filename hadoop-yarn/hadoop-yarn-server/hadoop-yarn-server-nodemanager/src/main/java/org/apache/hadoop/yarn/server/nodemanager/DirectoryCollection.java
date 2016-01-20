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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker;

/**
 * Manages a list of local storage directories.
 * 主要校验本地多个磁盘,哪个磁盘满了,哪个磁盘异常不可用了,哪些磁盘是正常磁盘
 */
class DirectoryCollection {
  private static final Log LOG = LogFactory.getLog(DirectoryCollection.class);

  //磁盘失败原因
  public enum DiskErrorCause {
    DISK_FULL,//磁盘满了
    OTHER//其他原因导致的失败
  }

  //磁盘异常时候的详细信息
  static class DiskErrorInformation {
    DiskErrorCause cause;
    String message;

    DiskErrorInformation(DiskErrorCause cause, String message) {
      this.cause = cause;
      this.message = message;
    }
  }

  /**
   * Returns a merged list which contains all the elements of l1 and l2
   * @param l1 the first list to be included
   * @param l2 the second list to be included
   * @return a new list containing all the elements of the first and second list
   * 合并两个list
   */
  static List<String> concat(List<String> l1, List<String> l2) {
    List<String> ret = new ArrayList<String>(l1.size() + l2.size());
    ret.addAll(l1);
    ret.addAll(l2);
    return ret;
  }

  // Good local storage directories
  private List<String> localDirs;//当前可用的目录 
  private List<String> errorDirs;//当前错误的目录 
  private List<String> fullDirs;//当前磁盘满的目录

  private int numFailures;//失败次数
  
  private float diskUtilizationPercentageCutoff;//的值在0-100之间,使用磁盘大小百分比限制,使用的磁盘占用总磁盘的百分比 > 该值,则说明磁盘满了
  private long diskUtilizationSpaceCutoff;//单位是兆,每个磁盘最小要保留的空间,如果剩余空间超过了<该值,则说明磁盘满了,例如该值为5M,则剩余空间为4M的时候,为磁盘满了

  /**
   * Create collection for the directories specified. No check for free space.
   * 
   * @param dirs
   *          directories to be monitored
   */
  public DirectoryCollection(String[] dirs) {
    this(dirs, 100.0F, 0);
  }

  /**
   * Create collection for the directories specified. Users must specify the
   * maximum percentage of disk utilization allowed. Minimum amount of disk
   * space is not checked.
   * 
   * @param dirs
   *          directories to be monitored
   * @param utilizationPercentageCutOff
   *          percentage of disk that can be used before the dir is taken out of
   *          the good dirs list
   * 
   */
  public DirectoryCollection(String[] dirs, float utilizationPercentageCutOff) {
    this(dirs, utilizationPercentageCutOff, 0);
  }

  /**
   * Create collection for the directories specified. Users must specify the
   * minimum amount of free space that must be available for the dir to be used.
   * 
   * @param dirs
   *          directories to be monitored
   * @param utilizationSpaceCutOff
   *          minimum space, in MB, that must be available on the disk for the
   *          dir to be marked as good
   * 
   */
  public DirectoryCollection(String[] dirs, long utilizationSpaceCutOff) {
    this(dirs, 100.0F, utilizationSpaceCutOff);
  }

  /**
   * Create collection for the directories specified. Users must specify the
   * maximum percentage of disk utilization allowed and the minimum amount of
   * free space that must be available for the dir to be used. If either check
   * fails the dir is removed from the good dirs list.
   * 
   * @param dirs
   *          directories to be monitored
   * @param utilizationPercentageCutOff
   *          percentage of disk that can be used before the dir is taken out of
   *          the good dirs list
   * @param utilizationSpaceCutOff
   *          minimum space, in MB, that must be available on the disk for the
   *          dir to be marked as good
   *          
   * 
   */
  public DirectoryCollection(String[] dirs, 
      float utilizationPercentageCutOff,
      long utilizationSpaceCutOff) {
	//初始化所有的目录都是正常的目录
    localDirs = new CopyOnWriteArrayList<String>(dirs);
    errorDirs = new CopyOnWriteArrayList<String>();
    fullDirs = new CopyOnWriteArrayList<String>();

    //diskUtilizationPercentageCutoff 的值在0-100之间
    diskUtilizationPercentageCutoff = utilizationPercentageCutOff;
    //diskUtilizationSpaceCutoff的值在0-utilizationSpaceCutOff之间
    diskUtilizationSpaceCutoff = utilizationSpaceCutOff;
    
    //设置值在0-100之间
    diskUtilizationPercentageCutoff =
        utilizationPercentageCutOff < 0.0F ? 0.0F
            : (utilizationPercentageCutOff > 100.0F ? 100.0F
                : utilizationPercentageCutOff);
    
    //设置值必须大于0
    diskUtilizationSpaceCutoff =
        utilizationSpaceCutOff < 0 ? 0 : utilizationSpaceCutOff;
  }

  /**
   * @return the current valid directories
   * 返回当前可用的目录 
   */
  synchronized List<String> getGoodDirs() {
    return Collections.unmodifiableList(localDirs);
  }

  /**
   * @return the failed directories
   * 返回errorDirs与errorDirs汇总值
   */
  synchronized List<String> getFailedDirs() {
    return Collections.unmodifiableList(
        DirectoryCollection.concat(errorDirs, fullDirs));
  }

  /**
   * @return the directories that have used all disk space
   */

  synchronized List<String> getFullDirs() {
    return fullDirs;
  }

  /**
   * @return total the number of directory failures seen till now
   */
  synchronized int getNumFailures() {
    return numFailures;
  }

  /**
   * Create any non-existent directories and parent directories, updating the
   * list of valid directories if necessary.
   * @param localFs local file system to use
   * @param perm absolute permissions to use for any directories created
   * @return true if there were no errors, false if at least one error occurred
   * 创建没有存在的目录
   */
  synchronized boolean createNonExistentDirs(FileContext localFs,FsPermission perm) {
    boolean failed = false;
    for (final String dir : localDirs) {
      try {
        createDir(localFs, new Path(dir), perm);
      } catch (IOException e) {
        LOG.warn("Unable to create directory " + dir + " error " +
            e.getMessage() + ", removing from the list of valid directories.");
        localDirs.remove(dir);
        errorDirs.add(dir);
        numFailures++;
        failed = true;
      }
    }
    return !failed;
  }

  /**
   * Check the health of current set of local directories(good and failed),
   * updating the list of valid directories if necessary.
   *
   * @return <em>true</em> if there is a new disk-failure identified in this
   *         checking or a failed directory passes the disk check <em>false</em>
   *         otherwise.
   */
  synchronized boolean checkDirs() {
    boolean setChanged = false;//true磁盘好坏有更改情况
    
    //预先设置三个集合,相当于copy的过程
    Set<String> preCheckGoodDirs = new HashSet<String>(localDirs);
    Set<String> preCheckFullDirs = new HashSet<String>(fullDirs);
    Set<String> preCheckOtherErrorDirs = new HashSet<String>(errorDirs);
    
    //汇总目录
    //合并所有的异常目录集合,是一个复制的过程
    List<String> failedDirs = DirectoryCollection.concat(errorDirs, fullDirs);
    //合并所有的目录集合,是一个复制的过程
    List<String> allLocalDirs = DirectoryCollection.concat(localDirs, failedDirs);

    //检查是否有异常的磁盘
    Map<String, DiskErrorInformation> dirsFailedCheck = testDirs(allLocalDirs);

    localDirs.clear();
    errorDirs.clear();
    fullDirs.clear();

    //循环每一个异常磁盘
    for (Map.Entry<String, DiskErrorInformation> entry : dirsFailedCheck.entrySet()) {
      String dir = entry.getKey();//磁盘路径
      DiskErrorInformation errorInformation = entry.getValue();//磁盘对应的异常类
      switch (entry.getValue().cause) {
      case DISK_FULL://磁盘满了
        fullDirs.add(entry.getKey());
        break;
      case OTHER://磁盘其他异常
        errorDirs.add(entry.getKey());
        break;
      }
      if (preCheckGoodDirs.contains(dir)) {
        LOG.warn("Directory " + dir + " error, " + errorInformation.message
            + ", removing from list of valid directories");
        setChanged = true;
        numFailures++;
      }
    }
    for (String dir : allLocalDirs) {
      if (!dirsFailedCheck.containsKey(dir)) {
        localDirs.add(dir);
        if (preCheckFullDirs.contains(dir) || preCheckOtherErrorDirs.contains(dir)) {
          setChanged = true;
          LOG.info("Directory " + dir
              + " passed disk check, adding to list of valid directories.");
        }
      }
    }
    Set<String> postCheckFullDirs = new HashSet<String>(fullDirs);
    Set<String> postCheckOtherDirs = new HashSet<String>(errorDirs);
    for (String dir : preCheckFullDirs) {
      if (postCheckOtherDirs.contains(dir)) {
        LOG.warn("Directory " + dir + " error "
            + dirsFailedCheck.get(dir).message);
      }
    }

    for (String dir : preCheckOtherErrorDirs) {
      if (postCheckFullDirs.contains(dir)) {
        LOG.warn("Directory " + dir + " error "
            + dirsFailedCheck.get(dir).message);
      }
    }
    return setChanged;
  }

  /**
   * 检查参数中每一个磁盘是否有异常,如果有则添加到集合中
   */
  Map<String, DiskErrorInformation> testDirs(List<String> dirs) {
    HashMap<String, DiskErrorInformation> ret = new HashMap<String, DiskErrorInformation>();
    for (final String dir : dirs) {
      String msg;
      try {
        File testDir = new File(dir);
        DiskChecker.checkDir(testDir);
        if (isDiskUsageOverPercentageLimit(testDir)) {//检查该磁盘是否使用的百分比达到上限了
          msg =
              "used space above threshold of "
                  + diskUtilizationPercentageCutoff
                  + "%";
          ret.put(dir,
            new DiskErrorInformation(DiskErrorCause.DISK_FULL, msg));//磁盘满了
          continue;
        } else if (isDiskFreeSpaceUnderLimit(testDir)) {//磁盘满了
          msg =
              "free space below limit of " + diskUtilizationSpaceCutoff
                  + "MB";
          ret.put(dir,
            new DiskErrorInformation(DiskErrorCause.DISK_FULL, msg));
          continue;
        }

        // create a random dir to make sure fs isn't in read-only mode
        /**
         * 1.在参数目录下创建一个目录,并且检查该目录是否有读、写、执行权限
         * 2.情况该目录下所有文件,并且删除该目录        
         */
        verifyDirUsingMkdir(testDir);
      } catch (IOException ie) {
    	//其他异常信息
        ret.put(dir,new DiskErrorInformation(DiskErrorCause.OTHER, ie.getMessage()));
      }
    }
    return ret;
  }

  /**
   * Function to test whether a dir is working correctly by actually creating a
   * random directory.
   *
   * @param dir
   *          the dir to test
   * 1.在参数目录下创建一个目录,并且检查该目录是否有读、写、执行权限
   * 2.情况该目录下所有文件,并且删除该目录
   */
  private void verifyDirUsingMkdir(File dir) throws IOException {

    //5位随机数,字母+数字组成的
    String randomDirName = RandomStringUtils.randomAlphanumeric(5);
    File target = new File(dir, randomDirName);
    int i = 0;
    while (target.exists()) {
      randomDirName = RandomStringUtils.randomAlphanumeric(5) + i;
      target = new File(dir, randomDirName);
      i++;
    }
    try {
      /**
   * 检查该目录
   * 1.目录必须能被创建
   * 2.该参数必须是目录,不能是文件
   * 3.该文件必须有可读、可写、可执行权限
       */
      DiskChecker.checkDir(target);
    } finally {
      //情况该目录下所有文件,并且删除该目录
      FileUtils.deleteQuietly(target);
    }
  }

  /**
   * 检查该磁盘是否使用的百分比达到上限了
   */
  private boolean isDiskUsageOverPercentageLimit(File dir) {
    float freePercentage = 100 * (dir.getUsableSpace() / (float) dir.getTotalSpace());//未使用空间大小/总磁盘大小,获取未使用磁盘百分比
    float usedPercentage = 100.0F - freePercentage;//使用磁盘百分比
    return (usedPercentage > diskUtilizationPercentageCutoff || usedPercentage >= 100.0F);
  }

  /**
   * 判断磁盘是否还有空间
   */
  private boolean isDiskFreeSpaceUnderLimit(File dir) {
    long freeSpace = dir.getUsableSpace() / (1024 * 1024);//获取当前dir所在磁盘剩余磁盘大小,单位是M
    return freeSpace < this.diskUtilizationSpaceCutoff;
  }

  /**
   * 创建该dir,并且赋予权限
   */
  private void createDir(FileContext localFs, Path dir, FsPermission perm)
      throws IOException {
    if (dir == null) {
      return;
    }
    try {
      localFs.getFileStatus(dir);
    } catch (FileNotFoundException e) {
      createDir(localFs, dir.getParent(), perm);
      localFs.mkdir(dir, perm, false);
      if (!perm.equals(perm.applyUMask(localFs.getUMask()))) {
        localFs.setPermission(dir, perm);
      }
    }
  }
  
  public float getDiskUtilizationPercentageCutoff() {
    return diskUtilizationPercentageCutoff;
  }

  public void setDiskUtilizationPercentageCutoff(float diskUtilizationPercentageCutoff) {
    this.diskUtilizationPercentageCutoff =
        diskUtilizationPercentageCutoff < 0.0F ? 0.0F
            : (diskUtilizationPercentageCutoff > 100.0F ? 100.0F
                : diskUtilizationPercentageCutoff);
  }

  public long getDiskUtilizationSpaceCutoff() {
    return diskUtilizationSpaceCutoff;
  }

  public void setDiskUtilizationSpaceCutoff(long diskUtilizationSpaceCutoff) {
    diskUtilizationSpaceCutoff =
        diskUtilizationSpaceCutoff < 0 ? 0 : diskUtilizationSpaceCutoff;
    this.diskUtilizationSpaceCutoff = diskUtilizationSpaceCutoff;
  }
}

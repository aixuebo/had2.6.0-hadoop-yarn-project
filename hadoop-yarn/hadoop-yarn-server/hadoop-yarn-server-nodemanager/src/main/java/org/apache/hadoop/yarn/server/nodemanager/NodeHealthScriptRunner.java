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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 
 * The class which provides functionality of checking the health of the node
 * using the configured node health script and reporting back to the service
 * for which the health checker has been asked to report.
 * 定时执行脚本,如果该脚本需要参数,则多脚本参数信息,用逗号拆分
 */
public class NodeHealthScriptRunner extends AbstractService {

  private static Log LOG = LogFactory.getLog(NodeHealthScriptRunner.class);

  /** Absolute path to the health script. script文件*/
  private String nodeHealthScript;
  /** Delay after which node health script to be executed 每次执行间隔时间*/
  private long intervalTime;
  /** Time after which the script should be timedout 脚本超时时间,如果该时间内还没有执行完脚本,则出异常*/
  private long scriptTimeout;
  /** Timer used to schedule node health monitoring script execution 定时调度器*/
  private Timer nodeHealthScriptScheduler;

  /** ShellCommandExecutor used to execute monitoring script */
  ShellCommandExecutor shexec = null;

  /** Configuration used by the checker */
  private Configuration conf;

  /** Pattern used for searching in the output of the node health script */
  static private final String ERROR_PATTERN = "ERROR";

  /** Time out error message 超时后的输出信息*/
  static final String NODE_HEALTH_SCRIPT_TIMED_OUT_MSG = "Node health script timed out";

  private boolean isHealthy;//当前是否健康

  private String healthReport;//健康情况的报告

  private long lastReportedTime;//最后一次执行脚本时间

  private TimerTask timer;
  
  private enum HealthCheckerExitStatus {
    SUCCESS,//成功检查
    TIMED_OUT,//检查脚本超时
    FAILED_WITH_EXIT_CODE,//失败退出,正常返回状态码
    FAILED_WITH_EXCEPTION,//失败退出,出现异常
    FAILED//检查失败,即脚本输出中带有ERROR信息
  }


  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * node health script.
   * 定时执行器
   * 脚本参数信息,用逗号拆分
   */
  private class NodeHealthMonitorExecutor extends TimerTask {

    String exceptionStackTrace = "";//异常堆栈信息

    public NodeHealthMonitorExecutor(String[] args) {
      ArrayList<String> execScript = new ArrayList<String>();
      execScript.add(nodeHealthScript);
      if (args != null) {
        execScript.addAll(Arrays.asList(args));
      }
      shexec = new ShellCommandExecutor(execScript
          .toArray(new String[execScript.size()]), null, null, scriptTimeout);
    }

    @Override
    public void run() {
      HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
      try {
        shexec.execute();
      } catch (ExitCodeException e) {
        // ignore the exit code of the script
        status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
        // On Windows, we will not hit the Stream closed IOException
        // thrown by stdout buffered reader for timeout event.
        if (Shell.WINDOWS && shexec.isTimedOut()) {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
      } catch (Exception e) {
        LOG.warn("Caught exception : " + e.getMessage());
        if (!shexec.isTimedOut()) {
          status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
        } else {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
        exceptionStackTrace = StringUtils.stringifyException(e);
      } finally {
        if (status == HealthCheckerExitStatus.SUCCESS) {
          //如果成功执行脚本,检查脚本输出是否带有ERROR信息,如果带有,则最终返回HealthCheckerExitStatus.FAILED;
          if (hasErrors(shexec.getOutput())) {
            status = HealthCheckerExitStatus.FAILED;
          }
        }
        reportHealthStatus(status);
      }
    }

    /**
     * Method which is used to parse output from the node health monitor and
     * send to the report address.
     * 
     * The timed out script or script which causes IOException output is
     * ignored.
     * 
     * The node is marked unhealthy if
     * <ol>
     * <li>The node health script times out</li>
     * <li>The node health scripts output has a line which begins with ERROR</li>
     * <li>An exception is thrown while executing the script</li>
     * </ol>
     * If the script throws {@link IOException} or {@link ExitCodeException} the
     * output is ignored and node is left remaining healthy, as script might
     * have syntax error.
     * 
     * @param status
     */
    void reportHealthStatus(HealthCheckerExitStatus status) {
      long now = System.currentTimeMillis();
      switch (status) {
      case SUCCESS:
        setHealthStatus(true, "", now);
        break;
      case TIMED_OUT:
        setHealthStatus(false, NODE_HEALTH_SCRIPT_TIMED_OUT_MSG);
        break;
      case FAILED_WITH_EXCEPTION:
        setHealthStatus(false, exceptionStackTrace);
        break;
      case FAILED_WITH_EXIT_CODE:
    	  //脚本异常导致的退出,因此说明NodeManager是正常的
        setHealthStatus(true, "", now);
        break;
      case FAILED:
        setHealthStatus(false, shexec.getOutput());
        break;
      }
    }

    /**
     * Method to check if the output string has line which begins with ERROR.
     * 
     * @param output
     *          string
     * @return true if output string has error pattern in it.
     * 如果成功执行脚本,检查脚本输出是否带有ERROR信息,如果带有,则最终返回HealthCheckerExitStatus.FAILED;
     */
    private boolean hasErrors(String output) {
      String[] splits = output.split("\n");
      for (String split : splits) {
        if (split.startsWith(ERROR_PATTERN)) {
          return true;
        }
      }
      return false;
    }
  }

  public NodeHealthScriptRunner() {
    super(NodeHealthScriptRunner.class.getName());
    this.lastReportedTime = System.currentTimeMillis();
    this.isHealthy = true;
    this.healthReport = "";    
  }

  /*
   * Method which initializes the values for the script path and interval time.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    this.nodeHealthScript = 
        conf.get(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH);
    this.intervalTime = conf.getLong(YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS);
    this.scriptTimeout = conf.getLong(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS);
    //脚本参数信息,用逗号拆分
    String[] args = conf.getStrings(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_OPTS,
        new String[] {});
    timer = new NodeHealthMonitorExecutor(args);
    super.serviceInit(conf);
  }

  /**
   * Method used to start the Node health monitoring.
   * 
   */
  @Override
  protected void serviceStart() throws Exception {
    // if health script path is not configured don't start the thread.
    if (!shouldRun(conf)) {
      LOG.info("Not starting node health monitor");
      return;
    }
    //定时执行脚本
    nodeHealthScriptScheduler = new Timer("NodeHealthMonitor-Timer", true);
    // Start the timer task immediately and
    // then periodically at interval time.
    nodeHealthScriptScheduler.scheduleAtFixedRate(timer, 0, intervalTime);
    super.serviceStart();
  }

  /**
   * Method used to terminate the node health monitoring service.
   * 
   */
  @Override
  protected void serviceStop() {
    if (!shouldRun(conf)) {
      return;
    }
    if (nodeHealthScriptScheduler != null) {
      nodeHealthScriptScheduler.cancel();
    }
    if (shexec != null) {
      Process p = shexec.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
  }

  /**
   * Gets the if the node is healthy or not
   * 
   * @return true if node is healthy
   */
  public boolean isHealthy() {
    return isHealthy;
  }

  /**
   * Sets if the node is healhty or not considering disks' health also.
   * 
   * @param isHealthy
   *          if or not node is healthy
   */
  private synchronized void setHealthy(boolean isHealthy) {
    this.isHealthy = isHealthy;
  }

  /**
   * Returns output from health script. if node is healthy then an empty string
   * is returned.
   * 
   * @return output from health script
   */
  public String getHealthReport() {
    return healthReport;
  }

  /**
   * Sets the health report from the node health script. Also set the disks'
   * health info obtained from DiskHealthCheckerService.
   *
   * @param healthReport
   */
  private synchronized void setHealthReport(String healthReport) {
    this.healthReport = healthReport;
  }
  
  /**
   * Returns time stamp when node health script was last run.
   * 
   * @return timestamp when node health script was last run
   */
  public long getLastReportedTime() {
    return lastReportedTime;
  }

  /**
   * Sets the last run time of the node health script.
   * 
   * @param lastReportedTime
   */
  private synchronized void setLastReportedTime(long lastReportedTime) {
    this.lastReportedTime = lastReportedTime;
  }

  /**
   * Method used to determine if or not node health monitoring service should be
   * started or not. Returns true if following conditions are met:
   * 
   * <ol>
   * <li>Path to Node health check script is not empty</li>
   * <li>Node health check script file exists</li>
   * </ol>
   * 
   * @param conf
   * @return true if node health monitoring service can be started.
   * 是否有脚本,并且脚本权限是可执行权限
   */
  public static boolean shouldRun(Configuration conf) {
    String nodeHealthScript = conf.get(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH); 
    if (nodeHealthScript == null || nodeHealthScript.trim().isEmpty()) {
      return false;
    }
    File f = new File(nodeHealthScript);
    return f.exists() && FileUtil.canExecute(f);
  }

  /**
   * @param isHealthy 是否健康
   * @param output 输出健康报告信息
   */
  private synchronized void setHealthStatus(boolean isHealthy, String output) {
    this.setHealthy(isHealthy);
    this.setHealthReport(output);
  }
  
  /**
   * 
   * @param isHealthy 是否健康
   * @param output 输出健康报告信息
   * @param time 最后更新时间
   */
  private synchronized void setHealthStatus(boolean isHealthy, String output,long time) {
    this.setHealthStatus(isHealthy, output);
    this.setLastReportedTime(time);
  }

  /**
   * Used only by tests to access the timer task directly
   * @return the timer task
   */
  TimerTask getTimerTask() {
    return timer;
  }
}

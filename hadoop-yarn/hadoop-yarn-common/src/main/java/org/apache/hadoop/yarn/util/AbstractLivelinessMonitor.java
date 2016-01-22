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

package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.service.AbstractService;

/**
 * A simple liveliness monitor with which clients can register, trust the
 * component to monitor liveliness, get a call-back on expiry and then finally
 * unregister.
 * 监听泛型对象是否活着
 */
@Public
@Evolving
public abstract class AbstractLivelinessMonitor<O> extends AbstractService {

  private static final Log LOG = LogFactory.getLog(AbstractLivelinessMonitor.class);

  //thread which runs periodically to see the last time since a heartbeat is
  //received.
  private Thread checkerThread;//当前多线程
  private volatile boolean stopped;//是否他停止
  public static final int DEFAULT_EXPIRE = 5*60*1000;//5 mins
  private int expireInterval = DEFAULT_EXPIRE;//过期时间
  private int monitorInterval = expireInterval/3;//时间间隔

  private final Clock clock;

  //key正在运行的信息,value是该节点最近的ping时间
  private Map<O, Long> running = new HashMap<O, Long>();

  public AbstractLivelinessMonitor(String name, Clock clock) {
    super(name);
    this.clock = clock;
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    checkerThread = new Thread(new PingChecker());
    checkerThread.setName("Ping Checker");
    checkerThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (checkerThread != null) {
      checkerThread.interrupt();
    }
    super.serviceStop();
  }

  protected abstract void expire(O ob);

  protected void setExpireInterval(int expireInterval) {
    this.expireInterval = expireInterval;
  }

  protected void setMonitorInterval(int monitorInterval) {
    this.monitorInterval = monitorInterval;
  }

  /**
   * 该节点有心跳,则相当于该节点有ping
   */
  public synchronized void receivedPing(O ob) {
    //only put for the registered objects 仅仅对已经收到的信息做处理,即收到的信息才能被ping
    if (running.containsKey(ob)) {
      running.put(ob, clock.getTime());
    }
  }

  public synchronized void register(O ob) {
    running.put(ob, clock.getTime());
  }

  public synchronized void unregister(O ob) {
    running.remove(ob);
  }

  /**
   * 每隔monitorInterval进行一次处理
   * 超过了过期时间则从集合中移除,并且调用expire方法通知该对象被移除了
   */
  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (AbstractLivelinessMonitor.this) {
          Iterator<Map.Entry<O, Long>> iterator = running.entrySet().iterator(); 

          //avoid calculating current time everytime in loop 当前时间
          long currentTime = clock.getTime();

          while (iterator.hasNext()) {
            Map.Entry<O, Long> entry = iterator.next();
            if (currentTime > entry.getValue() + expireInterval) {//当前时间超过了过期时间,则移除
              iterator.remove();
              expire(entry.getKey());
              LOG.info("Expired:" + entry.getKey().toString() + 
                      " Timed out after " + expireInterval/1000 + " secs");
            }
          }
        }
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info(getName() + " thread interrupted");
          break;
        }
      }
    }
  }

}
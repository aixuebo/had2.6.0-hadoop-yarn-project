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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p>State of a <code>Node</code>.</p>
 * 节点的状态信息
 */
@Public
@Unstable
public enum NodeState {
  /** New node */
  NEW, 
  
  /** Running node */
  RUNNING, 
  
  /** Node is unhealthy 节点不健康*/
  UNHEALTHY, 
  
  /** Node is out of service 节点要退出服务,不在提供服务,即节点要退役*/
  DECOMMISSIONED, 
  
  /** Node has not sent a heartbeat for some configured time threshold
   * 节点不能发送心跳了
   **/
  LOST, 
  
  /** Node has rebooted
   *  重新启动该节点
   **/
  REBOOTED;
  
  //不健康、退役、不能发送心跳了,都称之为节点不可用,返回true
  public boolean isUnusable() {
    return (this == UNHEALTHY || this == DECOMMISSIONED || this == LOST);
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

/**
 * resourceManager中任务的一个实例的状态
 */
public enum RMAppAttemptState {
	
  NEW, SUBMITTED,//已经提交到队列中了
  SCHEDULED,//队列已经调度了该尝试任务,该阶段要向调度器提交一个AM容器
  
  ALLOCATED_SAVING,//说明正在保存分配的AM容器信息
  ALLOCATED,//说明AM容器已经分配完成
  
  LAUNCHED,//启动AM
  RUNNING, //该尝试任务已经在运行中了
  
  FINISHED, KILLED,FAILED,//该尝试任务的最终状态
  
  FINISHING, 
  FINAL_SAVING,
  
  LAUNCHED_UNMANAGED_SAVING//表示AM容器是job提交的节点去作为AM
}

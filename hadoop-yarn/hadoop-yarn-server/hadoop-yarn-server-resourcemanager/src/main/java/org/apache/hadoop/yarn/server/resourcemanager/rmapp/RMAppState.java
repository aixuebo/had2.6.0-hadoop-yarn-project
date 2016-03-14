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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

/**
 * resourceManager中任务的状态
 * @author Administrator
 *
 */
public enum RMAppState {
  NEW,
  NEW_SAVING,//说明app正在向RMStateStore里面写入日志中
  SUBMITTED,//说明app已经提交到调度器中了
  ACCEPTED,//说明调度器已经接受了该app了
  
  RUNNING,//该app已经开始被调度运行了
  FINAL_SAVING,
  
  KILLING,//正在kill过程中
  FINISHING,//正在完成过程中
  
  FINISHED,//已经完成
  FAILED,//已经失败
  KILLED//已经kill了
}

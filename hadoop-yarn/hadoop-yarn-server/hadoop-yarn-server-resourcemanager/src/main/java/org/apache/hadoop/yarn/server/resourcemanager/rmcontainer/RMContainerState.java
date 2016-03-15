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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

public enum RMContainerState {
  NEW, 
  RESERVED, //预保留
  ALLOCATED, //说明该容器已经准备好了
  ACQUIRED,//表示队列已经分配了该容器 
  RUNNING, //说明该容器已经启动了
  
  COMPLETED, //说明该容器已经完成了,是已知的完成状态
  EXPIRED,//该容器已经无心跳了,也是一种完成状态
  KILLED,//该容器已经被kill杀死了,也是一种完成状态
  
  RELEASED //调度器触发的该事件,释放一个容器
}

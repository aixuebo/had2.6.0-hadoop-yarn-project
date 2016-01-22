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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event;

/**
 * 对资源的初始化事件类型
 */
public enum LocalizationEventType {
  INIT_APPLICATION_RESOURCES,//一个应用要在本地进行初始化,初始化应用的日志,包括初始化的失败和成功
  INIT_CONTAINER_RESOURCES,//加载容器中这些可见性下的的资源,发送下载资源请求即可
  CACHE_CLEANUP,//当接收到LocalizationEventType.CACHE_CLEANUP事件时,调用ResourceLocalizationService的handleCacheCleanup方法,定时周期的清理缓存大小
  CLEANUP_CONTAINER_RESOURCES,//清理这些可见性下的资源,或者清理该容器的所有资源
  DESTROY_APPLICATION_RESOURCES,//由于该应用已经被完成,因此要清理该应用加载的资源
}

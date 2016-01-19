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

import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizedResource;

/**
 * Events delivered to {@link LocalizedResource}. Each of these
 * events is a subclass of {@link ResourceEvent}.
 */
public enum ResourceEventType {
  /** See {@link ResourceRequestEvent} */
  REQUEST,//资源请求,该类标志资源请求类,用于表达资源来了,该下载了
  /** See {@link ResourceLocalizedEvent} */ 
  LOCALIZED,//资源加载完成,这个时候已经本地有该文件,因此知道该文件路径以及该文件大小,是ResourceLocalizationService类PublicLocalizer的run方法触发的该事件
  /** See {@link ResourceReleaseEvent} */
  RELEASE,// 释放该容器的所有资源
  /** See {@link ResourceFailedLocalizationEvent} */
  LOCALIZATION_FAILED,
  /** See {@link ResourceRecoveredEvent} */
  RECOVERED
}

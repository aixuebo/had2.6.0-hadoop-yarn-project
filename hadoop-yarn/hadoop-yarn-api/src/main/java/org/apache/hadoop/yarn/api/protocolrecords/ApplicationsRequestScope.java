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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Enumeration that controls the scope of applications fetched
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum ApplicationsRequestScope {
  /** All jobs 可以查看所有的job*/
  ALL,

  /** Jobs viewable by current user 只能查看该user可以有VIEW_APP权限的job*/
  VIEWABLE,

  /** Jobs owned by current user 仅能查看user自己的job*/
  OWN
}

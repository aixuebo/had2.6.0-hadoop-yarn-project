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
package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;

@Private
@Unstable
public class DefaultResourceCalculator extends ResourceCalculator {
  
	//仅仅比较内存大小
  @Override
  public int compare(Resource unused, Resource lhs, Resource rhs) {
    // Only consider memory
    return lhs.getMemory() - rhs.getMemory();
  }

  //返回左边的内存能容纳多少个右边的内存资源
  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    // Only consider memory
    return available.getMemory() / required.getMemory();
  }

  //numerator.getMemory() / denominator.getMemory()
  @Override
  public float divide(Resource unused, 
      Resource numerator, Resource denominator) {
    return ratio(numerator, denominator);
  }
  
  //true表示蚕食的内存是0,是不允许进行除法运算的
  public boolean isInvalidDivisor(Resource r) {
    if (r.getMemory() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(Resource a, Resource b) {
    return (float)a.getMemory() / b.getMemory();
  }

  /**
   * 内存相除之后,向上取整,表示会最终多获取一个资源
   */
  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemory(), denominator));
  }

  /**
   * 格式化内存,无论r给的空间有多大,都能保证
   * minimumResource<r<maximumResource
   * 并且r最终的内存大小是比最终的多一些,一些取决于stepFactor的资源大小
   */
  @Override
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor) {
    int normalizedMemory = Math.min(
        roundUp(
            Math.max(r.getMemory(), minimumResource.getMemory()),//约束需要的资源最少也得是minimumResource资源
            stepFactor.getMemory()),
            maximumResource.getMemory());//约束需要的资源最多是maximumResource资源
    return Resources.createResource(normalizedMemory);
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
                            Resource maximumResource) {
    return normalize(r, minimumResource, maximumResource, minimumResource);
  }

  /**
   * 最终让r的资源略大一部分,略大的范围是stepFactor的内存大小范围
   */
  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getMemory(), stepFactor.getMemory())
        );
  }

  /**
   * 最终让r的资源略小一部分,略小的范围是stepFactor的内存大小范围
   */
  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemory(), stepFactor.getMemory()));
  }

  /**
   * 使r资源内存增加by倍,然后再提升略大一些内存,略大的内存范围取决于stepFactor的内存大小
   */
  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp((int)(r.getMemory() * by + 0.5), stepFactor.getMemory())
        );
  }

  /**
   * 使r资源内存增加by倍,然后再降低略小一些内存,略小的内存范围取决于stepFactor的内存大小
   */
  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getMemory() * by), 
            stepFactor.getMemory()
            )
        );
  }

}

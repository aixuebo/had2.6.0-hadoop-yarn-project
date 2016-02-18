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

/**
 * A set of {@link Resource} comparison and manipulation interfaces.
 */
@Private
@Unstable
public abstract class ResourceCalculator {

  public abstract int 
  compare(Resource clusterResource, Resource lhs, Resource rhs);
  
  /**
   * ceil其实就是向上取整,返回double值,例如Math.ceil(2.6) = 3.0
   * 
   * 因此该方法,含义就是a/b的值,向上取整
   * 例如 10/3 返回是4
   */
  public static int divideAndCeil(int a, int b) {
    if (b == 0) {
      return 0;
    }
    
    //该公式可以拆开看,就是a/b + (b-1)/b 其实等于a/b之后多加了一个部分,因此会向上取整了
    return (a + (b - 1)) / b;
  }

  /**
   * 含义:其实最终的返回值与a的值是相近的,只是会比a略大一点,至于这一点是多少,取决于b的范围,比如b是100,则最终值与a相比较,最多差距100
   * 公式简化为(a/b)*b 约等于a
   * 但是divideAndCeil(a, b) * b的含义是a/b后向上取整了,因此会比a最终大一部分.
   * 举例 a =10,b=3 则divideAndCeil(a, b) = ceil(3.3) = 4,因此最终值是4 * 3 = 12,比a这个10大了2,而2正好小于b的值3
   */
  public static int roundUp(int a, int b) {
    return divideAndCeil(a, b) * b;
  }

  /**
   * 含义:其实最终的返回值与a的值是相近的,只是会比a略小一点,至于这一点是多少,取决于b的范围,比如b是100,则最终值与a相比较,最多差距100
   * 公式简化为(a/b)*b 约等于a
   * 但是(a / b) * b的含义是a/b后向下取整了,因此会比a最终小一部分.
   * 举例 a =10,b=3 则a / b = down(3.3) = 3,因此最终值是3 * 3 = 9,比a这个10小了1,而1正好小于b的值3
   */
  public static int roundDown(int a, int b) {
    return (a / b) * b;
  }

  /**
   * Compute the number of containers which can be allocated given
   * <code>available</code> and <code>required</code> resources.
   * 
   * @param available available resources
   * @param required required resources
   * @return number of containers which can be allocated
   */
  public abstract int computeAvailableContainers(
      Resource available, Resource required);

  /**
   * Multiply resource <code>r</code> by factor <code>by</code> 
   * and normalize up using step-factor <code>stepFactor</code>.
   * 
   * @param r resource to be multiplied
   * @param by multiplier
   * @param stepFactor factor by which to normalize up 
   * @return resulting normalized resource
   */
  public abstract Resource multiplyAndNormalizeUp(
      Resource r, double by, Resource stepFactor);
  
  /**
   * Multiply resource <code>r</code> by factor <code>by</code> 
   * and normalize down using step-factor <code>stepFactor</code>.
   * 
   * @param r resource to be multiplied
   * @param by multiplier
   * @param stepFactor factor by which to normalize down 
   * @return resulting normalized resource
   */
  public abstract Resource multiplyAndNormalizeDown(
      Resource r, double by, Resource stepFactor);
  
  /**
   * Normalize resource <code>r</code> given the base 
   * <code>minimumResource</code> and verify against max allowed
   * <code>maximumResource</code>
   * 
   * @param r resource
   * @param minimumResource step-factor
   * @param maximumResource the upper bound of the resource to be allocated
   * @return normalized resource
   */
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource) {
    return normalize(r, minimumResource, maximumResource, minimumResource);
  }

  /**
   * Normalize resource <code>r</code> given the base 
   * <code>minimumResource</code> and verify against max allowed
   * <code>maximumResource</code> using a step factor for hte normalization.
   *
   * @param r resource
   * @param minimumResource minimum value
   * @param maximumResource the upper bound of the resource to be allocated
   * @param stepFactor the increment for resources to be allocated
   * @return normalized resource
   */
  public abstract Resource normalize(Resource r, Resource minimumResource,
                                     Resource maximumResource, 
                                     Resource stepFactor);


  /**
   * Round-up resource <code>r</code> given factor <code>stepFactor</code>.
   * 
   * @param r resource
   * @param stepFactor step-factor
   * @return rounded resource
   */
  public abstract Resource roundUp(Resource r, Resource stepFactor);
  
  /**
   * Round-down resource <code>r</code> given factor <code>stepFactor</code>.
   * 
   * @param r resource
   * @param stepFactor step-factor
   * @return rounded resource
   */
  public abstract Resource roundDown(Resource r, Resource stepFactor);
  
  /**
   * Divide resource <code>numerator</code> by resource <code>denominator</code>
   * using specified policy (domination, average, fairness etc.); hence overall
   * <code>clusterResource</code> is provided for context.
   *  
   * @param clusterResource cluster resources
   * @param numerator numerator
   * @param denominator denominator
   * @return <code>numerator</code>/<code>denominator</code> 
   *         using specific policy
   */
  public abstract float divide(
      Resource clusterResource, Resource numerator, Resource denominator);
  
  /**
   * Determine if a resource is not suitable for use as a divisor
   * (will result in divide by 0, etc)
   *
   * @param r resource
   * @return true if divisor is invalid (should not be used), false else
   */
  public abstract boolean isInvalidDivisor(Resource r);

  /**
   * Ratio of resource <code>a</code> to resource <code>b</code>.
   * 
   * @param a resource 
   * @param b resource
   * @return ratio of resource <code>a</code> to resource <code>b</code>
   */
  public abstract float ratio(Resource a, Resource b);

  /**
   * Divide-and-ceil <code>numerator</code> by <code>denominator</code>.
   * 
   * @param numerator numerator resource
   * @param denominator denominator
   * @return resultant resource
   */
  public abstract Resource divideAndCeil(Resource numerator, int denominator);
  
}

/*
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
package org.apache.beam.sdk.extensions.sql.impl.udaf;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine;

public class ArrayAgg {

  public static class ArrayAggArray<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accum, T input) {
      accum.add(input);
      return accum;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accums) {
      List<T> merged = new ArrayList<>();
      for (List<T> accum : accums) {
        for (T o : accum) {
          merged.add(o);
        }
      }
      return merged;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      if (accumulator.isEmpty()) {
        return null;
      }
      return accumulator;
    }
  }
}

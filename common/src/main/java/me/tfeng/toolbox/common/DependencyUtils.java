/**
 * Copyright 2016 Thomas Feng
 *
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

package me.tfeng.toolbox.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.HashMultimap;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class DependencyUtils {

  public static <T> List<T> dependencySort(Collection<T> collection, Comparator<T> dependencyComparator) {
    List<T> result = new ArrayList<>(collection.size());

    HashMultimap<T, T> dependencies = HashMultimap.create();
    Set<T> elements = new HashSet<>(collection);
    for (T element1 : elements) {
      // element1 depends on element2.
      elements.stream().filter(element2 -> element1 != element2)
          .filter(element2 -> dependencyComparator.compare(element1, element2) > 0)
          .forEach(element2 -> dependencies.put(element1, element2) // element1 depends on element2
      );
    }

    while (!elements.isEmpty()) {
      T next = null;
      for (T element : elements) {
        if (dependencies.get(element).isEmpty()) {
          next = element;
          break;
        }
      }
      if (next == null) {
        throw new RuntimeException("Unable to sort list with respect to element dependency; "
            + "remaining elements that form dependency cycle(s) are: " + dependencies.keySet());
      }
      elements.remove(next);
      dependencies.removeAll(next);
      for (Iterator<Entry<T, T>> iterator = dependencies.entries().iterator(); iterator.hasNext();) {
        if (next == iterator.next().getValue()) {
          iterator.remove();
        }
      }
      result.add(next);
    }

    return result;
  }
}

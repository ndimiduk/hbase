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
package org.apache.hadoop.hbase.master.webapp;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterators;

/**
 * An {@link Iterator} over {@code delegate} that limits results to the first {@code limit}
 * entries.
 * <p>Could just use {@link Iterators#limit(Iterator, int)} except that our consumer needs an API
 * to check if the underlying iterator is not yet exhausted.
 */
@InterfaceAudience.Private
public class LimitIterator<T> implements Iterator<T> {

  private final Iterator<T> delegate;
  private final int limit;
  private int count;

  LimitIterator(final Iterator<T> delegate, final int limit) {
    this.delegate = delegate;
    this.limit = limit;
    this.count = 0;
  }

  /**
   * @return {@code true} when {@code delegate} has more entries, {@code false} otherwise.
   */
  public boolean delegateHasMore() {
    return delegate.hasNext();
  }

  @Override
  public boolean hasNext() {
    if (count < limit) {
      return delegate.hasNext();
    }
    return false;
  }

  @Override
  public T next() {
    if (!hasNext()) { throw new NoSuchElementException(); }
    count++;
    return delegate.next();
  }
}

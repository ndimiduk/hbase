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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class})
public class TestLimitIterator {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLimitIterator.class);

  @Test
  public void delegateLargerThanLimit() {
    _delegateLargerThanLimit();
  }

  @Test(expected = NoSuchElementException.class)
  public void overrun() {
    final LimitIterator<Integer> limited = _delegateLargerThanLimit();
    limited.next();
  }

  private static LimitIterator<Integer> _delegateLargerThanLimit() {
    final List<Integer> vals = Arrays.asList(1, 2, 3);
    final LimitIterator<Integer> limited = new LimitIterator<>(vals.iterator(), vals.size() - 1);
    final List<Integer> actual = new LinkedList<>();
    limited.forEachRemaining(actual::add);
    assertEquals(Arrays.asList(1, 2), actual);
    assertTrue(limited.delegateHasMore());
    return limited;
  }

  @Test
  public void delegateSmallerThanLimit() {
    final List<Integer> vals = Arrays.asList(1, 2, 3);
    final LimitIterator<Integer> limited = new LimitIterator<>(vals.iterator(), vals.size() + 1);
    final List<Integer> actual = new LinkedList<>();
    limited.forEachRemaining(actual::add);
    assertEquals(vals, actual);
    assertFalse(limited.delegateHasMore());
  }

  @Test
  public void delegateEqualToLimit() {
    final List<Integer> vals = Arrays.asList(1, 2, 3);
    final LimitIterator<Integer> limited = new LimitIterator<>(vals.iterator(), vals.size());
    final List<Integer> actual = new LinkedList<>();
    limited.forEachRemaining(actual::add);
    assertEquals(vals, actual);
    assertFalse(limited.delegateHasMore());
  }
}

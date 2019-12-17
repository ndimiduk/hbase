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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Rule;
import org.junit.rules.ExternalResource;

/**
 * A {@link Rule} that manages an instance of the {@link MiniHBaseCluster}.
 */
public class TestClusterRule extends ExternalResource {
  private final HBaseTestingUtility testingUtility;
  private final StartMiniClusterOption miniClusterOptions;

  private MiniHBaseCluster miniCluster;

  public TestClusterRule() {
    this(StartMiniClusterOption.builder().build());
  }

  public TestClusterRule(final StartMiniClusterOption miniClusterOptions) {
    this.testingUtility = new HBaseTestingUtility();
    this.miniClusterOptions = miniClusterOptions;
  }

  public CompletableFuture<AsyncConnection> createConnection() {
    if (miniCluster == null) { throw new IllegalStateException("test cluster not initialized"); }
    return ConnectionFactory.createAsyncConnection(miniCluster.getConf());
  }

  @Override
  protected void before() throws Throwable {
    miniCluster = testingUtility.startMiniCluster(miniClusterOptions);
  }

  @Override
  protected void after() {
    try {
      testingUtility.shutdownMiniCluster();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

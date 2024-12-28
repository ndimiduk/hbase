package org.apache.hadoop.hbase.calcite.adapter;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.SchemaPlus;

/**
 * An implementation of {@link HBaseSchema} that supports injection of its connection instance.
 */
public class TestHBaseSchema extends HBaseSchema {

  public TestHBaseSchema(SchemaPlus parentSchema, String name, HBaseConfiguration conf) {
    super(parentSchema, name, conf);
  }

  @Override
  protected void initConnection() {
  }

  public AsyncConnection setAsyncConnection(AsyncConnection conn) {
    synchronized (connectionLock) {
      AsyncConnection oldConn = this.conn;
      this.conn = conn;
      return oldConn;
    }
  }
}

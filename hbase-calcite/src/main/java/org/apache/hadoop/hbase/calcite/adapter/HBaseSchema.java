package org.apache.hadoop.hbase.calcite.adapter;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.Schema;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.SchemaPlus;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.Table;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.impl.AbstractSchema;

public class HBaseSchema extends AbstractSchema {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSchema.class);

  private final boolean isNamespaceSchema;
  private final String name;
  private final Configuration conf;

  protected final Object connectionLock = new Object();
  protected AsyncConnection conn;

  HBaseSchema(SchemaPlus parentSchema, String name, Configuration conf) {
    this.isNamespaceSchema = false;
    this.name = name;
    this.conf = conf;
    LOG.info("Creating instance of HBaseSchema name={}", name);
  }

  private HBaseSchema(String name, AsyncConnection conn) {
    this.isNamespaceSchema = true;
    this.name = name;
    this.conf = conn.getConfiguration();
    synchronized (connectionLock) {
      this.conn = conn;
    }
  }

  protected void initConnection() {
    if (conn != null) {
      return;
    }

    synchronized (connectionLock) {
      if (conn != null) {
        return;
      }

      try {
        this.conn = ConnectionFactory.createAsyncConnection(conf).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        if (conf != null) {
          try {
            conn.close();
          } catch (IOException e) {
            LOG.error("Error closing connection", e);
          }
        }
      }, "HBaseSchema-" + name + "-shutdownHook"));
    }
  }

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    if (isNamespaceSchema) {
      return super.getSubSchemaMap();
    }
    initConnection();
    try {
      return conn.getAdmin().listNamespaces().get().stream()
        .collect(Collectors.toMap(Function.identity(), name -> new HBaseSchema(name, conn)));
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Map<String, Table> getTableMap() {
    if (!isNamespaceSchema) {
      return super.getTableMap();
    }
    initConnection();
    LOG.info("Building Table Map");
    try {
      Map<String, Table> ret = conn.getAdmin().listTableDescriptors(true).get().stream().filter(d -> Objects.equals(d.getTableName().getNamespaceAsString(), name)).collect(Collectors.toMap(val -> val.getTableName().getNameAsString(),
        HBaseTable::new));
      return ret;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}

package org.apache.hadoop.hbase.calcite.adapter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.Schema;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.SchemaFactory;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.SchemaPlus;

public class HBaseSchemaFactory implements SchemaFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSchemaFactory.class);

  /**
   * Used in test to provide some dependency injection.
   */
  static final String SCHEMA_CLASS_KEY = "hbase.calcite.adapter.schema.class";

  @SuppressWarnings("unused")
  public static final HBaseSchemaFactory INSTANCE = new HBaseSchemaFactory();

  private HBaseSchemaFactory() {}

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    LOG.info("Providing HBaseSchema for parentSchema={}, name={}, operand={}", parentSchema, name, operand);
    HBaseConfiguration conf = new HBaseConfiguration();
    operand.entrySet().stream().filter(val -> val.getKey().toLowerCase().startsWith("hbase"))
      .forEach(val -> conf.set(val.getKey(), val.getValue().toString(), "HBaseSchemaFactory"));

    Class<? extends Schema> schemaClass = conf.getClass(SCHEMA_CLASS_KEY, HBaseSchema.class, Schema.class);
    try {
      Constructor<? extends Schema> ctor = schemaClass.getConstructor(SchemaPlus.class, String.class, HBaseConfiguration.class);
      return ctor.newInstance(parentSchema, name, conf);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
             InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}

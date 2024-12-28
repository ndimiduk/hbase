package org.apache.hadoop.hbase.calcite.adapter;

import org.apache.hbase.thirdparty.org.apache.calcite.rel.type.RelDataType;
import org.apache.hbase.thirdparty.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.impl.AbstractTable;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTable extends AbstractTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTable.class);

  private final TableDescriptor descriptor;

  protected HBaseTable(TableDescriptor descriptor) {
    this.descriptor = descriptor;
    LOG.info("Created table {}", this);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return null;
  }

  @Override public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("name", descriptor.getTableName().getNameAsString()).append("namespace", descriptor.getTableName().getNamespaceAsString()).toString();
  }
}

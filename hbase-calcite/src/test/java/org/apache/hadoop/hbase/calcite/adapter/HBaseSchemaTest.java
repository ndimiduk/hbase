package org.apache.hadoop.hbase.calcite.adapter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.apache.hbase.thirdparty.org.apache.calcite.jdbc.CalciteConnection;
import org.apache.hbase.thirdparty.org.apache.calcite.schema.SchemaPlus;
import org.apache.hbase.thirdparty.org.apache.calcite.util.Sources;

@RunWith(MockitoJUnitRunner.class)
public class HBaseSchemaTest {

  @Mock
  private AsyncConnection asyncConnection;

  @Mock
  private AsyncAdmin asyncAdmin;

  @Test
  public void test() throws Exception {
    Properties info = new Properties();
    info.put("model", yamlPath("test-model"));
    info.put(HBaseSchemaFactory.SCHEMA_CLASS_KEY, TestHBaseSchema.class.getCanonicalName());
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection = (CalciteConnection) conn;
      SchemaPlus testSchema = calciteConnection.getRootSchema().getSubSchema("TEST");
      assertThat(testSchema, notNullValue());
      TestHBaseSchema testHBaseSchema = testSchema.unwrap(TestHBaseSchema.class);
      assertThat(testHBaseSchema, notNullValue());
      assertThat(testHBaseSchema.setAsyncConnection(asyncConnection), nullValue());
      List<TableDescriptor> descriptors = List.of(
        TableDescriptorBuilder.newBuilder(TableName.META_TABLE_NAME).build(), TableDescriptorBuilder.newBuilder(TableName.valueOf("foo")).build(), TableDescriptorBuilder.newBuilder(TableName.valueOf("bar", "baz")).build());
      when(asyncConnection.getAdmin()).thenReturn(asyncAdmin);
      when(asyncAdmin.listNamespaces()).thenReturn(CompletableFuture.completedFuture(descriptors.stream().map(val -> val.getTableName().getNamespaceAsString()).toList()));
      when(asyncAdmin.listTableDescriptors(true)).thenReturn(CompletableFuture.completedFuture(descriptors));
      try(ResultSet schemas = conn.getMetaData().getSchemas()) {
        System.out.println("schemas:");
        printResultSet(schemas);
      }
      try(ResultSet catalogs = conn.getMetaData().getCatalogs()) {
        System.out.println("catalogs:");
        printResultSet(catalogs);
      }
      try(ResultSet tables = conn.getMetaData().getTables(null, "TEST", null, null)) {
        System.out.println("tables:");
        printResultSet(tables);
      }
    }
  }

  private String yamlPath(String model) {
    return resourcePath(model + ".yaml");
  }

  private String resourcePath(String path) {
    final URL url = Objects.requireNonNull(HBaseSchemaTest.class.getResource("/" + path));
    return Sources.of(url).file().getAbsolutePath();
  }

  private static void printResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    while (resultSet.next()) {
      for (int i = 1; i < metaData.getColumnCount(); i++) {
        if (i > 1) System.out.print(", ");
        System.out.print(metaData.getColumnName(i) + ": " + resultSet.getString(i));
      }
      System.out.println();
    }
  }
}

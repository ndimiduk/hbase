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

import io.netty.handler.codec.http.QueryStringEncoder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A support class for the "Meta Entries" section in
 * {@code resources/hbase-webapps/master/table.jsp}.
 */
@InterfaceAudience.Private
public class MetaBrowser implements Iterable<RegionReplicaInfo> {
  private static final Logger logger = LoggerFactory.getLogger(MetaBrowser.class);

  public static final String NAME_PARAM = "name";
  public static final String SCAN_LIMIT_PARAM = "scan_limit";
  public static final String SCAN_REGION_STATE_PARAM = "scan_region_state";
  public static final String SCAN_START_PARAM = "scan_start";
  public static final String SCAN_TABLE_PARAM = "scan_table";

  public static final int SCAN_LIMIT_DEFAULT = 10;
  public static final int SCAN_LIMIT_MAX = 10_000;

  private final Connection connection;
  private final HttpServletRequest request;
  private final List<String> errorMessages;
  private final String name;
  private final Integer scanLimit;
  private final RegionState.State scanRegionState;
  private final byte[] scanStart;
  private final TableName scanTable;

  public MetaBrowser(final Connection connection, final HttpServletRequest request) {
    this.connection = connection;
    this.request = request;
    this.errorMessages = new LinkedList<>();
    this.name = resolveName(request);
    this.scanLimit = resolveScanLimit(request);
    this.scanRegionState = resolveScanRegionState(request);
    this.scanStart = resolveScanStart(request);
    this.scanTable = resolveScanTable(request);
  }

  public List<String> getErrorMessages() {
    return errorMessages;
  }

  public String getName() {
    return name;
  }

  public Integer getScanLimit() {
    return scanLimit;
  }

  public byte[] getScanStart() {
    return scanStart;
  }

  public RegionState.State getScanRegionState() {
    return scanRegionState;
  }

  public TableName getScanTable() {
    return scanTable;
  }

  @Override
  public Iterator<RegionReplicaInfo> iterator() {
    try {
      return limitIterator();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public LimitIterator<RegionReplicaInfo> limitIterator() throws IOException {
    logger.debug("initiating meta scan, {}", this);

    final Table table = connection.getTable(TableName.META_TABLE_NAME);
    // TODO: buffering the entire result set seems unnecessary.
    final List<RegionReplicaInfo> results = new LinkedList<>();
    try (final ResultScanner scanner = table.getScanner(buildScan())) {
      for (final Result result : scanner) {
        results.addAll(RegionReplicaInfo.from(result));
      }
    }
    return new LimitIterator<>(
      results.iterator(), scanLimit != null ? scanLimit : SCAN_LIMIT_DEFAULT);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("scanStart", scanStart)
      .append("scanLimit", scanLimit)
      .append("scanTable", scanTable)
      .append("scanRegionState", scanRegionState)
      .toString();
  }

  private static String resolveName(final HttpServletRequest request) {
    return resolveRequestParameter(request, NAME_PARAM);
  }

  private Integer resolveScanLimit(final HttpServletRequest request) {
    final String requestValueStr = resolveRequestParameter(request, SCAN_LIMIT_PARAM);
    if (StringUtils.isBlank(requestValueStr)) { return null; }

    final Integer requestValue = tryParseInt(requestValueStr);
    if (requestValue == null) {
      errorMessages.add(buildScanLimitMalformedErrorMessage(requestValueStr));
      return null;
    }
    if (requestValue <= 0) {
      errorMessages.add(buildScanLimitLTEZero(requestValue));
      return SCAN_LIMIT_DEFAULT;
    }

    final int truncatedValue = Math.min(requestValue, SCAN_LIMIT_MAX);
    if (requestValue != truncatedValue) {
      errorMessages.add(buildScanLimitExceededErrorMessage(requestValue));
    }
    return truncatedValue;
  }

  private RegionState.State resolveScanRegionState(final HttpServletRequest request) {
    final String requestValueStr = resolveRequestParameter(request, SCAN_REGION_STATE_PARAM);
    if (requestValueStr == null) { return null; }
    final RegionState.State requestValue = tryValueOf(RegionState.State.class, requestValueStr);
    if (requestValue == null) {
      errorMessages.add(buildScanRegionStateMalformedErrorMessage(requestValueStr));
      return null;
    }
    return requestValue;
  }

  private static byte[] resolveScanStart(final HttpServletRequest request) {
    final String requestValue = resolveRequestParameter(request, SCAN_START_PARAM);
    if (requestValue == null) { return null; }
    return Bytes.toBytesBinary(requestValue);
  }

  private static TableName resolveScanTable(final HttpServletRequest request) {
    final String requestValue = resolveRequestParameter(request, SCAN_TABLE_PARAM);
    if (requestValue == null) { return null; }
    return TableName.valueOf(requestValue);
  }

  private static String resolveRequestParameter(final HttpServletRequest request,
    final String param) {
    if (request == null) { return null; }
    final String requestValueStrEnc = request.getParameter(param);
    if (StringUtils.isBlank(requestValueStrEnc)) { return null; }
    return urlDecode(requestValueStrEnc);
  }

  private static Filter buildTableFilter(final TableName tableName) {
    return new PrefixFilter(tableName.toBytes());
  }

  private static Filter buildScanRegionStateFilter(final RegionState.State state) {
    return new SingleColumnValueFilter(
      HConstants.CATALOG_FAMILY,
      HConstants.TABLE_STATE_QUALIFIER,
      CompareOperator.EQUAL,
      // use the same serialization strategy as found in MetaTableAccessor#addRegionStateToPut
      Bytes.toBytes(state.name()));
  }

  private Filter buildScanFilter() {
    if (scanTable == null && scanRegionState == null) {
      return null;
    }

    final List<Filter> filters = new ArrayList<>(2);
    if (scanTable != null) { filters.add(buildTableFilter(scanTable)); }
    if (scanRegionState != null) { filters.add(buildScanRegionStateFilter(scanRegionState)); }
    if (filters.size() == 1) { return filters.get(0); }
    return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
  }

  private Scan buildScan() {
    final Scan metaScan = new Scan()
      .addFamily(HConstants.CATALOG_FAMILY)
      .setMaxVersions(1)
      .setLimit((scanLimit != null ? scanLimit : SCAN_LIMIT_DEFAULT) + 1);
    if (scanStart != null) { metaScan.withStartRow(scanStart, false); }
    final Filter filter = buildScanFilter();
    if (filter != null) { metaScan.setFilter(filter); }
    return metaScan;
  }

  private <T> void maybeAddParam(final QueryStringEncoder encoder, final String paramName,
    final T value) {
    if (value != null) {
      encoder.addParam(paramName, value.toString());
    }
  }

  private QueryStringEncoder buildFirstPageEncoder() {
    final QueryStringEncoder encoder =
      new QueryStringEncoder(request.getRequestURI());
    maybeAddParam(encoder, NAME_PARAM, name);
    maybeAddParam(encoder, SCAN_LIMIT_PARAM, scanLimit);
    maybeAddParam(encoder, SCAN_REGION_STATE_PARAM, scanRegionState);
    maybeAddParam(encoder, SCAN_TABLE_PARAM, scanTable);
    return encoder;
  }

  public String buildFirstPageUrl() {
    return buildFirstPageEncoder().toString();
  }

  public static String buildStartParamFrom(final byte[] lastRow) {
    if (lastRow == null) { return null; }
    return urlEncode(Bytes.toStringBinary(lastRow));
  }

  public String buildNextPageUrl(final byte[] lastRow) {
    final QueryStringEncoder encoder = buildFirstPageEncoder();
    final String startRow = buildStartParamFrom(lastRow);
    maybeAddParam(encoder, SCAN_START_PARAM, startRow);
    return encoder.toString();
  }

  private static String urlEncode(final String val) {
    if (StringUtils.isEmpty(val)) { return null; }
    try {
      return URLEncoder.encode(val, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  private static String urlDecode(final String val) {
    if (StringUtils.isEmpty(val)) { return null; }
    try {
      return URLDecoder.decode(val, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  private static Integer tryParseInt(final String val) {
    if (StringUtils.isEmpty(val)) { return null; }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static <T extends Enum<T>> T tryValueOf(final Class<T> clazz,
    final String value) {
    if (clazz == null || value == null) { return null; }
    try {
      return T.valueOf(clazz, value);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String buildScanLimitExceededErrorMessage(final int requestValue) {
    return String.format(
      "Requested SCAN_LIMIT value %d exceeds maximum value %d.", requestValue, SCAN_LIMIT_MAX);
  }

  private static String buildScanLimitMalformedErrorMessage(final String requestValue) {
    return String.format(
      "Requested SCAN_LIMIT value '%s' cannot be parsed as an integer.", requestValue);
  }

  private static String buildScanLimitLTEZero(final int requestValue) {
    return String.format("Requested SCAN_LIMIT value %d is <= 0.", requestValue);
  }

  private static String buildScanRegionStateMalformedErrorMessage(final String requestValue) {
    return String.format(
      "Requested SCAN_REGION_STATE value '%s' cannot be parsed as a RegionState.", requestValue);
  }
}

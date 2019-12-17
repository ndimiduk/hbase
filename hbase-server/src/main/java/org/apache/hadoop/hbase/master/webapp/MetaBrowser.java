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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.QueryStringEncoder;

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

  private final AsyncConnection connection;
  private final HttpServletRequest request;
  private final ExecutorService pool;
  private final List<String> errorMessages;
  private final String name;
  private final Integer scanLimit;
  private final RegionState.State scanRegionState;
  private final byte[] scanStart;
  private final TableName scanTable;

  public MetaBrowser(final AsyncConnection connection, final HttpServletRequest request) {
    this.connection = connection;
    this.request = request;
    this.pool = buildThreadPool();
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
    return limitIterator();
  }

  public LimitIterator<RegionReplicaInfo> limitIterator() {
    logger.debug("initiating meta scan, {}", this);

    final AsyncTable<ScanResultConsumer> asyncTable =
      connection.getTable(TableName.META_TABLE_NAME, pool);
    // TODO: buffering the entire result set seems unnecessary.
    final List<RegionReplicaInfo> results = asyncTable.scanAll(buildScan()).join()
      .stream()
      .map(RegionReplicaInfo::from)
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
    return new LimitIterator<>(
      results.iterator(), Optional.ofNullable(scanLimit).orElse(SCAN_LIMIT_DEFAULT));
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

  private static ExecutorService buildThreadPool() {
    return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setNameFormat("MetaBrowser-%d")
      .setDaemon(true)
      .setUncaughtExceptionHandler(
        (thread, throwable) -> logger.info("Error in worker thread, {}", throwable.getMessage()))
      .build());
  }

  private static String resolveName(final HttpServletRequest request) {
    return Optional.ofNullable(request)
      .map(req -> req.getParameter(NAME_PARAM))
      .filter(StringUtils::isNotBlank)
      .map(MetaBrowser::urlDecode)
      .orElse(null);
  }

  private Integer resolveScanLimit(final HttpServletRequest request) {
    final Optional<String> requestValueStr = Optional.ofNullable(request)
      .map(req -> req.getParameter(SCAN_LIMIT_PARAM))
      .filter(StringUtils::isNotBlank);
    if (!requestValueStr.isPresent()) { return null; }

    final Integer requestValue = requestValueStr
      .flatMap(MetaBrowser::tryParseInt)
      .orElse(null);
    if (requestValue == null) {
      errorMessages.add(buildScanLimitMalformedErrorMessage(requestValueStr.get()));
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
    final Optional<String> requestValueStr = Optional.ofNullable(request)
      .map(req -> req.getParameter(SCAN_REGION_STATE_PARAM))
      .filter(StringUtils::isNotBlank)
      .map(MetaBrowser::urlDecode);
    if (!requestValueStr.isPresent()) { return null; }

    final RegionState.State requestValue = requestValueStr
      .flatMap(val -> tryValueOf(RegionState.State.class, val))
      .orElse(null);
    if (requestValue == null) {
      errorMessages.add(buildScanRegionStateMalformedErrorMessage(requestValueStr.get()));
      return null;
    }
    return requestValue;
  }

  private static byte[] resolveScanStart(final HttpServletRequest request) {
    return Optional.ofNullable(request)
      .map(req -> req.getParameter(SCAN_START_PARAM))
      .filter(StringUtils::isNotBlank)
      .map(MetaBrowser::urlDecode)
      .map(Bytes::toBytesBinary)
      .orElse(null);
  }

  private static TableName resolveScanTable(final HttpServletRequest request) {
    return Optional.ofNullable(request)
      .map(req -> req.getParameter(SCAN_TABLE_PARAM))
      .filter(StringUtils::isNotBlank)
      .map(MetaBrowser::urlDecode)
      .map(TableName::valueOf)
      .orElse(null);
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

  private Optional<Filter> buildScanFilter() {
    if (scanTable == null && scanRegionState == null) {
      return Optional.empty();
    }

    final List<Filter> filters = new ArrayList<>(2);
    Optional.ofNullable(scanTable)
      .map(MetaBrowser::buildTableFilter)
      .ifPresent(filters::add);
    Optional.ofNullable(scanRegionState)
      .map(MetaBrowser::buildScanRegionStateFilter)
      .ifPresent(filters::add);

    if (filters.size() == 1) {
      return Optional.of(filters.get(0));
    }

    return Optional.of(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters));
  }

  private Scan buildScan() {
    final Scan metaScan = new Scan()
      .addFamily(HConstants.CATALOG_FAMILY)
      .readVersions(1)
      .setLimit(Optional.ofNullable(scanLimit).orElse(SCAN_LIMIT_DEFAULT) + 1);
    Optional.ofNullable(scanStart)
      .ifPresent(startRow -> metaScan.withStartRow(startRow, false));
    buildScanFilter().ifPresent(metaScan::setFilter);
    return metaScan;
  }

  private <T> void maybeAddParam(final QueryStringEncoder encoder, final String paramName,
    final T value) {
    Optional.ofNullable(value)
      .map(Object::toString)
      .ifPresent(val -> encoder.addParam(paramName, val));
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
    return Optional.ofNullable(lastRow)
      .map(Bytes::toStringBinary)
      .map(MetaBrowser::urlEncode)
      .orElse(null);
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

  private static Optional<Integer> tryParseInt(final String val) {
    if (StringUtils.isEmpty(val)) { return Optional.empty(); }
    try {
      return Optional.of(Integer.parseInt(val));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  private static <T extends Enum<T>> Optional<T> tryValueOf(final Class<T> clazz,
    final String value) {
    if (clazz == null || value == null) { return Optional.empty(); }
    try {
      return Optional.of(value).map(val -> T.valueOf(clazz, val));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
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

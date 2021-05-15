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
package org.apache.hadoop.hbase.http.jersey;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerRequestContext;
import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerResponseContext;
import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerResponseFilter;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response.Status;

/**
 * Generate a uniform response wrapper around the Entity returned from the resource.
 */
@InterfaceAudience.Private
public class ResponseEntityMapper implements ContainerResponseFilter {
  private static final Logger logger = LoggerFactory.getLogger(ResponseEntityMapper.class);

  @Override
  public void filter(
    ContainerRequestContext requestContext,
    ContainerResponseContext responseContext
  ) throws IOException {
    final int statusCode = responseContext.getStatus();

    if (Status.OK.getStatusCode() == statusCode) {
      responseContext.setEntity(ImmutableMap.of("data", responseContext.getEntity()));
      return;
    }

    logger.warn("could not filter unrecognized response code {}.", statusCode);
    responseContext.setStatusInfo(Status.INTERNAL_SERVER_ERROR);
  }
}

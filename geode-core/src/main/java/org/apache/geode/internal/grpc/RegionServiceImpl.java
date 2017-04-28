/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.Region;
import org.apache.geode.protobuf.generated.*;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class RegionServiceImpl extends RegionServiceGrpc.RegionServiceImplBase {
  private static final Logger logger = LogService.getLogger();
  private final GemFireCacheImpl cache;

  public RegionServiceImpl(GemFireCacheImpl gemFireCache) {
    cache = gemFireCache;
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutReply> responseObserver) {
    Region region = cache.getRegion(request.getRegionName());
    PutReply.Builder replyBuilder = PutReply.newBuilder();
    if (region == null) {
      replyBuilder.setIsSuccess(false);
    } else {
      try {
        region.put(request.getKey(), request.getValue());
        replyBuilder.setIsSuccess(true);
      } catch (GemFireException e) {
        logger.error("RegionServiceImpl::put", e);
        replyBuilder.setIsSuccess(false);
      }
    }
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetReply> responseObserver) {
    Region region = cache.getRegion(request.getRegionName());
    GetReply.Builder replyBuilder = GetReply.newBuilder();
    if (region == null) {
      replyBuilder.setIsSuccess(false);
    } else {
      try {
        ByteString val = (ByteString) region.get(request.getKey());
        replyBuilder.setValue(val);
        replyBuilder.setIsSuccess(true);
      } catch (GemFireException e) {
        logger.error("RegionServiceImpl::get", e);
        replyBuilder.setIsSuccess(false);
      }
    }
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}

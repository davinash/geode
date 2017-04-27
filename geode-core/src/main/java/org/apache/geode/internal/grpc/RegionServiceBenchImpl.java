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
import org.apache.geode.generated.RegionServiceBenchMark.*;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Created by adongre on 26/4/17.
 */
public class RegionServiceBenchImpl
    extends RegionServiceBenchMarkGrpc.RegionServiceBenchMarkImplBase {
  private static final Logger logger = LogService.getLogger();
  private final GemFireCacheImpl cache;

  public RegionServiceBenchImpl(GemFireCacheImpl cache) {
    this.cache = cache;
  }

  private PdxInstance convertToBytearrayMap(List<MapKeyValueEntry> values) {
    PdxInstanceFactory pdxInstanceFactory =
        cache.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME);
    values.forEach(E -> pdxInstanceFactory.writeByteArray(E.getKey(), E.getValue().toByteArray()));
    return pdxInstanceFactory.create();
  }

  @Override
  public void putMap(PutMapRequest request, StreamObserver<PutMapReply> responseObserver) {
    Region region = cache.getRegion(request.getRegionName());
    PutMapReply.Builder replyBuilder = PutMapReply.newBuilder();
    if (region == null) {
      replyBuilder.setIsSuccess(false);
    } else {
      try {
        region.put(request.getKey(), convertToBytearrayMap(request.getMapFieldsList()));
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
  public void getMap(GetMapRequest request, StreamObserver<GetMapReply> responseObserver) {
    Region region = cache.getRegion(request.getRegionName());
    GetMapReply.Builder replyBuilder = GetMapReply.newBuilder();
    if (region == null) {
      replyBuilder.setIsSuccess(false);
    } else {
      try {
        PdxInstance val = (PdxInstance) region.get(request.getKey());
        if (val != null) {
          int index = 0;
          for (String fieldName : val.getFieldNames()) {
            byte[] v = (byte[]) val.getField(fieldName);

            replyBuilder.addMapFields(index++,
                MapKeyValueEntry.newBuilder().setKey(fieldName).setValue(ByteString.copyFrom(v)));
          }
          replyBuilder.setIsSuccess(true);
        } else {
          replyBuilder.setIsSuccess(false);
        }
      } catch (GemFireException e) {
        logger.error("RegionServiceImpl::get", e);
        replyBuilder.setIsSuccess(false);
      }
    }
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}

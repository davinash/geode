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
package org.apache.geode.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.geode.cache.*;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.generated.RegionService.*;
import org.apache.geode.generated.RegionServiceBenchMark.*;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ClientServerTest.class)
public class ClientServerGrpcDUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  private Host host = null;
  private VM vm0 = null;
  private VM vm1 = null;


  @Override
  public void preSetUp() throws Exception {
    disconnectAllFromDS();
    super.preSetUp();
  }

  @Override
  public void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
  }

  private Object startServerOn(VM vm, final String locators) {
    return vm.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        Cache c = null;
        try {
          c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = CacheFactory.create(getSystem(props));
        ((GemFireCacheImpl)c).getCacheConfig().setPdxReadSerialized(true);
        CacheServer s = c.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        s.setPort(port);
        s.start();
        return port;
      }
    });
  }

  private int getDUnitLocatorPort() {
    return DistributedTestUtils.getDUnitLocatorPort();
  }

  private int getLocatorPort() {
    if (DUnitLauncher.isLaunched()) {
      String locatorString = DUnitLauncher.getLocatorString();
      int index = locatorString.indexOf("[");
      return Integer.parseInt(locatorString.substring(index + 1, locatorString.length() - 1));
    } else {
      return getDUnitLocatorPort();
    }
  }

  private void createClientCache() {
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf = ccf.addPoolLocator("127.0.0.1", getLocatorPort());
    ccf.create();
  }

  private void createRegionOn(VM vm, final String regionName) {
    vm.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    });
  }

  @Test
  public void testGRPCOps() throws InterruptedException {
    final String REGION_NAME = "TestGrpcPRRegion";
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    createClientCache();

    createRegionOn(this.vm0, REGION_NAME);

    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

    RegionServiceGrpc.RegionServiceBlockingStub blockingStub;
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", 9050).usePlaintext(true).build();
    blockingStub = RegionServiceGrpc.newBlockingStub(channel);

    // Perform Sample PUT Operation using gRPC Client
    for (int i = 0; i < 10; i++) {
      ByteString key = ByteString.copyFrom(("Key-" + i).getBytes());
      ByteString val = ByteString.copyFrom(("Val-" + i).getBytes());

      PutReply putReply;
      try {
        putReply = blockingStub.put(
            PutRequest.newBuilder().setKey(key).setValue(val).setRegionName(REGION_NAME).build());

        assertTrue(putReply.getIsSuccess());
      } catch (StatusRuntimeException e) {
        logger.warn("RPC failed: {0}", e.getStatus());
        return;
      }
    }
    // Verify using Geode API
    this.vm0.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region r = cache.getRegion(REGION_NAME);
      for (int i = 0; i < 10; i++) {
        ByteString key = ByteString.copyFrom(("Key-" + i).getBytes());
        ByteString expectedVal = ByteString.copyFrom(("Val-" + i).getBytes());
        Assert.assertEquals(expectedVal, r.get(key));
      }
    });

    for (int i = 0; i < 10; i++) {
      ByteString key = ByteString.copyFrom(("Key-" + i).getBytes());
      ByteString expectedVal = ByteString.copyFrom(("Val-" + i).getBytes());
      GetReply getReply;
      try {
        getReply = blockingStub
            .get(GetRequest.newBuilder().setKey(key).setRegionName(REGION_NAME).build());
        assertTrue(getReply.getIsSuccess());
        assertEquals(expectedVal, getReply.getValue());
      } catch (StatusRuntimeException e) {
        logger.warn("RPC failed: {0}", e.getStatus());
        return;
      }
    }
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    clientCache.getRegion(REGION_NAME).destroyRegion();
  }


  @Test
  public void testGRPCOpsBench() throws InterruptedException {
    final String REGION_NAME = "testGRPCOpsBench";
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    createClientCache();

    createRegionOn(this.vm0, REGION_NAME);

    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

    RegionServiceBenchMarkGrpc.RegionServiceBenchMarkBlockingStub blockingStub;
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", 9050).usePlaintext(true).build();
    blockingStub = RegionServiceBenchMarkGrpc.newBlockingStub(channel);

    // Perform Sample PUT Operation using gRPC Client
    for (int i = 0; i < 10; i++) {
      ByteString key = ByteString.copyFrom(("Key-" + i).getBytes());

      PutMapRequest.Builder requestBuilder = PutMapRequest.newBuilder();

      for ( int j = 0; j < 5; j++) {
        MapKeyValueEntry.Builder mapKVBuilder = MapKeyValueEntry.newBuilder();
        mapKVBuilder.setKey("MapKey-" + j)
            .setValue(ByteString.copyFrom(("MapValue-" + j).getBytes())).build();
        requestBuilder.addMapFields(mapKVBuilder.build());
      }

      PutMapReply putMapReply;
      try {
        putMapReply = blockingStub.putMap(requestBuilder.setKey(key).setRegionName(REGION_NAME).build());
        assertTrue(putMapReply.getIsSuccess());
      } catch (StatusRuntimeException e) {
        logger.warn("RPC failed: {0}", e.getStatus());
        return;
      }
    }
    // Verify using Geode API
    this.vm0.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region r = cache.getRegion(REGION_NAME);
      for (int i = 0; i < 10; i++) {
        ByteString key = ByteString.copyFrom(("Key-" + i).getBytes());
        PdxInstanceFactory pdxInstanceFactory =
            cache.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME);
        for ( int j = 0; j < 5; j++) {
          pdxInstanceFactory.writeByteArray("MapKey-" + j,
              ByteString.copyFrom(("MapValue-" + j).getBytes()).toByteArray());
        }
        PdxInstance expectedValue = pdxInstanceFactory.create();
        Assert.assertEquals(expectedValue, r.get(key));
      }
    });

    for (int i = 0; i < 10; i++) {
      ByteString key = ByteString.copyFrom(("Key-" + i).getBytes());
      //ByteString expectedVal = ByteString.copyFrom(("Val-" + i).getBytes());
      GetMapReply getMapReply;
      try {
        getMapReply = blockingStub
            .getMap(GetMapRequest.newBuilder().setKey(key).setRegionName(REGION_NAME).build());
        assertTrue(getMapReply.getIsSuccess());
        assertEquals(5, getMapReply.getMapFieldsCount());


        PutMapRequest.Builder requestBuilder = PutMapRequest.newBuilder();
        for ( int j = 0; j < 5; j++) {
          MapKeyValueEntry.Builder mapKVBuilder = MapKeyValueEntry.newBuilder();
          mapKVBuilder.setKey("MapKey-" + j)
              .setValue(ByteString.copyFrom(("MapValue-" + j).getBytes()));
          requestBuilder.addMapFields(mapKVBuilder.build());
        }
        Assert.assertEquals(requestBuilder.getMapFieldsList(), getMapReply.getMapFieldsList());

      } catch (StatusRuntimeException e) {
        logger.warn("RPC failed: {0}", e.getStatus());
        return;
      }
    }
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    clientCache.getRegion(REGION_NAME).destroyRegion();
  }
}

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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.generated.RegionService.RegionServiceGrpc;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

@Category(ClientServerTest.class)
public class ClientServerGrpcDUnitTest extends JUnit4CacheTestCase {
  private Host host = null;
  private VM vm0 = null;
  private VM vm1 = null;
  private final String REGION_NAME = "TestGrpcPRRegion";

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

  private void createRegionOn(VM vm) {
    vm.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
    });
  }

  @Test
  public void testGRPCOps() throws InterruptedException {
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    createClientCache();

    createRegionOn(this.vm0);

    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

    RegionServiceGrpc.RegionServiceBlockingStub blockingStub;
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", 9050).usePlaintext(true).build();
    blockingStub = RegionServiceGrpc.newBlockingStub(channel);

    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);


    clientCache.getRegion(REGION_NAME).destroyRegion();
  }
}

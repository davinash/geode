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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;

@Category(ClientServerTest.class)
public class ClientServerGrpcDUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  private static Host host;
  private static VM server1;

  @Override
  public final void postSetUp() throws Exception {
    host = Host.getHost(0);
    server1 = host.getVM(2);
  }

  private int initServerCache() {
    Object[] args = new Object[]{};
    return ((Integer) server1.invoke(ClientServerGrpcDUnitTest.class, "createServerCache", args))
        .intValue();
  }

  private static Integer createServerCache()
      throws Exception {
    System.setProperty("grpc.server.port", "9050");
    Cache cache = new ClientServerGrpcDUnitTest().createCacheV(new Properties());
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.start();
    logger.info("Started server on port " + server.getPort());
    return new Integer(server.getPort());

  }

  private Cache createCacheV(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = getCache();
    assertNotNull(cache);
    return cache;
  }

  @Test
  public void testGRPCOps() throws Exception {
    createServerCache();
  }
}

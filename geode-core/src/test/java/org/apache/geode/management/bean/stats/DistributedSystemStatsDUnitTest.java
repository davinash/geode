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
package org.apache.geode.management.bean.stats;

import static com.jayway.awaitility.Awaitility.*;
import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagementTestRule;
import org.apache.geode.management.Manager;
import org.apache.geode.management.Member;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings({"unused", "serial"})
public class DistributedSystemStatsDUnitTest {

  @Manager
  private VM manager;

  @Member
  private VM[] members;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().start(true).build();

  @Test
  public void testDistributedSystemStats() throws Exception {
    this.manager.invoke("verifyMBeans", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      assertNotNull(cache);

      SystemManagementService service =
          (SystemManagementService) ManagementService.getManagementService(cache);
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
      assertNotNull(distributedSystemMXBean);

      Set<DistributedMember> otherMemberSet =
          cache.getDistributionManager().getOtherNormalDistributionManagerIds();
      assertEquals(3, otherMemberSet.size());

      for (DistributedMember member : otherMemberSet) {
        ObjectName memberMXBeanName = service.getMemberMBeanName(member);
        await().atMost(2, TimeUnit.MINUTES).until(() -> assertTrue(
            ManagementFactory.getPlatformMBeanServer().isRegistered(memberMXBeanName)));

        MemberMXBean memberMXBean = service.getMBeanProxy(memberMXBeanName, MemberMXBean.class);
        assertNotNull(memberMXBean);

        final long lastRefreshTime = service.getLastUpdateTime(memberMXBeanName);
        await().atMost(1, TimeUnit.MINUTES)
            .until(() -> assertTrue(service.getLastUpdateTime(memberMXBeanName) > lastRefreshTime));
      }
    });
  }
}

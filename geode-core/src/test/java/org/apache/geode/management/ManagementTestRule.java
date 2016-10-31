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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;

/**
 * Overriding MethodRule is only way to get {@code Object target}
 */
@SuppressWarnings("unused")
public class ManagementTestRule implements MethodRule, Serializable {

  public static Builder builder() {
    return new Builder();
  }

  private final int managersCount;
  private final int membersCount;
  private final boolean start;
  private final boolean managersFirst;
  private final boolean createManagers;
  private final boolean createMembers;

  private JUnit4CacheTestCase helper;

  private VM[] managers;
  private VM[] members;

  protected ManagementTestRule(final Builder builder) {
    this.helper = new JUnit4CacheTestCase() {};
    this.managersCount = builder.managersCount;
    this.membersCount = builder.membersCount;
    this.start = builder.start;
    this.managersFirst = builder.managersFirst;
    this.createManagers = builder.createManagers;
    this.createMembers = builder.createMembers;
  }

  public DistributedMember getDistributedMember() {
    return getCache().getDistributedSystem().getDistributedMember();
  }

  public DistributedMember getDistributedMember(final VM vm) {
    return vm.invoke("getDistributedMember", () -> getDistributedMember());
  }

  public void createManagers() {
    for (VM manager : this.managers) {
      manager.invoke(() -> createManager(true));
    }
  }

  public void createMembers() {
    for (VM member : this.members) {
      member.invoke(() -> createMember());
    }
  }

  public void createManager() {
    createManager(true);
  }

  public void createManager(final Properties properties) {
    createManager(properties, true);
  }

  public void createManager(final boolean start) {
    createManager(new Properties(), start);
  }

  public void createManager(final Properties properties, final boolean start) {
    setPropertyIfNotSet(properties, JMX_MANAGER, "true");
    setPropertyIfNotSet(properties, JMX_MANAGER_START, "false");
    setPropertyIfNotSet(properties, JMX_MANAGER_PORT, "0");
    setPropertyIfNotSet(properties, HTTP_SERVICE_PORT, "0");
    setPropertyIfNotSet(properties, ENABLE_TIME_STATISTICS, "true");
    setPropertyIfNotSet(properties, STATISTIC_SAMPLING_ENABLED, "true");

    this.helper.getCache(properties);

    if (start) {
      startManager();
    }
  }

  public void createManager(final VM managerVM) {
    managerVM.invoke("createManager", () -> createManager());
  }

  public void createManager(final VM managerVM, final boolean start) {
    managerVM.invoke("createManager", () -> createManager(start));
  }

  public void createManager(final VM managerVM, final Properties properties) {
    managerVM.invoke("createManager", () -> createManager(properties, true));
  }

  public void createManager(final VM managerVM, final Properties properties, final boolean start) {
    managerVM.invoke("createManager", () -> createManager(properties, start));
  }

  public void createMember() {
    createMember(new Properties());
  }

  public void createMember(final Properties properties) {
    setPropertyIfNotSet(properties, JMX_MANAGER, "false");
    setPropertyIfNotSet(properties, ENABLE_TIME_STATISTICS, "true");
    setPropertyIfNotSet(properties, STATISTIC_SAMPLING_ENABLED, "true");

    System.out.println("KIRK: creating " + properties.getProperty(NAME));
    this.helper.getCache(properties);
  }

  public void createMember(final VM memberVM) {
    Properties properties = new Properties();
    properties.setProperty(NAME, "memberVM-" + memberVM.getPid());
    memberVM.invoke("createMember", () -> createMember(properties));
  }

  public void createMember(final VM memberVM, final Properties properties) throws Exception {
    memberVM.invoke("createMember", () -> createMember(properties));
  }

  public Cache getCache() {
    // Cache cache = GemFireCacheImpl.getInstance();
    // if (cache != null && !cache.isClosed()) {
    // return cache;
    // }
    return this.helper.getCache();
  }

  public ClientCache getClientCache() {
    return this.helper.getClientCache(new ClientCacheFactory());
  }

  public boolean hasCache() {
    // Cache cache = GemFireCacheImpl.getInstance();
    // if (cache != null && !cache.isClosed()) {
    // return true;
    // }
    return this.helper.hasCache();
  }

  public Cache basicGetCache() {
    // Cache cache = GemFireCacheImpl.getInstance();
    // if (cache != null && !cache.isClosed()) {
    // return cache;
    // }
    return this.helper.basicGetCache();
  }

  public ManagementService getManagementService() {
    assertThat(hasCache()).isTrue();
    return ManagementService.getManagementService(basicGetCache());
  }

  public SystemManagementService getSystemManagementService() {
    assertThat(hasCache()).isTrue();
    return (SystemManagementService) ManagementService.getManagementService(basicGetCache());
  }

  public ManagementService getExistingManagementService() {
    assertThat(hasCache()).isTrue();
    return ManagementService.getExistingManagementService(basicGetCache());
  }

  public void startManager() {
    SystemManagementService service = getSystemManagementService();
    service.createManager();
    service.startManager();
  }

  public void startManager(final VM managerVM) {
    managerVM.invoke("startManager", () -> startManager());
  }

  public void stopManager() {
    if (getManagementService().isManager()) {
      getManagementService().stopManager();
    }
  }

  public void stopManager(final VM managerVM) {
    managerVM.invoke("stopManager", () -> stopManager());
  }

  public Set<DistributedMember> getOtherNormalMembers() {
    Set<DistributedMember> allMembers = new HashSet<>(getAllNormalMembers());
    allMembers.remove(getDistributedMember());
    return allMembers;
  }

  public Set<DistributedMember> getAllNormalMembers() {
    return getDistributionManager().getNormalDistributionManagerIds(); // excludes LOCATOR_DM_TYPE
  }

  private DM getDistributionManager() {
    return ((GemFireCacheImpl) getCache()).getDistributionManager();
  }

  public void disconnectAllFromDS() {
    stopManagerQuietly();
    Invoke.invokeInEveryVM("stopManager", () -> stopManagerQuietly());
    JUnit4DistributedTestCase.disconnectFromDS();
    Invoke.invokeInEveryVM("disconnectFromDS", () -> JUnit4DistributedTestCase.disconnectFromDS());
  }

  private void setPropertyIfNotSet(final Properties properties, final String key,
      final String value) {
    if (!properties.containsKey(key)) {
      properties.setProperty(key, value);
    }
  }

  private void stopManagerQuietly() {
    try {
      if (hasCache() && !basicGetCache().isClosed()) {
        stopManager();
      }
    } catch (DistributedSystemDisconnectedException | NullPointerException ignore) {
    }
  }

  @Override
  public Statement apply(final Statement base, final FrameworkMethod method, final Object target) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        setUp(target);
        try {
          base.evaluate();
        } finally {
          tearDown();
        }
      }
    };
  }

  private void setUp(final Object target) throws Exception {
    DUnitLauncher.launchIfNeeded();
    JUnit4DistributedTestCase.disconnectAllFromDS();

    int whichVM = 0;

    this.managers = new VM[this.managersCount];
    for (int i = 0; i < this.managersCount; i++) {
      this.managers[i] = getHost(0).getVM(whichVM);
      whichVM++;
    }

    this.members = new VM[this.membersCount];
    for (int i = 0; i < this.membersCount; i++) {
      this.members[i] = getHost(0).getVM(whichVM);
      whichVM++;
    }

    if (this.start) {
      start();
    }

    processAnnotations(target);
  }

  private void start() {
    if (this.createManagers && this.managersFirst) {
      createManagers();
    }
    if (this.createMembers) {
      createMembers();
    }
    if (this.createManagers && !this.managersFirst) {
      createManagers();
    }
  }

  private void tearDown() throws Exception {
    JUnit4DistributedTestCase.disconnectAllFromDS();
  }

  private void processAnnotations(final Object target) {
    try {
      Class<?> clazz = target.getClass();

      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        boolean alreadyAssigned = false;
        for (Annotation annotation : field.getAnnotations()) {
          if (annotation.annotationType().equals(Manager.class)) {
            // annotated with @Manager
            throwIfAlreadyAssigned(field, alreadyAssigned);
            assignManagerField(target, field);
            alreadyAssigned = true;
          }
          if (annotation.annotationType().equals(Member.class)) {
            // annotated with @Manager
            throwIfAlreadyAssigned(field, alreadyAssigned);
            assignMemberField(target, field);
            alreadyAssigned = true;
          }
        }
      }
    } catch (IllegalAccessException e) {
      throw new Error(e);
    }
  }

  private void throwIfAlreadyAssigned(final Field field, final boolean alreadyAssigned) {
    if (alreadyAssigned) {
      throw new IllegalStateException(
          "Field " + field.getName() + " is already annotated with " + field.getAnnotations());
    }
  }

  private void assignManagerField(final Object target, final Field field)
      throws IllegalAccessException {
    throwIfNotSameType(field, VM.class);

    field.setAccessible(true);
    if (field.getType().isArray()) {
      field.set(target, this.managers);
    } else {
      field.set(target, this.managers[0]);
    }
  }

  private void assignMemberField(final Object target, final Field field)
      throws IllegalAccessException {
    throwIfNotSameType(field, VM.class);

    field.setAccessible(true);
    if (field.getType().isArray()) {
      field.set(target, this.members);
    } else {
      field.set(target, this.members[0]);
    }
  }

  private void throwIfNotSameType(final Field field, final Class clazz) {
    if (!field.getType().equals(clazz) && // non-array
        !field.getType().getComponentType().equals(clazz)) { // array
      throw new IllegalArgumentException(
          "Field " + field.getName() + " is not same type as " + clazz.getName());
    }
  }

  public static class Builder {

    private boolean start = false;

    private boolean createManagers = true;

    private boolean createMembers = true;

    private int managersCount = 1;

    private int membersCount = 3;

    private boolean managersFirst = true;

    protected Builder() {}

    public Builder createManagers(final boolean value) {
      this.createManagers = value;
      return this;
    }

    public Builder createMembers(final boolean value) {
      this.createMembers = value;
      return this;
    }

    public Builder withManagers(final int count) {
      this.managersCount = count;
      return this;
    }

    public Builder withMembers(final int count) {
      this.membersCount = count;
      return this;
    }

    public Builder managersFirst(final boolean value) {
      this.managersFirst = value;
      return this;
    }

    public Builder start(final boolean value) {
      this.start = value;
      return this;
    }

    public ManagementTestRule build() {
      return new ManagementTestRule(this);
    }
  }
}

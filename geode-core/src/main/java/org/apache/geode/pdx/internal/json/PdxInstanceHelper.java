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
package org.apache.geode.pdx.internal.json;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;

/*
 * This class is intermediate class to create PdxInstance.
 */
public class PdxInstanceHelper implements JSONToPdxMapper {
  private static final Logger logger = LogService.getLogger();

  protected JSONToPdxMapper m_parent;
  protected PdxInstanceFactoryImpl m_pdxInstanceFactory;
  protected PdxInstance m_pdxInstance;
  protected String m_PdxName;// when pdx is member, else null if part of lists

  public PdxInstanceHelper(String className, JSONToPdxMapper parent) {
    GemFireCacheImpl gci = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    if (logger.isTraceEnabled()) {
      logger.trace("ClassName {}", className);
    }
    m_PdxName = className;
    m_parent = parent;
    m_pdxInstanceFactory =
        (PdxInstanceFactoryImpl) gci.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME, false);
  }

  public JSONToPdxMapper getParent() {
    return m_parent;
  }

  public void setPdxFieldName(String name) {
    if (logger.isTraceEnabled()) {
      logger.trace("setPdxClassName : {}", name);
    }
    m_PdxName = name;
  }

  public PdxInstance getPdxInstance() {
    return m_pdxInstance;
  }

  public String getPdxFieldName() {
    // return m_fieldName != null ? m_fieldName : "emptyclassname"; //when object is just like { }
    return m_PdxName;
  }

  public void addStringField(String fieldName, String value) {
    addField("addStringField", fieldName, value, FieldType.STRING);
  }

  public void addByteField(String fieldName, byte value) {
    addField("addByteField", fieldName, value, FieldType.BYTE);
  }

  public void addShortField(String fieldName, short value) {
    addField("addShortField", fieldName, value, FieldType.SHORT);
  }

  public void addIntField(String fieldName, int value) {
    addField("addIntField", fieldName, value, FieldType.INT);
  }

  public void addLongField(String fieldName, long value) {
    addField("addLongField", fieldName, value, FieldType.LONG);
  }

  public void addBigDecimalField(String fieldName, BigDecimal value) {
    addField("addBigDecimalField", fieldName, value, FieldType.OBJECT);
  }

  public void addBigIntegerField(String fieldName, BigInteger value) {
    addField("addBigIntegerField", fieldName, value, FieldType.OBJECT);
  }

  public void addBooleanField(String fieldName, boolean value) {
    addField("addBooleanField", fieldName, value, FieldType.BOOLEAN);
  }

  public void addFloatField(String fieldName, float value) {
    addField("addFloatField", fieldName, value, FieldType.FLOAT);
  }

  public void addDoubleField(String fieldName, double value) {
    addField("addDoubleField", fieldName, value, FieldType.DOUBLE);
  }

  public void addNullField(String fieldName) {
    addField("addNullField", fieldName, null, FieldType.OBJECT);
  }

  public void addListField(String fieldName, PdxListHelper value) {
    addField("addListField", fieldName, value.getList(), FieldType.OBJECT);
  }

  public void endListField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("endListField fieldName: {}", fieldName);
    }
  }

  public void addObjectField(String fieldName, Object value) {
    if (fieldName == null) {
      throw new IllegalStateException("addObjectField:Object should have fieldname");
    }
    addField("addObjectField", fieldName, value, FieldType.OBJECT);
  }

  public void endObjectField(String fieldName) {
    if (logger.isTraceEnabled()) {
      logger.trace("endObjectField fieldName: {}", fieldName);
    }
    m_pdxInstance = createPdxInstance();
  }

  protected PdxInstance createPdxInstance() {
    return m_pdxInstanceFactory.create();
  }

  protected void addField(String parentMethodName, String fieldName, Object value,
      FieldType fieldType) {
    if (logger.isTraceEnabled()) {
      logger.trace("{} fieldName: {}; value: {}", parentMethodName, fieldName, value);
    }
    addDataForWriting(fieldType, fieldName, value);
  }

  protected void addDataForWriting(FieldType fieldType, String fieldName, Object value) {
    writeData(fieldType, fieldName, value);
  }

  protected void writeData(FieldType fieldType, String fieldName, Object value) {
    switch (fieldType) {
      case BOOLEAN:
        m_pdxInstanceFactory.writeBoolean(fieldName, (boolean) value);
        break;
      case BYTE:
        m_pdxInstanceFactory.writeByte(fieldName, (byte) value);
        break;
      case SHORT:
        m_pdxInstanceFactory.writeShort(fieldName, (short) value);
        break;
      case INT:
        m_pdxInstanceFactory.writeInt(fieldName, (int) value);
        break;
      case LONG:
        m_pdxInstanceFactory.writeLong(fieldName, (long) value);
        break;
      case FLOAT:
        m_pdxInstanceFactory.writeFloat(fieldName, (float) value);
        break;
      case DOUBLE:
        m_pdxInstanceFactory.writeDouble(fieldName, (double) value);
        break;
      case STRING:
      case OBJECT:
        m_pdxInstanceFactory.writeObject(fieldName, value);
        break;
      default:
        new RuntimeException("Unable to convert json field: " + fieldName);
        break;
    }
  }
}

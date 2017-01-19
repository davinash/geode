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

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.TreeSet;

/*
 * This class is intermediate class to create PdxInstance.
 */
public class PdxInstanceSortedHelper extends PdxInstanceHelper {
  private static final Logger logger = LogService.getLogger();

  private TreeSet<JSONFieldHolder> sortedFields =
      new TreeSet<>(Comparator.comparing(o -> o.fieldName));

  public PdxInstanceSortedHelper(String className, JSONToPdxMapper parent) {
    super(className, parent);
  }

  private class JSONFieldHolder<T> {
    private String fieldName;
    private T value;
    private FieldType type;

    public JSONFieldHolder(String fieldName, T value, FieldType fieldType) {
      this.fieldName = fieldName;
      this.value = value;
      this.type = fieldType;
    }

    @Override
    public String toString() {
      return "JSONFieldHolder [fieldName=" + fieldName + ", value=" + value + ", type=" + type
          + "]";
    }
  }

  protected void addDataForWriting(FieldType fieldType, String fieldName, Object value) {
    sortedFields.add(new JSONFieldHolder(fieldName, value, fieldType));
  }

  @Override
  protected PdxInstance createPdxInstance() {
    for (JSONFieldHolder jsonFieldHolder : sortedFields) {
      writeData(jsonFieldHolder.type, jsonFieldHolder.fieldName, jsonFieldHolder.value);
    }
    return m_pdxInstanceFactory.create();
  }
}

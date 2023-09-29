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

package org.project.hop.workflow.actions.hash;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;

public class HashPluginMetaFunction implements Cloneable {
  public enum CalculationType implements IEnumHasCode {
    NONE("-", "-"), SHA1("SHA_1", "Sha1"), SHA256("SHA_256", "Sha256"), SHA384("SHA_384",
        "Sha384"), SHA512("SHA_512", "Sha512");

    public static String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < descriptions.length; i++) {
        descriptions[i] = values()[i].getDescription();
      }
      return descriptions;
    }

    public static CalculationType getTypeWithCode(String code) {
      for (CalculationType value : values()) {
        if (value.getCode().equals(code)) {
          return value;
        }
      }
      return NONE;
    }

    public static CalculationType getTypeWithDescription(String description) {
      for (CalculationType value : values()) {
        if (value.getDescription().equals(description)) {
          return value;
        }
      }
      return NONE;
    }

    private String code;

    private String description;

    CalculationType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }

  @SuppressWarnings("unused")
  private static final Class<?> PKG = HashPluginMeta.class; // For Translator

  @HopMetadataProperty(key = "field_name",
      injectionKeyDescription = "HashPlugin.Injection.Calculation.FieldName")
  private String fieldName;


  @HopMetadataProperty(key = "field",
      injectionKeyDescription = "HashPlugin.Injection.Calculation.Field")
  private String field;

  @HopMetadataProperty(key = "calc_type",
      injectionKeyDescription = "HashPlugin.Injection.Calculation.CalculationType",
      storeWithCode = true)
  private CalculationType calcType;

  @HopMetadataProperty(key = "hex_result",
      injectionKeyDescription = "HashPlugin.Injection.Calculation.HexResult", storeWithCode = true)
  private boolean hexResult;

  public HashPluginMetaFunction() {
    this.calcType = CalculationType.NONE;
  }

  public HashPluginMetaFunction(HashPluginMetaFunction f) {
    this.fieldName = f.fieldName;
    this.field = f.field;
    this.calcType = f.calcType;
    this.hexResult = f.hexResult;
  }

  /**
   * @param fieldName out field name
   * @param calcType calculation type, see CALC_* set of constants defined
   */
  public HashPluginMetaFunction(String fieldName, String field, CalculationType calcType,
      boolean hexResult) {
    this.fieldName = fieldName;
    this.field = field;
    this.calcType = calcType;
    this.hexResult = hexResult;
  }

  @Override
  public HashPluginMetaFunction clone() {
    return new HashPluginMetaFunction(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HashPluginMetaFunction that = (HashPluginMetaFunction) o;
    return Objects.equals(fieldName, that.fieldName) && Objects.equals(field, that.field)
        && calcType == that.calcType && hexResult == that.hexResult;
  }

  /**
   * Gets calcType
   *
   * @return value of calcType
   */
  public CalculationType getCalcType() {
    return calcType;
  }

  public String getField() {
    // TODO Auto-generated method stub
    return field;
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, calcType, hexResult);
  }

  /**
   * Gets hexResult
   *
   * @return value of hexResult
   */
  public boolean isHexResult() {
    return hexResult;
  }

  /** @param calcType The calcType to set */
  public void setCalcType(CalculationType calcType) {
    this.calcType = calcType;
  }

  /** @param fieldName The fieldName to set */
  public void setField(String field) {
    this.field = field;
  }

  /** @param fieldName The fieldName to set */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /** @param hexResult The hexResult to set */
  public void setHexResult(boolean hexResult) {
    this.hexResult = hexResult;
  }
}

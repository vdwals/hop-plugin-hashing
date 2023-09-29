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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

// https://github.com/apache/hop/blob/master/plugins/transforms/calculator/src/main/java/org/apache/hop/pipeline/transforms/calculator/Calculator.java

public class HashPlugin extends BaseTransform<HashPluginMeta, HashPluginData> {
  public class FieldIndexes {
    public int indexName;
    public int indexA;
  }

  private static final Class<?> PKG = HashPlugin.class; // Needed by Translator

  public HashPlugin(TransformMeta transformMeta, HashPluginMeta meta, HashPluginData data,
      int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * @param inputRowMeta the input row metadata
   * @param r the input row (data)
   * @return A row including the calculations, excluding the temporary values
   * @throws HopValueException in case there is a calculation error.
   * @throws HopPluginException
   */
  private Object[] calcFields(IRowMeta inputRowMeta, Object[] r)
      throws HopValueException, HopFileNotFoundException, HopPluginException {
    // First copy the input data to the new result...
    Object[] calcData = RowDataUtil.resizeArray(r, data.getCalcRowMeta().size());

    for (int i = 0, index = inputRowMeta.size() + i; i < meta.getFunctions().size(); i++, index++) {
      HashPluginMetaFunction fn = meta.getFunctions().get(i);
      if (!Utils.isEmpty(fn.getFieldName())) {
        IValueMeta inputMeta = null;
        Object inputData = null;

        if (data.getFieldIndexes()[i].indexA >= 0) {
          inputMeta = data.getCalcRowMeta().getValueMeta(data.getFieldIndexes()[i].indexA);

          inputData = calcData[data.getFieldIndexes()[i].indexA];
        }

        IValueMeta convertMeta = data.getValueMetaFor(IValueMeta.TYPE_STRING, "String");
        String convertData = (String) convertMeta.convertData(inputMeta, inputData);

        switch (fn.getCalcType()) {
          case NONE:
            break;
          case SHA1:
            calcData[index] =
                fn.isHexResult() ? DigestUtils.sha1Hex(convertData) : DigestUtils.sha1(convertData);
            break;
          case SHA256:
            calcData[index] = fn.isHexResult() ? DigestUtils.sha256Hex(convertData)
                : DigestUtils.sha256(convertData);
            break;
          case SHA384:
            calcData[index] = fn.isHexResult() ? DigestUtils.sha384Hex(convertData)
                : DigestUtils.sha384(convertData);
            break;
          case SHA512:
            calcData[index] = fn.isHexResult() ? DigestUtils.sha512Hex(convertData)
                : DigestUtils.sha512(convertData);
            break;
          default:
            throw new HopValueException(
                BaseMessages.getString(PKG, "HashPlugin.Log.UnknownCalculationType")
                    + fn.getCalcType());
        }
      }
    }

    // OK, now we should refrain from adding the temporary fields to the result.
    // So we remove them.
    //
    return calcData;
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if (r == null) { // no more input to be expected...
      setOutputDone();
      data.clearValuesMetaMapping();
      return false;
    }

    if (first) {
      first = false;
      data.setOutputRowMeta(getInputRowMeta().clone());
      meta.getFields(data.getOutputRowMeta(), getTransformName(), null, null, this,
          metadataProvider);

      // get all metadata, including source rows and temporary fields.
      data.setCalcRowMeta(meta.getAllFields(getInputRowMeta()));

      data.setFieldIndexes(new FieldIndexes[meta.getFunctions().size()]);

      // Calculate the indexes of the values and arguments in the target data or temporary data
      // We do this in advance to save time later on.
      //
      // CHECKSTYLE:Indentation:OFF
      for (int i = 0; i < meta.getFunctions().size(); i++) {
        HashPluginMetaFunction function = meta.getFunctions().get(i);
        data.getFieldIndexes()[i] = new FieldIndexes();

        if (Utils.isEmpty(function.getFieldName())) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "HashPlugin.Error.NoNameField", "" + (i + 1)));
        }
        data.getFieldIndexes()[i].indexName =
            data.getCalcRowMeta().indexOfValue(function.getFieldName());
        if (data.getFieldIndexes()[i].indexName < 0) {
          // Nope: throw an exception
          throw new HopTransformException(BaseMessages.getString(PKG,
              "HashPlugin.Error.UnableFindField", function.getFieldName(), "" + (i + 1)));
        }

        if (Utils.isEmpty(function.getField())) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "HashPlugin.Error.NoNameField", "" + (i + 1)));
        }
        data.getFieldIndexes()[i].indexA = data.getCalcRowMeta().indexOfValue(function.getField());
        if (data.getFieldIndexes()[i].indexA < 0) {
          // Nope: throw an exception
          throw new HopTransformException(BaseMessages.getString(PKG,
              "HashPlugin.Error.NoNameField", function.getFieldName(), "" + (i + 1)));
        }
      }

    }

    if (log.isRowLevel()) {
      logRowlevel(BaseMessages.getString(PKG, "HashPlugin.Log.ReadRow") + getLinesRead() + " : "
          + getInputRowMeta().getString(r));
    }

    try {
      Object[] row = calcFields(getInputRowMeta(), r);
      putRow(data.getOutputRowMeta(), row); // copy row to possible alternate rowset(s).

      if (log.isRowLevel()) {
        logRowlevel("Wrote row #" + getLinesWritten() + " : " + getInputRowMeta().getString(r));
      }
      if (checkFeedback(getLinesRead()) && log.isBasic()) {
        logBasic(BaseMessages.getString(PKG, "HashPlugin.Log.Linenr", "" + getLinesRead()));
      }
    } catch (HopFileNotFoundException e) {
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG,
          "Calculator.ErrorInTransformRunning" + " : " + e.getMessage()));
      throw new HopTransformException(
          BaseMessages.getString(PKG, "HashPlugin.ErrorInTransformRunning"), e);
    }
    return true;
  }
}

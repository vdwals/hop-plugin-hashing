package org.project.hop.workflow.actions.hash;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class HashPluginData extends BaseTransformData implements ITransformData {
  private IRowMeta outputRowMeta;
  private IRowMeta calcRowMeta;

  private HashPlugin.FieldIndexes[] fieldIndexes;

  private int[] tempIndexes;

  private final Map<Integer, IValueMeta> resultMetaMapping;

  public HashPluginData() {
    resultMetaMapping = new HashMap<>();
  }

  public void clearValuesMetaMapping() {
    resultMetaMapping.clear();
  }

  public IRowMeta getCalcRowMeta() {
    return calcRowMeta;
  }

  public HashPlugin.FieldIndexes[] getFieldIndexes() {
    return fieldIndexes;
  }

  public IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  public int[] getTempIndexes() {
    return tempIndexes;
  }

  public IValueMeta getValueMetaFor(int resultType, String name) throws HopPluginException {
    // don't need any synchronization as data instance belongs only to one transform instance
    IValueMeta meta = resultMetaMapping.get(resultType);
    if (meta == null) {
      meta = ValueMetaFactory.createValueMeta(name, resultType);
      resultMetaMapping.put(resultType, meta);
    }
    return meta;
  }

  public void setCalcRowMeta(IRowMeta calcRowMeta) {
    this.calcRowMeta = calcRowMeta;
  }

  public void setFieldIndexes(HashPlugin.FieldIndexes[] fieldIndexes) {
    this.fieldIndexes = fieldIndexes;
  }

  public void setOutputRowMeta(IRowMeta outputRowMeta) {
    this.outputRowMeta = outputRowMeta;
  }

  public void setTempIndexes(int[] tempIndexes) {
    this.tempIndexes = tempIndexes;
  }
}

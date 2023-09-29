package org.project.hop.workflow.actions.hash;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(id = "HASH", name = "i18n::HashPlugin.Name",
    description = "i18n::HashPlugin.Description", image = "sample.svg",
    categoryDescription = "HashPlugin.Category",
    documentationUrl = "" /* url to your documentation */)
public class HashPluginMeta extends BaseTransformMeta<HashPlugin, HashPluginData> {

  private static final Class<?> PKG = HashPluginMeta.class; // Needed by Translator

  /** The calculations to be performed */
  @HopMetadataProperty(key = "calculation", injectionGroupKey = "Calculations",
      injectionGroupDescription = "HashPlugin.Injection.Calculations")
  private List<HashPluginMetaFunction> functions;

  public HashPluginMeta() {
    this.functions = new ArrayList<>();
  }

  @Override
  public void check(List<ICheckResult> remarks, PipelineMeta pipelineMeta,
      TransformMeta transformMeta, IRowMeta prev, String[] input, String[] output, IRowMeta info,
      IVariables variables, IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK,
          BaseMessages.getString(PKG, "HashPlugin.CheckResult.ExpectedInputOk"), transformMeta);
      remarks.add(cr);

      if (prev == null || prev.size() == 0) {
        cr = new CheckResult(ICheckResult.TYPE_RESULT_WARNING,
            BaseMessages.getString(PKG, "HashPlugin.CheckResult.ExpectedInputError"),
            transformMeta);
      } else {
        cr = new CheckResult(ICheckResult.TYPE_RESULT_OK,
            BaseMessages.getString(PKG, "HashPlugin.CheckResult.FieldsReceived", "" + prev.size()),
            transformMeta);
      }
    } else {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(PKG, "HashPlugin.CheckResult.ExpectedInputError"), transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public HashPluginMeta clone() {
    HashPluginMeta meta = new HashPluginMeta();

    for (HashPluginMetaFunction function : functions) {
      meta.getFunctions().add(new HashPluginMetaFunction(function));
    }

    return meta;
  }

  public IRowMeta getAllFields(IRowMeta inputRowMeta) {
    IRowMeta rowMeta = inputRowMeta.clone();

    for (HashPluginMetaFunction calculation : getFunctions()) {
      if (!Utils.isEmpty(calculation.getFieldName())) { // It's a new field!
        IValueMeta v = getValueMeta(calculation, null);
        rowMeta.addValueMeta(v);
      }
    }
    return rowMeta;
  }

  @Override
  public void getFields(IRowMeta row, String origin, IRowMeta[] info, TransformMeta nextTransform,
      IVariables variables, IHopMetadataProvider metadataProvider) throws HopTransformException {
    for (HashPluginMetaFunction calculation : functions) {
      // a
      // new
      // field!
      IValueMeta v = getValueMeta(calculation, origin);
      row.addValueMeta(v);
    }
  }

  /**
   * Gets calculations
   *
   * @return value of calculations
   */
  public List<HashPluginMetaFunction> getFunctions() {
    return functions;
  }

  private IValueMeta getValueMeta(HashPluginMetaFunction fn, String origin) {
    IValueMeta v;
    try {
      v = ValueMetaFactory.createValueMeta(fn.getFieldName(),
          fn.isHexResult() ? IValueMeta.TYPE_STRING : IValueMeta.TYPE_BINARY);
    } catch (Exception ex) {
      return null;
    }
    v.setOrigin(origin);
    v.setComments(fn.getCalcType().getDescription());

    return v;
  }

  /** @param functions The calculations to set */
  public void setFunctions(List<HashPluginMetaFunction> functions) {
    this.functions = functions;
  }
}

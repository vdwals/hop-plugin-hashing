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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.project.hop.workflow.actions.hash.HashPluginMetaFunction.CalculationType;

public class HashPluginDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = HashPluginDialog.class; // Needed by Translator

  private final HashPluginMeta input;

  private final HashPluginMeta originalMeta;

  private Text wTransformName;

  private TableView wFields;
  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] colinf;

  public HashPluginDialog(Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta,
      String sname) {
    super(parent, variables, (BaseTransformMeta<?, ?>) in, pipelineMeta, sname);
    input = (HashPluginMeta) in;
    originalMeta = input.clone();
  }

  /** Cancel the dialog. */
  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  /** Copy information from the meta-data currentMeta to the dialog fields. */
  public void getData() {
    for (int i = 0; i < input.getFunctions().size(); i++) {
      HashPluginMetaFunction fn = input.getFunctions().get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(fn.getFieldName(), ""));
      item.setText(2, Const.NVL(fn.getField(), ""));
      item.setText(3, Const.NVL(fn.getCalcType().getDescription(), ""));
      item.setText(4, fn.isHexResult() ? BaseMessages.getString(PKG, "System.Combo.Yes")
          : BaseMessages.getString(PKG, "System.Combo.No"));
    }

    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private Image getImage() {
    return SwtSvgImageUtil.getImage(shell.getDisplay(), getClass().getClassLoader(), "sample.svg",
        ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE);
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    input.getFunctions().clear();

    for (TableItem item : wFields.getNonEmptyItems()) {

      String fieldName = item.getText(1);
      String field = item.getText(2);
      CalculationType calcType = CalculationType.getTypeWithDescription(item.getText(3));
      boolean hexResult =
          BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(4));

      input.getFunctions().add(new HashPluginMetaFunction(fieldName, field, calcType, hexResult));
    }

    if (!originalMeta.equals(input)) {
      input.setChanged();
      changed = input.hasChanged();
    }

    dispose();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "HashPlugin.Title"));


    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    int fdMargin = 15;

    // The buttons at the bottom of the dialog
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // Image
    Label wIcon = new Label(shell, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment(0, 0);
    fdlIcon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlIcon);
    PropsUi.setLook(wIcon);

    // TransformName line
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    PropsUi.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.bottom = new FormAttachment(wIcon, 0, SWT.CENTER);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(wlTransformName, margin);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(wIcon, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Draw line separator
    Label separator = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(0, 0);
    fdSeparator.top = new FormAttachment(wIcon, 0);
    fdSeparator.right = new FormAttachment(100, 0);
    separator.setLayoutData(fdSeparator);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "HashPlugin.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    wlFields.setLayoutData(fdlFields);

    final int nrFieldsRows = input.getFunctions().size();

    colinf = new ColumnInfo[] {
        new ColumnInfo(BaseMessages.getString(PKG, "HashPlugin.NewFieldColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT, false),
        new ColumnInfo(BaseMessages.getString(PKG, "HashPlugin.FieldColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {""}, false),
        new ColumnInfo(BaseMessages.getString(PKG, "HashPlugin.CalculationColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, CalculationType.getDescriptions()),
        new ColumnInfo(BaseMessages.getString(PKG, "HashPlugin.Hex.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, BaseMessages.getString(PKG, "System.Combo.No"),
            BaseMessages.getString(PKG, "System.Combo.Yes"))};

    // Draw line separator
    Label hSeparator = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSeparator = new FormData();
    fdhSeparator.left = new FormAttachment(0, 0);
    fdhSeparator.right = new FormAttachment(100, 0);
    fdhSeparator.bottom = new FormAttachment(wOk, -fdMargin);
    hSeparator.setLayoutData(fdhSeparator);

    wFields = new TableView(variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf,
        nrFieldsRows, lsMod, props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(hSeparator, -fdMargin);
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background
    //
    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
      if (transformMeta != null) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

          // Remember these fields...
          for (int i = 0; i < row.size(); i++) {
            inputFields.add(row.getValueMeta(i).getName());
          }
          setComboBoxes();
        } catch (HopException e) {
          logError(BaseMessages.getString(PKG, "HashPlugin.Log.UnableToFindInput"));
        }
      }
    };
    new Thread(runnable).start();

    wFields.addModifyListener(arg0 ->
    // Now set the combo's
    shell.getDisplay().asyncExec(this::setComboBoxes));

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final List<String> fields = new ArrayList<>(inputFields);

    shell.getDisplay().syncExec(() -> {
      // Add the newly create fields.
      //
      int nrNonEmptyFields = wFields.nrNonEmpty();
      for (int i = 0; i < nrNonEmptyFields; i++) {
        TableItem item = wFields.getNonEmpty(i);
        fields.add(item.getText(1));
      }
    });

    String[] fieldNames = fields.toArray(new String[0]);
    colinf[1].setComboValues(fieldNames);
  }
}

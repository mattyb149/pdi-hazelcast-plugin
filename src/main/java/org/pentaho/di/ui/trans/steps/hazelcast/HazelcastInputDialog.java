/*******************************************************************************
 *
 * Copyright (C) 2014-2015 by Matt Burgess
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.ui.trans.steps.hazelcast;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.hazelcast.HazelcastInputMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.TextVar;

import java.net.InetSocketAddress;
import java.util.Set;

public class HazelcastInputDialog extends BaseHazelcastDialog {
    private static Class<?> PKG = HazelcastInputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

    private HazelcastInputMeta input;
    private boolean gotPreviousFields = false;
    private RowMetaInterface previousFields;

    private Label wlStructName;
    private TextVar wStructName;
    private FormData fdlStructName, fdStructName;

    private Label wlKeyField;
    private CCombo wKeyField;
    private FormData fdlKeyField, fdKeyField;

    private Label wlKeyTypeField;
    private CCombo wKeyTypeField;
    private FormData fdlKeyTypeField, fdKeyTypeField;

    private Label wlValueField;
    private TextVar wValueField;
    private FormData fdlValueField, fdValueField;

    private Label wlValueTypeField;
    private CCombo wValueTypeField;
    private FormData fdlValueTypeField, fdValueTypeField;

    private Label wlGroupName;
    private TextVar wGroupName;
    private FormData fdlGroupName, fdGroupName;

    private Label wlGroupPassword;
    private TextVar wGroupPassword;
    private FormData fdlGroupPassword, fdGroupPassword;


    public HazelcastInputDialog(Shell parent, Object in, TransMeta tr, String sname) {
        super(parent, (BaseStepMeta) in, tr, sname);
        input = (HazelcastInputMeta) in;
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        setShellImage(shell, input);

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
        };
        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.Stepname.Label"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.right = new FormAttachment(middle, -margin);
        fdlStepname.top = new FormAttachment(0, margin);
        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);

        // Map field
        wlStructName = new Label(shell, SWT.RIGHT);
        wlStructName.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.StructureName.Label"));
        props.setLook(wlStructName);
        fdlStructName = new FormData();
        fdlStructName.left = new FormAttachment(0, 0);
        fdlStructName.right = new FormAttachment(middle, -margin);
        fdlStructName.top = new FormAttachment(wStepname, margin);
        wlStructName.setLayoutData(fdlStructName);
        wStructName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wStructName);
        wStructName.addModifyListener(lsMod);
        fdStructName = new FormData();
        fdStructName.left = new FormAttachment(middle, 0);
        fdStructName.top = new FormAttachment(wStepname, margin);
        fdStructName.right = new FormAttachment(100, 0);
        wStructName.setLayoutData(fdStructName);

        // GroupName field
        wlGroupName = new Label(shell, SWT.RIGHT);
        wlGroupName.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.GroupName.Label"));
        props.setLook(wlGroupName);
        fdlGroupName = new FormData();
        fdlGroupName.left = new FormAttachment(0, 0);
        fdlGroupName.right = new FormAttachment(middle, -margin);
        fdlGroupName.top = new FormAttachment(wStructName, margin);
        wlGroupName.setLayoutData(fdlGroupName);
        wGroupName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wGroupName);
        wGroupName.addModifyListener(lsMod);
        fdGroupName = new FormData();
        fdGroupName.left = new FormAttachment(middle, 0);
        fdGroupName.top = new FormAttachment(wStructName, margin);
        fdGroupName.right = new FormAttachment(100, 0);
        wGroupName.setLayoutData(fdGroupName);

        // GroupPassword field
        wlGroupPassword = new Label(shell, SWT.RIGHT);
        wlGroupPassword.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.GroupPassword.Label"));
        props.setLook(wlGroupPassword);
        fdlGroupPassword = new FormData();
        fdlGroupPassword.left = new FormAttachment(0, 0);
        fdlGroupPassword.right = new FormAttachment(middle, -margin);
        fdlGroupPassword.top = new FormAttachment(wGroupName, margin);
        wlGroupPassword.setLayoutData(fdlGroupPassword);
        wGroupPassword = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wGroupPassword);
        wGroupPassword.addModifyListener(lsMod);
        fdGroupPassword = new FormData();
        fdGroupPassword.left = new FormAttachment(middle, 0);
        fdGroupPassword.top = new FormAttachment(wGroupName, margin);
        fdGroupPassword.right = new FormAttachment(100, 0);
        wGroupPassword.setLayoutData(fdGroupPassword);

        // Key field
        wlKeyField = new Label(shell, SWT.RIGHT);
        wlKeyField.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.KeyField.Label"));
        props.setLook(wlKeyField);
        fdlKeyField = new FormData();
        fdlKeyField.left = new FormAttachment(0, 0);
        fdlKeyField.right = new FormAttachment(middle, -margin);
        fdlKeyField.top = new FormAttachment(wGroupPassword, margin);
        wlKeyField.setLayoutData(fdlKeyField);
        wKeyField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wKeyField);
        wKeyField.addModifyListener(lsMod);
        fdKeyField = new FormData();
        fdKeyField.left = new FormAttachment(middle, 0);
        fdKeyField.top = new FormAttachment(wGroupPassword, margin);
        fdKeyField.right = new FormAttachment(100, 0);
        wKeyField.setLayoutData(fdKeyField);
        wKeyField.addFocusListener(new FocusListener() {
            public void focusLost(org.eclipse.swt.events.FocusEvent e) {
            }

            public void focusGained(org.eclipse.swt.events.FocusEvent e) {
                Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                shell.setCursor(busy);
                getFieldsInto(wKeyField);
                shell.setCursor(null);
                busy.dispose();
            }
        });

        // Key Type field
        wlKeyTypeField = new Label(shell, SWT.RIGHT);
        wlKeyTypeField.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.KeyTypeField.Label"));
        props.setLook(wlKeyTypeField);
        fdlKeyTypeField = new FormData();
        fdlKeyTypeField.left = new FormAttachment(0, 0);
        fdlKeyTypeField.right = new FormAttachment(middle, -margin);
        fdlKeyTypeField.top = new FormAttachment(wKeyField, margin);
        wlKeyTypeField.setLayoutData(fdlKeyTypeField);
        wKeyTypeField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wKeyTypeField);
        wKeyTypeField.addModifyListener(lsMod);
        fdKeyTypeField = new FormData();
        fdKeyTypeField.left = new FormAttachment(middle, 0);
        fdKeyTypeField.top = new FormAttachment(wKeyField, margin);
        fdKeyTypeField.right = new FormAttachment(100, 0);
        wKeyTypeField.setLayoutData(fdKeyTypeField);
        wKeyTypeField.setItems(ValueMeta.getAllTypes());


        // Value field
        wlValueField = new Label(shell, SWT.RIGHT);
        wlValueField.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.ValueField.Label"));
        props.setLook(wlValueField);
        fdlValueField = new FormData();
        fdlValueField.left = new FormAttachment(0, 0);
        fdlValueField.right = new FormAttachment(middle, -margin);
        fdlValueField.top = new FormAttachment(wKeyTypeField, margin);
        wlValueField.setLayoutData(fdlValueField);
        wValueField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wValueField);
        wValueField.addModifyListener(lsMod);
        fdValueField = new FormData();
        fdValueField.left = new FormAttachment(middle, 0);
        fdValueField.top = new FormAttachment(wKeyTypeField, margin);
        fdValueField.right = new FormAttachment(100, 0);
        wValueField.setLayoutData(fdValueField);

        // Value Type field
        wlValueTypeField = new Label(shell, SWT.RIGHT);
        wlValueTypeField.setText(BaseMessages.getString(PKG, "HazelcastInputDialog.ValueTypeField.Label"));
        props.setLook(wlValueTypeField);
        fdlValueTypeField = new FormData();
        fdlValueTypeField.left = new FormAttachment(0, 0);
        fdlValueTypeField.right = new FormAttachment(middle, -margin);
        fdlValueTypeField.top = new FormAttachment(wValueField, margin);
        wlValueTypeField.setLayoutData(fdlValueTypeField);
        wValueTypeField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wValueTypeField);
        wValueTypeField.addModifyListener(lsMod);
        fdValueTypeField = new FormData();
        fdValueTypeField.left = new FormAttachment(middle, 0);
        fdValueTypeField.top = new FormAttachment(wValueField, margin);
        fdValueTypeField.right = new FormAttachment(100, 0);
        wValueTypeField.setLayoutData(fdValueTypeField);
        wValueTypeField.setItems(ValueMeta.getAllTypes());


        // Servers
        addServerUI(shell, wValueTypeField, props, transMeta, lsMod, 5 /* TODO */, SWT.NONE);


        // Some buttons
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOK, wCancel}, margin, null);

        // Add listeners
        lsCancel = new Listener() {
            public void handleEvent(Event e) {
                cancel();
            }
        };
        lsOK = new Listener() {
            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        wStepname.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        // Set the shell size, based upon previous time...
        setSize();

        getData();
        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return stepname;
    }

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    public void getData() {
        if (!Const.isEmpty(input.getStructureName())) {
            wStructName.setText(input.getStructureName());
        }
        if (!Const.isEmpty(input.getKeyFieldName())) {
            wKeyField.setText(input.getKeyFieldName());
        }
        if (!Const.isEmpty(input.getKeyTypeName())) {
            wKeyTypeField.setText(input.getKeyTypeName());
        }
        if (!Const.isEmpty(input.getValueFieldName())) {
            wValueField.setText(input.getValueFieldName());
        }
        if (!Const.isEmpty(input.getValueTypeName())) {
            wValueTypeField.setText(input.getValueTypeName());
        }
        if (!Const.isEmpty(input.getGroupName())) {
            wGroupName.setText(input.getGroupName());
        }
        if (!Const.isEmpty(input.getGroupPassword())) {
            wGroupPassword.setText(input.getGroupPassword());
        }

        int i = 0;
        Set<InetSocketAddress> servers = input.getServers();
        if (servers != null) {
            for (InetSocketAddress addr : input.getServers()) {

                TableItem item = wServers.getNonEmpty(i++);
                int col = 1;

                item.setText(col++, addr.getHostName());
                item.setText(col++, Integer.toString(addr.getPort()));
            }
        }

        wServers.setRowNums();
        wServers.optWidth(true);

        wStepname.selectAll();
        wStepname.setFocus();
    }

    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    private void ok() {
        if (Const.isEmpty(wStepname.getText())) {
            return;
        }

        stepname = wStepname.getText(); // return value
        input.setStructureName(wStructName.getText());
        input.setKeyFieldName(wKeyField.getText());
        input.setKeyTypeName(wKeyTypeField.getText());
        input.setValueFieldName(wValueField.getText());
        input.setValueTypeName(wValueTypeField.getText());
        input.setGroupName(wGroupName.getText());
        input.setGroupPassword(wGroupPassword.getText());

        int nrServers = wServers.nrNonEmpty();

        input.allocate(nrServers);

        Set<InetSocketAddress> servers = input.getServers();

        for (int i = 0; i < nrServers; i++) {
            TableItem item = wServers.getNonEmpty(i);
            servers.add(new InetSocketAddress(item.getText(1), Integer.parseInt(item.getText(2))));
        }
        input.setServers(servers);

        dispose();
    }

    private void getFieldsInto(CCombo fieldCombo) {
        try {
            if (!gotPreviousFields) {
                previousFields = transMeta.getPrevStepFields(stepname);
            }

            String field = fieldCombo.getText();

            if (previousFields != null) {
                fieldCombo.setItems(previousFields.getFieldNames());
            }

            if (field != null) {
                fieldCombo.setText(field);
            }
            gotPreviousFields = true;

        } catch (KettleException ke) {
            new ErrorDialog(shell, BaseMessages.getString(PKG, "HazelcastInputDialog.FailedToGetFields.DialogTitle"),
                    BaseMessages.getString(PKG, "HazelcastInputDialog.FailedToGetFields.DialogMessage"), ke);
        }
    }
}

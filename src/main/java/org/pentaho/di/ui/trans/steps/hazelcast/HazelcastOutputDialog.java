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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.hazelcast.HazelcastOutputMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class HazelcastOutputDialog extends BaseHazelcastDialog {
  private static Class<?> PKG = HazelcastOutputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private HazelcastOutputMeta input;
  private boolean gotPreviousFields = false;
  private RowMetaInterface previousFields;

  private Label wlStructureName;
  private TextVar wStructureName;
  private FormData fdlStructureName, fdStructureName;

  private Label wlStructureType;
  private CCombo wStructureType;
  private FormData fdlStructureType, fdStructureType;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private Label wlExpirationTime;
  private TextVar wExpirationTime;
  private FormData fdlExpirationTime, fdExpirationTime;

  public HazelcastOutputDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (HazelcastOutputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    Control lastControl = wStepname;

    // Structure name
    wlStructureName = new Label( shell, SWT.RIGHT );
    wlStructureName.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.StructureName.Label" ) );
    props.setLook( wlStructureName );
    fdlStructureName = new FormData();
    fdlStructureName.left = new FormAttachment( 0, 0 );
    fdlStructureName.right = new FormAttachment( middle, -margin );
    fdlStructureName.top = new FormAttachment( wStepname, margin );
    wlStructureName.setLayoutData( fdlStructureName );
    wStructureName = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStructureName );
    wStructureName.addModifyListener( lsMod );
    fdStructureName = new FormData();
    fdStructureName.left = new FormAttachment( middle, 0 );
    fdStructureName.top = new FormAttachment( wStepname, margin );
    fdStructureName.right = new FormAttachment( 100, 0 );
    wStructureName.setLayoutData( fdStructureName );

    lastControl = wStructureName;

    // TODO Structure type
    wlStructureType = new Label( shell, SWT.RIGHT );
    wlStructureType.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.StructureType.Label" ) );
    props.setLook( wlStructureType );
    fdlStructureType = new FormData();
    fdlStructureType.left = new FormAttachment( 0, 0 );
    fdlStructureType.top = new FormAttachment( lastControl, margin );
    fdlStructureType.right = new FormAttachment( middle, -margin );
    wlStructureType.setLayoutData( fdlStructureType );
    wStructureType = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wStructureType.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.StructureType.Label" ) );
    props.setLook( wStructureType );

    wStructureType.setItems( getStructureTypes() );
    wStructureType.addModifyListener( lsMod );
    fdStructureType = new FormData();
    fdStructureType.left = new FormAttachment( middle, 0 );
    fdStructureType.top = new FormAttachment( wStructureName, margin );
    fdStructureType.right = new FormAttachment( 100, 0 );
    wStructureType.setLayoutData( fdStructureType );

    lastControl = wStructureType;

    // Expiration field
    wlExpirationTime = new Label( shell, SWT.RIGHT );
    wlExpirationTime.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.ExpirationTime.Label" ) );
    props.setLook( wlExpirationTime );
    fdlExpirationTime = new FormData();
    fdlExpirationTime.left = new FormAttachment( 0, 0 );
    fdlExpirationTime.right = new FormAttachment( middle, -margin );
    fdlExpirationTime.top = new FormAttachment( lastControl, margin );
    wlExpirationTime.setLayoutData( fdlExpirationTime );
    wExpirationTime = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExpirationTime );
    wExpirationTime.addModifyListener( lsMod );
    fdExpirationTime = new FormData();
    fdExpirationTime.left = new FormAttachment( middle, 0 );
    fdExpirationTime.top = new FormAttachment( lastControl, margin );
    fdExpirationTime.right = new FormAttachment( 100, 0 );
    wExpirationTime.setLayoutData( fdExpirationTime );

    lastControl = wExpirationTime;

    // Servers
    addServerUI( shell, wExpirationTime, props, transMeta, lsMod, 5 /* TODO */, SWT.NONE );
    lastControl = wServers;

    // Fields
    ColumnInfo[] colinf = new ColumnInfo[]
      {
        new ColumnInfo(
          BaseMessages.getString( PKG, "HazelcastOutputDialog.FieldsTable.FieldName.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HazelcastOutputDialog.FieldsTable.Type.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMeta.getTypes(),
          true )
      };
    wlFields = new Label( shell, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.Output.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle / 4, -margin );
    fdlFields.top = new FormAttachment( lastControl, margin, SWT.BOTTOM );
    wlFields.setLayoutData( fdlFields );

    wFields =
      new TableView( transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, 5, lsMod,
        props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( middle / 4, 0 );
    fdFields.top = new FormAttachment( lastControl, margin * 2, SWT.BOTTOM );
    fdFields.right = new FormAttachment( 100, 0 );
    wFields.setLayoutData( fdFields );

    lastControl = wFields;

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "HazelcastOutputDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    setButtonPositions( new Button[]{ wOK, wCancel, wGet }, margin, null );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        getFields();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private String[] getStructureTypes() {
    return new String[]{ "Queue" }; // TODO Topic, etc.
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( !Const.isEmpty( input.getStructureName() ) ) {
      wStructureName.setText( input.getStructureName() );
    }
    if ( !Const.isEmpty( input.getStructureType() ) ) {
      wStructureType.setText( input.getStructureType() );
    }
    List<ValueMetaInterface> fields = input.getFields();
    if ( fields != null ) {
      int i = 0;
      wFields.table.setItemCount( fields.size() );
      for ( ValueMetaInterface field : fields ) {

        TableItem item = wFields.table.getItem( i );
        int col = 1;

        item.setText( col++, field.getName() );
        item.setText( col++, field.getTypeDesc() );
        i++;
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );
    wExpirationTime.setText( Integer.toString( input.getExpirationTime() ) );

    int i = 0;
    Set<InetSocketAddress> servers = input.getServers();
    if ( servers != null ) {
      for ( InetSocketAddress addr : input.getServers() ) {

        TableItem item = wServers.getNonEmpty( i++ );
        int col = 1;

        item.setText( col++, addr.getHostName() );
        item.setText( col++, Integer.toString( addr.getPort() ) );
      }
    }

    wServers.setRowNums();
    wServers.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    input.setStructureName( wStructureName.getText() );
    input.setStructureType( wStructureType.getText() );

    int nrFields = wFields.nrNonEmpty();

    List<ValueMetaInterface> fields = new ArrayList<ValueMetaInterface>( nrFields );
    for ( int i = 0; i < nrFields; i++ ) {
      try {
        TableItem item = wFields.getNonEmpty( i );

        ValueMetaInterface field =
          ValueMetaFactory.createValueMeta( item.getText( 1 ), ValueMetaFactory.getIdForValueMeta( item.getText( 2 ) ) );
        fields.add( field );
      } catch ( Exception e ) {
        // TODO
      }
    }
    input.setFields( fields );

    input.setExpirationTime( Integer.parseInt( wExpirationTime.getText() ) );

    int nrServers = wServers.nrNonEmpty();

    input.allocate( nrServers );

    Set<InetSocketAddress> servers = input.getServers();

    for ( int i = 0; i < nrServers; i++ ) {
      TableItem item = wServers.getNonEmpty( i );
      servers.add( new InetSocketAddress( item.getText( 1 ), Integer.parseInt( item.getText( 2 ) ) ) );
    }
    input.setServers( servers );

    dispose();
  }

  private void getFieldsInto( CCombo fieldCombo ) {
    try {
      if ( !gotPreviousFields ) {
        previousFields = transMeta.getPrevStepFields( stepname );
      }

      String field = fieldCombo.getText();

      if ( previousFields != null ) {
        fieldCombo.setItems( previousFields.getFieldNames() );
      }

      if ( field != null ) {
        fieldCombo.setText( field );
      }
      gotPreviousFields = true;

    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "HazelcastOutputDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "HazelcastOutputDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void getFields() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        BaseStepDialog.getFieldsFromPrevious( r, wFields, 1, new int[]{ 1 }, new int[]{ }, -1, -1, null );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "ZooKeeperOutputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "ZooKeeperOutputDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }
}

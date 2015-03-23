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
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.hazelcast.BaseHazelcastMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Created by mburgess on 3/23/15.
 */
public abstract class BaseHazelcastDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = BaseHazelcastMeta.class;  // for i18n purposes, needed by Translator2!!
  // $NON-NLS-1$

  protected Label wlServers;
  protected TableView wServers;
  protected FormData fdlServers, fdServers;

  public BaseHazelcastDialog( Shell parent, BaseStepMeta in, TransMeta tr, String sname ) {
    super( parent, in, tr, sname );
  }

  public void addServerUI( Shell shell, Composite previousComposite, PropsUI props, TransMeta transMeta,
                           ModifyListener lsMod, int numInitialServers, int options ) {
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    wlServers = new Label( shell, SWT.RIGHT );
    wlServers.setText( BaseMessages.getString( PKG, "BaseHazelcastDialog.Servers.Label" ) );
    props.setLook( wlServers );
    fdlServers = new FormData();
    fdlServers.left = new FormAttachment( 0, 0 );
    fdlServers.right = new FormAttachment( middle / 4, -margin );
    fdlServers.top = new FormAttachment( previousComposite, margin );
    wlServers.setLayoutData( fdlServers );

    ColumnInfo[] colinf =
      new ColumnInfo[]{
        new ColumnInfo( BaseMessages.getString( PKG, "BaseHazelcastDialog.HostName.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "BaseHazelcastDialog.Port.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    wServers =
      new TableView( transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, numInitialServers, lsMod,
        props );

    fdServers = new FormData();
    fdServers.left = new FormAttachment( middle / 4, 0 );
    fdServers.top = new FormAttachment( previousComposite, margin * 2 );
    fdServers.right = new FormAttachment( 100, 0 );
    wServers.setLayoutData( fdServers );
  }
}

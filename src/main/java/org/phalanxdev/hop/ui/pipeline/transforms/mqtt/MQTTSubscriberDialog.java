/*! ******************************************************************************
 *
 * MQTT for the Hop orchestration platform
 *
 * http://www.project-hop.org
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

package org.phalanxdev.hop.ui.pipeline.transforms.mqtt;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.phalanxdev.hop.pipeline.transforms.mqtt.MQTTPublisherMeta;
import org.phalanxdev.hop.pipeline.transforms.mqtt.MQTTSubscriberMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Dialog class for the MQTTSubscriber
 *
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 * @version $Revision: $
 */
public class MQTTSubscriberDialog extends BaseTransformDialog implements ITransformDialog {

  protected MQTTSubscriberMeta m_subscriberMeta;

  private CTabFolder m_wTabFolder;

  private CTabItem m_wGeneralTab;
  private TextVar m_wBroker;
  private TextVar m_wClientID;
  private TextVar m_wTimeout;
  private TextVar m_wkeepAlive;
  private TextVar m_wQOS;
  private TextVar m_wExecuteForDuration;

  private CTabItem m_wCredentialsTab;
  private Button m_wRequiresAuth;
  private Button m_wIsCleanSession;  
  private Label m_wlUsername;
  private TextVar m_wUsername;
  private Label m_wlPassword;
  private TextVar m_wPassword;
  //Path to persistence store
  private Label m_wlPath;
  private TextVar m_wPath;

  private CTabItem m_wSSLTab;
  private TextVar m_wCAFile;
  private TextVar m_wCertFile;
  private TextVar m_wKeyFile;
  private TextVar m_wKeyPassword;

  private CTabItem m_wTopicsTab;
  private TableView m_wTopicsTable;
  private CCombo m_wTopicMessageTypeCombo;
  private Button m_wAllowObjectMessages;

  public MQTTSubscriberDialog( Shell parent, IVariables variables, BaseTransformMeta baseTransformMeta,
                               PipelineMeta pipelineMeta, String transformname ) {
    super( parent, variables, baseTransformMeta, pipelineMeta, transformname );
    m_subscriberMeta = (MQTTSubscriberMeta) baseTransformMeta;
  }

  public MQTTSubscriberDialog( Shell parent, IVariables variables, Object baseStepMeta,
                               PipelineMeta pipelineMeta, String transformName ) {
    super( parent, variables, (BaseTransformMeta) baseStepMeta, pipelineMeta, transformName );
    m_subscriberMeta = (MQTTSubscriberMeta) baseStepMeta;
  }

  public MQTTSubscriberDialog( Shell parent, int nr, IVariables variables, Object in, PipelineMeta tr ) {
    super( parent, nr, variables, (BaseTransformMeta) in, tr );
    m_subscriberMeta = (MQTTSubscriberMeta) in;
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN
      | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, m_subscriberMeta );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_subscriberMeta.setChanged();
      }
    };
	SelectionAdapter lsSa=new SelectionAdapter() {
	  public void modifySelect( SelectionEvent e ) {
        m_subscriberMeta.setChanged();
      }
	};
    changed = m_subscriberMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "MQTTClientDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.StepName.Label" ) );
    props.setLook( wlTransformName );

    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

    // ====================
    // START OF TAB FOLDER
    // ====================
    m_wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( m_wTabFolder, Props.WIDGET_STYLE_TAB );

    // ====================
    // GENERAL TAB
    // ====================

    m_wGeneralTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wGeneralTab.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.GeneralTab.Label" ) ); //$NON-NLS-1$

    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = 3;
    mainLayout.marginHeight = 3;

    Composite wGeneralTabComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wGeneralTabComp );
    wGeneralTabComp.setLayout( mainLayout );

    // Broker URL
    Label wlBroker = new Label( wGeneralTabComp, SWT.RIGHT );
    wlBroker.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "MQTTClientDialog.Broker.Label" ) );
    props.setLook( wlBroker );
    FormData fdlBroker = new FormData();
    fdlBroker.top = new FormAttachment( 0, margin * 2 );
    fdlBroker.left = new FormAttachment( 0, 0 );
    fdlBroker.right = new FormAttachment( middle, -margin );
    wlBroker.setLayoutData( fdlBroker );
    m_wBroker = new TextVar( variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT
      | SWT.BORDER );
    props.setLook( m_wBroker );
    m_wBroker.addModifyListener( lsMod );
    FormData fdBroker = new FormData();
    fdBroker.top = new FormAttachment( 0, margin * 2 );
    fdBroker.left = new FormAttachment( middle, 0 );
    fdBroker.right = new FormAttachment( 100, 0 );
    m_wBroker.setLayoutData( fdBroker );
    lastControl = m_wBroker;

    // Connection timeout
    Label wlConnectionTimeout = new Label( wGeneralTabComp, SWT.RIGHT );
    wlConnectionTimeout.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.ConnectionTimeout.Label" ) );
    wlConnectionTimeout
      .setToolTipText( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.ConnectionTimeout.ToolTip" ) );
    props.setLook( wlConnectionTimeout );
    FormData fdlConnectionTimeout = new FormData();
    fdlConnectionTimeout.top = new FormAttachment( lastControl, margin );
    fdlConnectionTimeout.left = new FormAttachment( 0, 0 );
    fdlConnectionTimeout.right = new FormAttachment( middle, -margin );
    wlConnectionTimeout.setLayoutData( fdlConnectionTimeout );
    m_wTimeout = new TextVar( variables, wGeneralTabComp, SWT.SINGLE
      | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wTimeout );
    m_wTimeout.addModifyListener( lsMod );
    FormData fdConnectionTimeout = new FormData();
    fdConnectionTimeout.top = new FormAttachment( lastControl, margin );
    fdConnectionTimeout.left = new FormAttachment( middle, 0 );
    fdConnectionTimeout.right = new FormAttachment( 100, 0 );
    m_wTimeout.setLayoutData( fdConnectionTimeout );
    lastControl = m_wTimeout;

    // Keep alive interval
    Label wKeepAliveLab = new Label( wGeneralTabComp, SWT.RIGHT );
    wKeepAliveLab.setText( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.KeepAlive.Label" ) );
    wKeepAliveLab
      .setToolTipText( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.KeepAlive.ToolTip" ) );
    props.setLook( wKeepAliveLab );
    FormData fd = new FormData();
    fd.top = new FormAttachment( lastControl, margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    wKeepAliveLab.setLayoutData( fd );
    m_wkeepAlive = new TextVar( variables, wGeneralTabComp, SWT.SINGLE
      | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wkeepAlive );
    m_wkeepAlive.addModifyListener( lsMod );
    fd = new FormData();
    fd.top = new FormAttachment( lastControl, margin );
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_wkeepAlive.setLayoutData( fd );
    lastControl = m_wkeepAlive;

    // Client ID
    Label wlClientID = new Label( wGeneralTabComp, SWT.RIGHT );
    wlClientID.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.ClientID.Label" ) );
    props.setLook( wlClientID );
    FormData fdlClientID = new FormData();
    fdlClientID.top = new FormAttachment( lastControl, margin );
    fdlClientID.left = new FormAttachment( 0, 0 );
    fdlClientID.right = new FormAttachment( middle, -margin );
    wlClientID.setLayoutData( fdlClientID );
    m_wClientID = new TextVar( variables, wGeneralTabComp, SWT.SINGLE
      | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wClientID );
    m_wClientID.addModifyListener( lsMod );
    FormData fdClientID = new FormData();
    fdClientID.top = new FormAttachment( lastControl, margin );
    fdClientID.left = new FormAttachment( middle, 0 );
    fdClientID.right = new FormAttachment( 100, 0 );
    m_wClientID.setLayoutData( fdClientID );
    lastControl = m_wClientID;
	
   //Clean Session
	Label wlIsCleanSession = new Label( wGeneralTabComp, SWT.RIGHT );
    wlIsCleanSession.setText(
        BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.IsCleanSession.Label" ) );
    props.setLook( wlIsCleanSession);
    FormData fdClean = new FormData();
    fdClean.top = new FormAttachment( lastControl, margin );
    fdClean.left = new FormAttachment( 0, 0 );
    fdClean.right = new FormAttachment( middle, -margin );
    wlIsCleanSession.setLayoutData( fdClean );
    m_wIsCleanSession = new Button( wGeneralTabComp, SWT.CHECK );
    props.setLook( m_wIsCleanSession );
	m_wIsCleanSession.addSelectionListener(lsSa);
	fdClean = new FormData();
    fdClean.top = new FormAttachment( lastControl, margin );
    fdClean.left = new FormAttachment( middle, 0 );
    fdClean.right = new FormAttachment( 100, 0 );
    m_wIsCleanSession.setLayoutData( fdClean );
    lastControl = m_wIsCleanSession;

    // QOS
    Label wlQOS = new Label( wGeneralTabComp, SWT.RIGHT );
    wlQOS.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "MQTTClientDialog.QOS.Label" ) );
    props.setLook( wlQOS );
    FormData fdlQOS = new FormData();
    fdlQOS.top = new FormAttachment( lastControl, margin );
    fdlQOS.left = new FormAttachment( 0, 0 );
    fdlQOS.right = new FormAttachment( middle, -margin );
    wlQOS.setLayoutData( fdlQOS );
    m_wQOS = new TextVar( variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT
      | SWT.BORDER );
    props.setLook( m_wQOS );
    m_wQOS.addModifyListener( lsMod );
    FormData fdQOS = new FormData();
    fdQOS.top = new FormAttachment( lastControl, margin );
    fdQOS.left = new FormAttachment( middle, 0 );
    fdQOS.right = new FormAttachment( 100, 0 );
    m_wQOS.setLayoutData( fdQOS );
    lastControl = m_wQOS;
	
	//path to persistence store
	Label wlPath = new Label( wGeneralTabComp, SWT.RIGHT );
    wlPath.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.Path.Label" ) );
    props.setLook( wlPath );
    FormData fdlPath = new FormData();
    fdlPath.top = new FormAttachment( lastControl, margin );
    fdlPath.left = new FormAttachment( 0, 0 );
    fdlPath.right = new FormAttachment( middle, -margin );
    wlPath.setLayoutData( fdlPath );
    m_wPath = new TextVar( variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wPath );
    m_wPath.addModifyListener( lsMod );
    FormData fdPath = new FormData();
    fdPath.top = new FormAttachment( lastControl, margin );
    fdPath.left = new FormAttachment( middle, 0 );
    fdPath.right = new FormAttachment( 100, 0 );
    m_wPath.setLayoutData( fdPath );
    lastControl = m_wPath;

    // Execute for duration
    Label wExecuteForLab = new Label( wGeneralTabComp, SWT.RIGHT );
    wExecuteForLab.setText( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.ExecuteFor.Label" ) );
    wExecuteForLab
      .setToolTipText( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.ExecuteFor.ToolTip" ) );
    props.setLook( wExecuteForLab );
    fd = new FormData();
    fd.top = new FormAttachment( lastControl, margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    wExecuteForLab.setLayoutData( fd );

    m_wExecuteForDuration = new TextVar( variables, wGeneralTabComp, SWT.SINGLE
      | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wExecuteForDuration );
    fd = new FormData();
    fd.top = new FormAttachment( lastControl, margin );
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_wExecuteForDuration.setLayoutData( fd );
    lastControl = m_wExecuteForDuration;

    FormData fdGeneralTabComp = new FormData();
    fdGeneralTabComp.left = new FormAttachment( 0, 0 );
    fdGeneralTabComp.top = new FormAttachment( 0, 0 );
    fdGeneralTabComp.right = new FormAttachment( 100, 0 );
    fdGeneralTabComp.bottom = new FormAttachment( 100, 0 );
    wGeneralTabComp.setLayoutData( fdGeneralTabComp );

    wGeneralTabComp.layout();
    m_wGeneralTab.setControl( wGeneralTabComp );

    // ====================
    // CREDENTIALS TAB
    // ====================
    m_wCredentialsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wCredentialsTab.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.CredentialsTab.Title" ) ); //$NON-NLS-1$

    Composite wCredentialsComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wCredentialsComp );

    FormLayout fieldsCompLayout = new FormLayout();
    fieldsCompLayout.marginWidth = Const.FORM_MARGIN;
    fieldsCompLayout.marginHeight = Const.FORM_MARGIN;
    wCredentialsComp.setLayout( fieldsCompLayout );

    Label wlRequiresAuth = new Label( wCredentialsComp, SWT.RIGHT );
    wlRequiresAuth.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.RequireAuth.Label" ) );
    props.setLook( wlRequiresAuth );
    FormData fdlRequriesAuth = new FormData();
    fdlRequriesAuth.left = new FormAttachment( 0, 0 );
    fdlRequriesAuth.top = new FormAttachment( 0, margin * 2 );
    fdlRequriesAuth.right = new FormAttachment( middle, -margin );
    wlRequiresAuth.setLayoutData( fdlRequriesAuth );
    m_wRequiresAuth = new Button( wCredentialsComp, SWT.CHECK );
    props.setLook( m_wRequiresAuth );
    FormData fdRequiresAuth = new FormData();
    fdRequiresAuth.left = new FormAttachment( middle, 0 );
    fdRequiresAuth.top = new FormAttachment( 0, margin * 2 );
    fdRequiresAuth.right = new FormAttachment( 100, 0 );
    m_wRequiresAuth.setLayoutData( fdRequiresAuth );

    m_wRequiresAuth.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        boolean enabled = m_wRequiresAuth.getSelection();
        m_wlUsername.setEnabled( enabled );
        m_wUsername.setEnabled( enabled );
        m_wlPassword.setEnabled( enabled );
        m_wPassword.setEnabled( enabled );
      }
    } );
    lastControl = m_wRequiresAuth;

    // Username field
    m_wlUsername = new Label( wCredentialsComp, SWT.RIGHT );
    m_wlUsername.setEnabled( false );
    m_wlUsername.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.Username.Label" ) ); //$NON-NLS-1$
    props.setLook( m_wlUsername );
    FormData fdlUsername = new FormData();
    fdlUsername.left = new FormAttachment( 0, -margin );
    fdlUsername.right = new FormAttachment( middle, -2 * margin );
    fdlUsername.top = new FormAttachment( lastControl, 2 * margin );
    m_wlUsername.setLayoutData( fdlUsername );

    m_wUsername = new TextVar( variables, wCredentialsComp, SWT.SINGLE
      | SWT.LEFT | SWT.BORDER );
    m_wUsername.setEnabled( false );
    m_wUsername.setToolTipText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.Username.Tooltip" ) );
    props.setLook( m_wUsername );
    m_wUsername.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, -margin );
    fdResult.top = new FormAttachment( lastControl, 2 * margin );
    fdResult.right = new FormAttachment( 100, 0 );
    m_wUsername.setLayoutData( fdResult );
    lastControl = m_wUsername;

    // Password field
    m_wlPassword = new Label( wCredentialsComp, SWT.RIGHT );
    m_wlPassword.setEnabled( false );
    m_wlPassword.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.Password.Label" ) ); //$NON-NLS-1$
    props.setLook( m_wlPassword );
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, -margin );
    fdlPassword.right = new FormAttachment( middle, -2 * margin );
    fdlPassword.top = new FormAttachment( lastControl, margin );
    m_wlPassword.setLayoutData( fdlPassword );

    m_wPassword = new TextVar( variables, wCredentialsComp, SWT.SINGLE
      | SWT.LEFT | SWT.BORDER | SWT.PASSWORD );
    m_wPassword.setEnabled( false );
    m_wPassword.setToolTipText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.Password.Tooltip" ) );
    props.setLook( m_wPassword );
    m_wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment( middle, -margin );
    fdPassword.top = new FormAttachment( lastControl, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    m_wPassword.setLayoutData( fdPassword );

    FormData fdCredentialsComp = new FormData();
    fdCredentialsComp.left = new FormAttachment( 0, 0 );
    fdCredentialsComp.top = new FormAttachment( 0, 0 );
    fdCredentialsComp.right = new FormAttachment( 100, 0 );
    fdCredentialsComp.bottom = new FormAttachment( 100, 0 );
    wCredentialsComp.setLayoutData( fdCredentialsComp );

    wCredentialsComp.layout();
    m_wCredentialsTab.setControl( wCredentialsComp );

    // ====================
    // SSL TAB
    // ====================
    m_wSSLTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wSSLTab.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "MQTTClientDialog.SSLTab.Label" ) ); //$NON-NLS-1$

    Composite wSSLComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wSSLComp );

    FormLayout sslCompLayout = new FormLayout();
    sslCompLayout.marginWidth = Const.FORM_MARGIN;
    sslCompLayout.marginHeight = Const.FORM_MARGIN;
    wSSLComp.setLayout( sslCompLayout );

    // Server CA file path
    Label wlCAFile = new Label( wSSLComp, SWT.RIGHT );
    wlCAFile.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "MQTTClientDialog.CAFile.Label" ) ); //$NON-NLS-1$
    props.setLook( wlCAFile );
    FormData fdlCAFile = new FormData();
    fdlCAFile.left = new FormAttachment( 0, -margin );
    fdlCAFile.right = new FormAttachment( middle, -2 * margin );
    fdlCAFile.top = new FormAttachment( 0, 2 * margin );
    wlCAFile.setLayoutData( fdlCAFile );

    m_wCAFile = new TextVar( variables, wSSLComp, SWT.SINGLE | SWT.LEFT
      | SWT.BORDER );
    m_wCAFile.setToolTipText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.CAFile.Tooltip" ) );
    props.setLook( m_wCAFile );
    m_wCAFile.addModifyListener( lsMod );
    FormData fdCAFile = new FormData();
    fdCAFile.left = new FormAttachment( middle, -margin );
    fdCAFile.top = new FormAttachment( 0, 2 * margin );
    fdCAFile.right = new FormAttachment( 100, 0 );
    m_wCAFile.setLayoutData( fdCAFile );
    lastControl = m_wCAFile;

    // Client certificate file path
    Label wlCertFile = new Label( wSSLComp, SWT.RIGHT );
    wlCertFile.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.CertFile.Label" ) ); //$NON-NLS-1$
    props.setLook( wlCertFile );
    FormData fdlCertFile = new FormData();
    fdlCertFile.left = new FormAttachment( 0, -margin );
    fdlCertFile.right = new FormAttachment( middle, -2 * margin );
    fdlCertFile.top = new FormAttachment( lastControl, margin );
    wlCertFile.setLayoutData( fdlCertFile );

    m_wCertFile = new TextVar( variables, wSSLComp, SWT.SINGLE | SWT.LEFT
      | SWT.BORDER );
    m_wCertFile.setToolTipText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.CertFile.Tooltip" ) );
    props.setLook( m_wCertFile );
    m_wCertFile.addModifyListener( lsMod );
    FormData fdCertFile = new FormData();
    fdCertFile.left = new FormAttachment( middle, -margin );
    fdCertFile.top = new FormAttachment( lastControl, margin );
    fdCertFile.right = new FormAttachment( 100, 0 );
    m_wCertFile.setLayoutData( fdCertFile );
    lastControl = m_wCertFile;

    // Client key file path
    Label wlKeyFile = new Label( wSSLComp, SWT.RIGHT );
    wlKeyFile.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "MQTTClientDialog.KeyFile.Label" ) ); //$NON-NLS-1$
    props.setLook( wlKeyFile );
    FormData fdlKeyFile = new FormData();
    fdlKeyFile.left = new FormAttachment( 0, -margin );
    fdlKeyFile.right = new FormAttachment( middle, -2 * margin );
    fdlKeyFile.top = new FormAttachment( lastControl, margin );
    wlKeyFile.setLayoutData( fdlKeyFile );

    m_wKeyFile = new TextVar( variables, wSSLComp, SWT.SINGLE | SWT.LEFT
      | SWT.BORDER );
    m_wKeyFile.setToolTipText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.KeyFile.Tooltip" ) );
    props.setLook( m_wKeyFile );
    m_wKeyFile.addModifyListener( lsMod );
    FormData fdKeyFile = new FormData();
    fdKeyFile.left = new FormAttachment( middle, -margin );
    fdKeyFile.top = new FormAttachment( lastControl, margin );
    fdKeyFile.right = new FormAttachment( 100, 0 );
    m_wKeyFile.setLayoutData( fdKeyFile );
    lastControl = m_wKeyFile;

    // Client key file password path
    Label wlKeyPassword = new Label( wSSLComp, SWT.RIGHT );
    wlKeyPassword.setText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.KeyPassword.Label" ) ); //$NON-NLS-1$
    props.setLook( wlKeyPassword );
    FormData fdlKeyPassword = new FormData();
    fdlKeyPassword.left = new FormAttachment( 0, -margin );
    fdlKeyPassword.right = new FormAttachment( middle, -2 * margin );
    fdlKeyPassword.top = new FormAttachment( lastControl, margin );
    wlKeyPassword.setLayoutData( fdlKeyPassword );

    m_wKeyPassword = new TextVar( variables, wSSLComp, SWT.SINGLE | SWT.LEFT
      | SWT.BORDER | SWT.PASSWORD );
    m_wKeyPassword.setToolTipText( BaseMessages
      .getString( MQTTPublisherMeta.PKG,
        "MQTTClientDialog.KeyPassword.Tooltip" ) );
    props.setLook( m_wKeyPassword );
    m_wKeyPassword.addModifyListener( lsMod );
    FormData fdKeyPassword = new FormData();
    fdKeyPassword.left = new FormAttachment( middle, -margin );
    fdKeyPassword.top = new FormAttachment( lastControl, margin );
    fdKeyPassword.right = new FormAttachment( 100, 0 );
    m_wKeyPassword.setLayoutData( fdKeyPassword );
    lastControl = m_wKeyPassword;

    FormData fdSSLComp = new FormData();
    fdSSLComp.left = new FormAttachment( 0, 0 );
    fdSSLComp.top = new FormAttachment( 0, 0 );
    fdSSLComp.right = new FormAttachment( 100, 0 );
    fdSSLComp.bottom = new FormAttachment( 100, 0 );
    wSSLComp.setLayoutData( fdSSLComp );

    wSSLComp.layout();
    m_wSSLTab.setControl( wSSLComp );

    // ====================
    // Topics TAB
    // ====================
    m_wTopicsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wTopicsTab.setText( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.TopicsTab.Label" ) );
    Composite wTopicsComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wTopicsComp );
    FormLayout topicsLayout = new FormLayout();
    topicsLayout.marginWidth = Const.FORM_MARGIN;
    topicsLayout.marginHeight = Const.FORM_MARGIN;
    wTopicsComp.setLayout( topicsLayout );

    Label wTopicTypeLab = new Label( wTopicsComp, SWT.RIGHT );
    props.setLook( wTopicTypeLab );
    wTopicTypeLab.setText( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.TopicMessageType.Label" ) );
    wTopicTypeLab.setToolTipText(  BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientDialog.TopicMessageType.ToolTip" )  );
    fd = new FormData();
    fd.top = new FormAttachment( 0, margin * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    wTopicTypeLab.setLayoutData( fd );

    m_wTopicMessageTypeCombo = new CCombo( wTopicsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wTopicMessageTypeCombo );
    m_wTopicMessageTypeCombo.setItems( ValueMetaFactory.getAllValueMetaNames() );
    fd = new FormData();
    fd.top = new FormAttachment( 0, margin * 2 );
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_wTopicMessageTypeCombo.setLayoutData( fd );
    lastControl = m_wTopicMessageTypeCombo;


    Label wlAllowObjectMessages = new Label( wTopicsComp, SWT.RIGHT );
    wlAllowObjectMessages.setText( BaseMessages
        .getString( MQTTPublisherMeta.PKG,
            "MQTTClientDialog.AllowObjectMessages.Label" ) );
    props.setLook( wlAllowObjectMessages );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_wTopicMessageTypeCombo, margin * 2 );
    fd.right = new FormAttachment( middle, -margin );
    wlAllowObjectMessages.setLayoutData( fd );

    m_wAllowObjectMessages = new Button( wTopicsComp, SWT.CHECK );
    props.setLook( m_wAllowObjectMessages );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_wTopicMessageTypeCombo, margin * 2 );
    fd.right = new FormAttachment( 100, 0 );
    m_wAllowObjectMessages.setLayoutData( fd );
    lastControl = m_wAllowObjectMessages;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo( "Topic", ColumnInfo.COLUMN_TYPE_TEXT ),
      };

    // colinf[ 1 ].setComboValues( ValueMetaFactory.getAllValueMetaNames() );
    m_wTopicsTable =
      new TableView( variables, wTopicsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, 5, lsMod, props );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -200 );
    m_wTopicsTable.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wTopicsComp.setLayoutData( fd );
    wTopicsComp.layout();
    m_wTopicsTab.setControl( wTopicsComp );

    // ====================
    // BUTTONS
    // ====================
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "System.Button.OK" ) ); //$NON-NLS-1$
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( MQTTPublisherMeta.PKG,
      "System.Button.Cancel" ) ); //$NON-NLS-1$

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // ====================
    // END OF TAB FOLDER
    // ====================
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -margin );
    m_wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wTransformName.addSelectionListener( lsDef );
    // m_wInputField.addSelectionListener( lsDef );

    m_wTabFolder.setSelection( 0 );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize( shell, 440, 350, true );

    getData( m_subscriberMeta, true );
    m_subscriberMeta.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void ok() {
    if ( !Utils.isEmpty( wTransformName.getText() ) ) {
      setData( m_subscriberMeta );
      transformName = wTransformName.getText();
      dispose();
    }
  }

  private void cancel() {
    transformName = null;
    m_subscriberMeta.setChanged( changed );
    dispose();
  }

  private void setData( MQTTSubscriberMeta subscriberMeta ) {
    subscriberMeta.setBroker( m_wBroker.getText() );

    subscriberMeta.setClientId( m_wClientID.getText() );
    subscriberMeta.setTimeout( m_wTimeout.getText() );
    subscriberMeta.setKeepAliveInterval( m_wkeepAlive.getText() );
    subscriberMeta.setExecuteForDuration( m_wExecuteForDuration.getText() );
    subscriberMeta.setQoS( m_wQOS.getText() );
    subscriberMeta.setPath( m_wPath.getText() ); //adding path to persistence store
    boolean requiresAuth = m_wRequiresAuth.getSelection();
    subscriberMeta.setRequiresAuth( requiresAuth );
    if ( requiresAuth ) {
      subscriberMeta.setUsername( m_wUsername.getText() );
      subscriberMeta.setPassword( m_wPassword.getText() );
    }
	
	boolean isCleanSession=m_wIsCleanSession.getSelection(); //Adding cleanSession
	subscriberMeta.setCleanSession(isCleanSession);

    subscriberMeta.setAllowReadMessageOfTypeObject( m_wAllowObjectMessages.getSelection() );

    subscriberMeta.setSSLCaFile( m_wCAFile.getText() );
    subscriberMeta.setSSLCertFile( m_wCertFile.getText() );
    subscriberMeta.setSSLKeyFile( m_wKeyFile.getText() );
    subscriberMeta.setSSLKeyFilePass( m_wKeyPassword.getText() );
    subscriberMeta.setMessageType(
      Const.NVL( m_wTopicMessageTypeCombo.getText(),
        ValueMetaFactory.getValueMetaName( IValueMeta.TYPE_STRING ) ) );

    // fields
    int nrNonEmptyFields = m_wTopicsTable.nrNonEmpty();
    List<String> topics = new ArrayList<String>();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = m_wTopicsTable.getNonEmpty( i );
      topics.add( item.getText( 1 ).trim() );
    }
    subscriberMeta.setTopics( topics );

    subscriberMeta.setChanged();
  }

  private void getData( MQTTSubscriberMeta subscriberMeta, boolean copyStepname ) {
    if ( copyStepname ) {
      wTransformName.setText( transformName );
    }
    m_wBroker.setText( Const.NVL( subscriberMeta.getBroker(), "" ) );
    m_wClientID.setText( Const.NVL( subscriberMeta.getClientId(), "" ) );
    m_wTimeout.setText( Const.NVL( subscriberMeta.getTimeout(), "30" ) );
    m_wkeepAlive.setText( Const.NVL( subscriberMeta.getKeepAliveInterval(), "60" ) );
    m_wQOS.setText( Const.NVL( subscriberMeta.getQoS(), "0" ) );
    m_wExecuteForDuration.setText( Const.NVL( subscriberMeta.getExecuteForDuration(), "0" ) );
    
	m_wIsCleanSession.setSelection( subscriberMeta.isCleanSession() );//Adding cleanSession
    m_wPath.setText( Const.NVL( subscriberMeta.getPath(), "" ) ); //adding path to persistence store

    m_wRequiresAuth.setSelection( subscriberMeta.isRequiresAuth() );
    m_wRequiresAuth.notifyListeners( SWT.Selection, new Event() );

    m_wUsername.setText( Const.NVL( subscriberMeta.getUsername(), "" ) );
    m_wPassword.setText( Const.NVL( subscriberMeta.getPassword(), "" ) );

    m_wAllowObjectMessages.setSelection( subscriberMeta.getAllowReadMessageOfTypeObject() );

    m_wCAFile.setText( Const.NVL( subscriberMeta.getSSLCaFile(), "" ) );
    m_wCertFile.setText( Const.NVL( subscriberMeta.getSSLCertFile(), "" ) );
    m_wKeyFile.setText( Const.NVL( subscriberMeta.getSSLKeyFile(), "" ) );
    m_wKeyPassword.setText( Const.NVL( subscriberMeta.getSSLKeyFilePass(), "" ) );
    m_wTopicMessageTypeCombo.setText( Const
      .NVL( subscriberMeta.getMessageType(), ValueMetaFactory.getValueMetaName( IValueMeta.TYPE_STRING ) ) );

    List<String> topics = subscriberMeta.getTopics();
    if ( topics.size() > 0 ) {
      m_wTopicsTable.clearAll( false );
      Table table = m_wTopicsTable.getTable();
      for ( String t : topics ) {
        TableItem item = new TableItem( table, SWT.NONE );
        item.setText( 1, t.trim() );
      }

      m_wTopicsTable.removeEmptyRows();
      m_wTopicsTable.setRowNums();
      m_wTopicsTable.optWidth( true );
    }
  }
}

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

package org.phalanxdev.hop.pipeline.transforms.mqtt;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.phalanxdev.hop.ui.pipeline.transforms.mqtt.MQTTPublisherDialog;
import org.w3c.dom.Node;

import java.util.List;

/**
 * MQTT Client step definitions and serializer to/from XML and to/from Kettle
 * repository.
 *
 * @author Michael Spector
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 */
@Transform( id = "MQTTPublisherMeta", image = "MQTTPublisherIcon.svg", name = "MQTT Publisher", description =
    "Publish messages to a " + "MQTT broker", categoryDescription = "Output" ) public class MQTTPublisherMeta
    extends BaseTransformMeta implements ITransformMeta<MQTTPublisher, MQTTPublisherData> {

  public static Class<?> PKG = MQTTPublisherMeta.class;

  private String broker;
  private String topic;
  private String field;
  private String clientId;
  private String timeout = "30"; // seconds according to the docs
  private String qos = "0";
  private Boolean requiresAuth=false;
  private Boolean cleanSession=true; // Clean Session
  private String lastWillMessage; //Last Will message
  private String lastWillTopic; //Last Will Topic  
  private Boolean lastWillRetained=false; //Last Will retain Mode

  
  private Boolean retained=false; // Retained mode indicator
  private String username;
  private String password;
  private String sslCaFile;
  private String sslCertFile;
  private String sslKeyFile;
  private String sslKeyFilePass;

  private boolean m_topicIsFromField;

  /**
   * @return Broker URL
   */
  public String getBroker() {
    return broker;
  }

  /**
   * @param broker Broker URL
   */
  public void setBroker( String broker ) {
    this.broker = broker;
  }

  /**
   * @return MQTT topic name
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic MQTT topic name
   */
  public void setTopic( String topic ) {
    this.topic = topic;
  }

  /**
   * @param tif true if the topic is to be set from an incoming field value (topic in this case will hold the name of an
   *            incoming field instead of an absolute topic name)
   */
  public void setTopicIsFromField( boolean tif ) {
    m_topicIsFromField = tif;
  }

  /**
   * @return true if the topic is to be set from an incoming field value (topic in this case will hold the name of an
   * incoming field instead of an absolute topic name)
   */
  public boolean getTopicIsFromField() {
    return m_topicIsFromField;
  }

  /**
   * @return Target message field name in Kettle stream
   */
  public String getField() {
    return field;
  }

  /**
   * @param field Target field name in Kettle stream
   */
  public void setField( String field ) {
    this.field = field;
  }

  /**
   * @return Client ID
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * @param clientId Client ID
   */
  public void setClientId( String clientId ) {
    this.clientId = clientId;
  }

  /**
   * @return Connection timeout
   */
  public String getTimeout() {
    return timeout;
  }

  /**
   * @param timeout Connection timeout
   */
  public void setTimeout( String timeout ) {
    this.timeout = timeout;
  }

  /**
   * @return QoS to use
   */
  public String getQoS() {
    return qos;
  }

  /**
   * @param qos QoS to use
   */
  public void setQoS( String qos ) {
    this.qos = qos;
  }

  /**
   * @return Whether MQTT broker requires authentication
   */
  public boolean isRequiresAuth() {
    return requiresAuth;
  }

  /**
   * @param requiresAuth Whether MQTT broker requires authentication
   */
  public void setRequiresAuth( boolean requiresAuth ) {
    this.requiresAuth = requiresAuth;
  }

  /**
   * @return Username to MQTT broker
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username Username to MQTT broker
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * @return Password to MQTT broker
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password Password to MQTT broker
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Server CA file
   */
  public String getSSLCaFile() {
    return sslCaFile;
  }

  /**
   * @param sslCaFile Server CA file
   */
  public void setSSLCaFile( String sslCaFile ) {
    this.sslCaFile = sslCaFile;
  }

  /**
   * @return Client certificate file
   */
  public String getSSLCertFile() {
    return sslCertFile;
  }

  /**
   * @param sslCertFile Client certificate file
   */
  public void setSSLCertFile( String sslCertFile ) {
    this.sslCertFile = sslCertFile;
  }

  /**
   * @return Client key file
   */
  public String getSSLKeyFile() {
    return sslKeyFile;
  }

  /**
   * @param sslKeyFile Client key file
   */
  public void setSSLKeyFile( String sslKeyFile ) {
    this.sslKeyFile = sslKeyFile;
  }

  /**
   * @return Client key file password
   */
  public String getSSLKeyFilePass() {
    return sslKeyFilePass;
  }

  /**
   * @param sslKeyFilePass Client key file password
   */
  public void setSSLKeyFilePass( String sslKeyFilePass ) {
    this.sslKeyFilePass = sslKeyFilePass;
  }
  
  /**
   * @param cleanSession True/False
   */
  public void setCleanSession(Boolean cleanSession){
	this.cleanSession=cleanSession;
  }	
  
    /**
   * @return Cif clean session is true or false
   */
  public boolean isCleanSession(){
	return cleanSession;
  }	

  /**
   * @param lastWillMessage Last Will Message
   */
  public void setLastWillMessage( String lastWillMessage ) {
    this.lastWillMessage = lastWillMessage;
  } 
  
  /**
   * @return Client last will message
   */
  public String getLastWillMessage() {
	  return lastWillMessage;
  }
  /**
   * @param lastWillTopic Last Will Message
   */
  public void setLastWillTopic( String lastWillTopic ) {
    this.lastWillTopic = lastWillTopic;
  } 
  
  /**
   * @return Client last will message
   */
  public String getLastWillTopic() {
	  return lastWillTopic;
  }
  
  /**
   * @param lastWillRetained Last Will message mode True/False
   */
  public void setLastWillRetained(boolean lastWillRetained){
	this.lastWillRetained=lastWillRetained;
  }	

    /**
   * @return if message is in retain mode
   */
  public boolean isLastWillRetained(){
	return lastWillRetained;
  }	 
  
  /**
   * @param retained message mode True/False
   */
  public void setRetained(boolean retained){
	this.retained=retained;
  }	

  /**
   * @return true if message is in retain mode
   */
  public boolean isRetained(){
    return retained;
  }	

  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta transMeta, TransformMeta stepMeta, IRowMeta prev,
      String[] input, String[] output, IRowMeta info, IVariables space,
      IHopMetadataProvider metaStore ) {

    if ( broker == null ) {
      remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidBroker" ), stepMeta ) );
    }
    if ( topic == null ) {
      remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidTopic" ), stepMeta ) );
    }
    if ( field == null ) {
      remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidField" ), stepMeta ) );
    }
    if ( clientId == null ) {
      remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidClientID" ), stepMeta ) );
    }
    if ( timeout == null ) {
      remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidConnectionTimeout" ), stepMeta ) );
    }
    if ( qos == null ) {
      remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidQOS" ), stepMeta ) );
    }
    if ( requiresAuth ) {
      if ( username == null ) {
        remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidUsername" ), stepMeta ) );
      }
      if ( password == null ) {
        remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidPassword" ), stepMeta ) );
      }
    }
  }

  @Override
  public ITransform createTransform( TransformMeta transformMeta, MQTTPublisherData transformDataInterface, int cnr, PipelineMeta pipelineMeta,
      Pipeline trans ) {
    return new MQTTPublisher( transformMeta, this, transformDataInterface, cnr, pipelineMeta, trans );
  }

  @Override
  public MQTTPublisherData getTransformData() {
    return new MQTTPublisherData();
  }

  @Override public void loadXml( Node stepnode, IHopMetadataProvider metadataProvider )
      throws HopXmlException {

    try {
      broker = XmlHandler.getTagValue( stepnode, "BROKER" );
      topic = XmlHandler.getTagValue( stepnode, "TOPIC" );
      String topicFromField = XmlHandler.getTagValue( stepnode, "TOPIC_IS_FROM_FIELD" );
      if ( !Utils.isEmpty( topicFromField ) ) {
        m_topicIsFromField = topicFromField.equalsIgnoreCase( "Y" );
      }
      field = XmlHandler.getTagValue( stepnode, "FIELD" );
      clientId = XmlHandler.getTagValue( stepnode, "CLIENT_ID" );
      timeout = XmlHandler.getTagValue( stepnode, "TIMEOUT" );
      qos = XmlHandler.getTagValue( stepnode, "QOS" );
      requiresAuth = Boolean.parseBoolean( XmlHandler.getTagValue( stepnode, "REQUIRES_AUTH" ) );
      username = XmlHandler.getTagValue( stepnode, "USERNAME" );
      password = XmlHandler.getTagValue( stepnode, "PASSWORD" );
	  lastWillTopic= XmlHandler.getTagValue( stepnode, "LASTWILLTOPIC" );
	  lastWillMessage= XmlHandler.getTagValue( stepnode, "LASTWILLMESSAGE" );
	  lastWillRetained= Boolean.parseBoolean( XmlHandler.getTagValue( stepnode,"LASTWILLRETAINED" ));
	  retained= Boolean.parseBoolean( XmlHandler.getTagValue( stepnode,"RETAINED" ));
	  cleanSession=Boolean.parseBoolean( XmlHandler.getTagValue( stepnode,"CLEANSESSION"));

      if ( !Utils.isEmpty( password ) ) {
        password = Encr.decryptPasswordOptionallyEncrypted( password );
      }

      Node sslNode = XmlHandler.getSubNode( stepnode, "SSL" );
      if ( sslNode != null ) {
        sslCaFile = XmlHandler.getTagValue( sslNode, "CA_FILE" );
        sslCertFile = XmlHandler.getTagValue( sslNode, "CERT_FILE" );
        sslKeyFile = XmlHandler.getTagValue( sslNode, "KEY_FILE" );
        sslKeyFilePass = XmlHandler.getTagValue( sslNode, "KEY_FILE_PASS" );
      }
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "MQTTClientMeta.Exception.loadXml" ), e );
    }
  }
  
  @Override
  public String getXml() throws HopException {
    StringBuilder retval = new StringBuilder();
    if ( broker != null ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "BROKER", broker ) );
    }
    if ( topic != null ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "TOPIC", topic ) );
    }

    retval.append( "    " ).
        append( XmlHandler.addTagValue( "TOPIC_IS_FROM_FIELD", m_topicIsFromField ) );

    if ( field != null ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "FIELD", field ) );
    }
    if ( clientId != null ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "CLIENT_ID", clientId ) );
    }
    if ( timeout != null ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "TIMEOUT", timeout ) );
    }
    if ( qos != null ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "QOS", qos ) );
    }

    retval.append( "    " ).append( XmlHandler.addTagValue( "REQUIRES_AUTH", Boolean.toString( requiresAuth ) ) );

    if ( username != null ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "USERNAME", username ) );
    }
    if ( password != null ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "PASSWORD", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    }
	 if ( cleanSession != null ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "CLEANSESSION", Boolean.toString(cleanSession ) )  );
    }
    if ( retained != null ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "RETAINED", Boolean.toString(retained ) ) );
    }
	if ( lastWillTopic != null ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "LASTWILLTOPIC",  lastWillTopic )  );
    }
	if ( lastWillMessage != null ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "LASTWILLMESSAGE", lastWillMessage) );
    }
	if ( lastWillRetained!= null ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "LASTWILLRETAINED", Boolean.toString(lastWillRetained ) ) );
    }
    if ( sslCaFile != null || sslCertFile != null || sslKeyFile != null || sslKeyFilePass != null ) {
      retval.append( "    " ).append( XmlHandler.openTag( "SSL" ) ).append( Const.CR );
      if ( sslCaFile != null ) {
        retval.append( "      " + XmlHandler.addTagValue( "CA_FILE", sslCaFile ) );
      }
      if ( sslCertFile != null ) {
        retval.append( "      " + XmlHandler.addTagValue( "CERT_FILE", sslCertFile ) );
      }
      if ( sslKeyFile != null ) {
        retval.append( "      " + XmlHandler.addTagValue( "KEY_FILE", sslKeyFile ) );
      }
      if ( sslKeyFilePass != null ) {
        retval.append( "      " + XmlHandler.addTagValue( "KEY_FILE_PASS", sslKeyFilePass ) );
      }
      retval.append( "    " ).append( XmlHandler.closeTag( "SSL" ) ).append( Const.CR );
    }

    return retval.toString();
  }

  @Override public void setDefault() {
  }

  @Override public String getDialogClassName() {
    return MQTTPublisherDialog.class.getCanonicalName();
  }
}

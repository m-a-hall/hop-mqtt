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

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.phalanxdev.hop.ui.pipeline.transforms.mqtt.MQTTSubscriberDialog;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta class for the MQTTSubscriber step
 *
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 * @version $Revision: $
 */
@Transform( id = "MQTTSubscriberMeta", image = "MQTTSubscriberIcon.svg", name = "MQTT Subscriber", description =
    "Subscribe to topics " + "at an MQTT broker", categoryDescription = "Input" ) public class MQTTSubscriberMeta
    extends BaseTransformMeta implements ITransformMeta<MQTTSubscriber, MQTTSubscriberData> {

  protected String m_broker = "";

  protected List<String> m_topics = new ArrayList<>();

  protected String m_messageType = ValueMetaFactory.getValueMetaName( IValueMeta.TYPE_STRING );

  private String m_clientId;
  private String m_timeout = "30"; // seconds according to the mqtt javadocs
  private String m_keepAliveInterval = "60"; // seconds according to the mqtt javadocs
  private String m_qos = "0";
  private boolean m_requiresAuth;
  private String m_username;
  private String m_password;
  private String m_sslCaFile;
  private String m_sslCertFile;
  private String m_sslKeyFile;
  private String m_sslKeyFilePass;

  private Boolean m_cleanSession = true; // Clean Session
  private String m_path; //Adding possiblity to persiste datastore (Clean Session = false) on file

  /**
   * Whether to allow messages of type object to be deserialized off the wire
   */
  private boolean m_allowReadObjectMessageType;

  /**
   * Execute for x seconds (0 means indefinitely)
   */
  private String m_executeForDuration = "0";

  /**
   * @return Broker URL
   */
  public String getBroker() {
    return m_broker;
  }

  /**
   * @param broker Broker URL
   */
  public void setBroker( String broker ) {
    m_broker = broker;
  }

  public void setTopics( List<String> topics ) {
    m_topics = topics;
  }

  public List<String> getTopics() {
    return m_topics;
  }

  /**
   * @param type the Kettle type of the message being received
   */
  public void setMessageType( String type ) {
    m_messageType = type;
  }

  /**
   * @return the Kettle type of the message being received
   */
  public String getMessageType() {
    return m_messageType;
  }

  /**
   * @return Client ID
   */
  public String getClientId() {
    return m_clientId;
  }

  /**
   * @param clientId Client ID
   */
  public void setClientId( String clientId ) {
    m_clientId = clientId;
  }

  /**
   * @return Connection m_timeout
   */
  public String getTimeout() {
    return m_timeout;
  }

  /**
   * @param timeout Connection m_timeout
   */
  public void setTimeout( String timeout ) {
    m_timeout = timeout;
  }

  /**
   * @param interval interval in seconds
   */
  public void setKeepAliveInterval( String interval ) {
    m_keepAliveInterval = interval;
  }

  /**
   * @return the keep alive interval (in seconds)
   */
  public String getKeepAliveInterval() {
    return m_keepAliveInterval;
  }

  /**
   * @return QoS to use
   */
  public String getQoS() {
    return m_qos;
  }

  /**
   * @param qos QoS to use
   */
  public void setQoS( String qos ) {
    m_qos = qos;
  }

  /**
   * @return Whether MQTT broker requires authentication
   */
  public boolean isRequiresAuth() {
    return m_requiresAuth;
  }

  /**
   * @param requiresAuth Whether MQTT broker requires authentication
   */
  public void setRequiresAuth( boolean requiresAuth ) {
    m_requiresAuth = requiresAuth;
  }

  /**
   * @return Username to MQTT broker
   */
  public String getUsername() {
    return m_username;
  }

  /**
   * @param username Username to MQTT broker
   */
  public void setUsername( String username ) {
    m_username = username;
  }

  /**
   * @return Password to MQTT broker
   */
  public String getPassword() {
    return m_password;
  }

  /**
   * @param password Password to MQTT broker
   */
  public void setPassword( String password ) {
    m_password = password;
  }

  /**
   * @return Server CA file
   */
  public String getSSLCaFile() {
    return m_sslCaFile;
  }

  /**
   * @param sslCaFile Server CA file
   */
  public void setSSLCaFile( String sslCaFile ) {
    m_sslCaFile = sslCaFile;
  }

  /**
   * @return Client certificate file
   */
  public String getSSLCertFile() {
    return m_sslCertFile;
  }

  /**
   * @param sslCertFile Client certificate file
   */
  public void setSSLCertFile( String sslCertFile ) {
    m_sslCertFile = sslCertFile;
  }

  /**
   * @return Client key file
   */
  public String getSSLKeyFile() {
    return m_sslKeyFile;
  }

  /**
   * @param sslKeyFile Client key file
   */
  public void setSSLKeyFile( String sslKeyFile ) {
    m_sslKeyFile = sslKeyFile;
  }

  /**
   * @return Client key file m_password
   */
  public String getSSLKeyFilePass() {
    return m_sslKeyFilePass;
  }

  /**
   * @param sslKeyFilePass Client key file m_password
   */
  public void setSSLKeyFilePass( String sslKeyFilePass ) {
    m_sslKeyFilePass = sslKeyFilePass;
  }

  /**
   * @param duration the duration (in seconds) to run for. 0 indicates run indefinitely
   */
  public void setExecuteForDuration( String duration ) {
    m_executeForDuration = duration;
  }

  /**
   * @return the number of seconds to run for (0 means run indefinitely)
   */
  public String getExecuteForDuration() {
    return m_executeForDuration;
  }

  /**
   * @param allow true to allow object messages to be deserialized off of the wire
   */
  public void setAllowReadMessageOfTypeObject( boolean allow ) {
    m_allowReadObjectMessageType = allow;
  }

  /**
   * @return true if deserializing object messages is ok
   */
  public boolean getAllowReadMessageOfTypeObject() {
    return m_allowReadObjectMessageType;
  }

  /**
   * @param cleanSession True/False
   */
  public void setCleanSession( Boolean cleanSession ) {
    m_cleanSession = cleanSession;
  }

  /**
   * @return Cif clean session is true or false
   */
  public boolean isCleanSession() {
    return m_cleanSession;
  }

  /**
   * @return path to persistence store
   */
  public String getPath() {
    return m_path;
  }

  /**
   * @param path path to persistence store
   */
  public void setPath( String path ) {
    m_path = path;
  }

  @Override public void setDefault() {

  }

  @Override
  public ITransform createTransform( TransformMeta transformMeta, MQTTSubscriberData mqttSubscriberData, int i,
      PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new MQTTSubscriber( transformMeta, this, mqttSubscriberData, i, pipelineMeta, pipeline );
  }

  @Override public MQTTSubscriberData getTransformData() {
    return new MQTTSubscriberData();
  }

  @Override public void loadXml( Node stepnode, IHopMetadataProvider metadataProvider )
      throws HopXmlException {
    m_broker = XmlHandler.getTagValue( stepnode, "BROKER" );
    String topics = XmlHandler.getTagValue( stepnode, "TOPICS" );
    m_topics = new ArrayList<>();
    if ( !org.apache.hop.core.util.Utils.isEmpty( topics ) ) {
      String[] parts = topics.split( "," );
      for ( String p : parts ) {
        m_topics.add( p.trim() );
      }
    }

    m_messageType = XmlHandler.getTagValue( stepnode, "MESSAGE_TYPE" );
    if ( org.apache.hop.core.util.Utils.isEmpty( m_messageType ) ) {
      m_messageType = ValueMetaFactory.getValueMetaName( IValueMeta.TYPE_STRING );
    }
    m_clientId = XmlHandler.getTagValue( stepnode, "CLIENT_ID" );
    m_timeout = XmlHandler.getTagValue( stepnode, "TIMEOUT" );
    m_keepAliveInterval = XmlHandler.getTagValue( stepnode, "KEEP_ALIVE" );
    m_executeForDuration = XmlHandler.getTagValue( stepnode, "EXECUTE_FOR_DURATION" );
    m_qos = XmlHandler.getTagValue( stepnode, "QOS" );
    m_requiresAuth = Boolean.parseBoolean( XmlHandler.getTagValue( stepnode, "REQUIRES_AUTH" ) );
    m_cleanSession = Boolean.parseBoolean( XmlHandler.getTagValue( stepnode, "CLEANSESSION" ) );

    m_username = XmlHandler.getTagValue( stepnode, "USERNAME" );
    m_password = XmlHandler.getTagValue( stepnode, "PASSWORD" );
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_password ) ) {
      m_password = Encr.decryptPasswordOptionallyEncrypted( m_password );
    }

    String allowObjects = XmlHandler.getTagValue( stepnode, "READ_OBJECTS" );
    if ( !org.apache.hop.core.util.Utils.isEmpty( allowObjects ) ) {
      m_allowReadObjectMessageType = Boolean.parseBoolean( allowObjects );
    }

    Node sslNode = XmlHandler.getSubNode( stepnode, "SSL" );
    if ( sslNode != null ) {
      m_sslCaFile = XmlHandler.getTagValue( sslNode, "CA_FILE" );
      m_sslCertFile = XmlHandler.getTagValue( sslNode, "CERT_FILE" );
      m_sslKeyFile = XmlHandler.getTagValue( sslNode, "KEY_FILE" );
      m_sslKeyFilePass = XmlHandler.getTagValue( sslNode, "KEY_FILE_PASS" );
    }
  }

  @Override public String getXml() throws HopException {
    StringBuilder retval = new StringBuilder();
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_broker ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "BROKER", m_broker ) );
    }

    if ( !org.apache.hop.core.util.Utils.isEmpty( m_topics ) ) {
      String topicString = "";
      for ( String t : m_topics ) {
        topicString += "," + t;
      }
      retval.append( "    " ).append( XmlHandler.addTagValue( "TOPICS", topicString.substring( 1 ) ) );
    }

    if ( !org.apache.hop.core.util.Utils.isEmpty( m_messageType ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "MESSAGE_TYPE", m_messageType ) );
    }

    if ( !org.apache.hop.core.util.Utils.isEmpty( m_clientId ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "CLIENT_ID", m_clientId ) );
    }
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_timeout ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "TIMEOUT", m_timeout ) );
    }
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_qos ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "QOS", m_qos ) );
    }
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_keepAliveInterval ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "KEEP_ALIVE", m_keepAliveInterval ) );
    }
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_executeForDuration ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "EXECUTE_FOR_DURATION", m_executeForDuration ) );
    }
    retval.append( "    " ).append( XmlHandler.addTagValue( "REQUIRES_AUTH", Boolean.toString( m_requiresAuth ) ) );
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_username ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "USERNAME", m_username ) );
    }
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_password ) ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "PASSWORD", Encr.encryptPasswordIfNotUsingVariables( m_password ) ) );
    }
    retval.append( "    " ).append( XmlHandler.addTagValue( "CLEANSESSION", Boolean.toString( m_cleanSession ) ) );

    if ( !org.apache.hop.core.util.Utils.isEmpty( m_path ) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "PATH", m_path ) );
    }
    retval.append( "    " )
        .append( XmlHandler.addTagValue( "READ_OBJECTS", Boolean.toString( m_allowReadObjectMessageType ) ) );

    if ( !org.apache.hop.core.util.Utils.isEmpty( m_sslCaFile ) || !org.apache.hop.core.util.Utils.isEmpty( m_sslCertFile )
        || !org.apache.hop.core.util.Utils.isEmpty( m_sslKeyFile ) || !org.apache.hop.core.util.Utils.isEmpty( m_sslKeyFilePass ) ) {
      retval.append( "    " ).append( XmlHandler.openTag( "SSL" ) ).append( Const.CR );
      if ( !org.apache.hop.core.util.Utils.isEmpty( m_sslCaFile ) ) {
        retval.append( "      " + XmlHandler.addTagValue( "CA_FILE", m_sslCaFile ) );
      }
      if ( !org.apache.hop.core.util.Utils.isEmpty( m_sslCertFile ) ) {
        retval.append( "      " + XmlHandler.addTagValue( "CERT_FILE", m_sslCertFile ) );
      }
      if ( !org.apache.hop.core.util.Utils.isEmpty( m_sslKeyFile ) ) {
        retval.append( "      " + XmlHandler.addTagValue( "KEY_FILE", m_sslKeyFile ) );
      }
      if ( !org.apache.hop.core.util.Utils.isEmpty( m_sslKeyFilePass ) ) {
        retval.append( "      " + XmlHandler.addTagValue( "KEY_FILE_PASS", m_sslKeyFilePass ) );
      }
      retval.append( "    " ).append( XmlHandler.closeTag( "SSL" ) ).append( Const.CR );
    }

    return retval.toString();
  }
  
  @Override
  public void getFields( IRowMeta rowMeta, String transformName, IRowMeta[] info, TransformMeta transformMeta,
      IVariables space, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    rowMeta.clear();
    try {
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Topic", IValueMeta.TYPE_STRING ) );
      rowMeta.addValueMeta(
          ValueMetaFactory.createValueMeta( "Message", ValueMetaFactory.getIdForValueMeta( getMessageType() ) ) );
    } catch ( HopPluginException e ) {
      throw new HopTransformException( e );
    }
  }

  @Override public String getDialogClassName() {
    return MQTTSubscriberDialog.class.getCanonicalName();
  }
}

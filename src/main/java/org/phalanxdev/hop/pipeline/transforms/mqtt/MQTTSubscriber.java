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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
//Adding Persistence store (for CleanSession=false)
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.phalanxdev.mqtt.SSLSocketFactoryGenerator;
//-----

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * MQTT subscriber step
 *
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 * @version $Revision: $
 */
public class MQTTSubscriber extends BaseTransform<MQTTSubscriberMeta, MQTTSubscriberData> implements ITransform<MQTTSubscriberMeta, MQTTSubscriberData> {

  protected boolean m_reconnectFailed;
  protected boolean m_isInit=false; // Added this for persistence. We need to be sure init is done before processing unconsumed messages at broker reconnection

  protected MQTTSubscriberMeta m_meta;
  protected MQTTSubscriberData m_data;

  public MQTTSubscriber( TransformMeta transformMeta, MQTTSubscriberMeta meta,
      MQTTSubscriberData data, int copyNr, PipelineMeta pipelineMeta,
      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    m_meta = meta;
    m_data = data;
  }

  public boolean processRow( ) throws HopException {

    if ( !isStopped() ) {

      if ( first ) {
        first = false;
        logBasic("process row first");
        if ( m_data.m_executionDuration > 0 ) {
          m_data.m_startTime = new Date();
        }

        m_data.m_outputRowMeta = new RowMeta();

        m_meta.getFields( m_data.m_outputRowMeta, getTransformName(), null, null,
            getPipelineMeta(), getMetadataProvider() );
      }

      if ( m_reconnectFailed ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.ReconnectFailed" ) );
        setStopped( true );
        return false;
      }

      if ( ( (MQTTSubscriberData) m_data ).m_executionDuration > 0 ) {
        if ( System.currentTimeMillis() - ( (MQTTSubscriberData) m_data ).m_startTime.getTime()
            > ( (MQTTSubscriberData) m_data ).m_executionDuration * 1000 ) {
          setOutputDone();
          return false;
        }
      }

      return true;
    } else {
      setStopped( true );
      return false;
    }
  }

  protected synchronized void shutdown( MQTTSubscriberData data ) {
    if ( data.m_client != null ) {
      try {
        if ( data.m_client.isConnected() ) {
          logBasic( "Disconnecting from MQTT broker" );
          data.m_client.disconnect();
        }
		data.m_client.close();
        data.m_client = null;
      } catch ( MqttException e ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorClosingMQTTClient.Message" ), e );
      }
    }
  }

  public boolean init( ) {
    if ( super.init( ) ) {
      try {
        configureConnection( m_meta, m_data );
        String runFor = m_meta.getExecuteForDuration();
        try {

		  ( (MQTTSubscriberData) m_data ).m_executionDuration = Long.parseLong( runFor );
        } catch ( NumberFormatException e ) {
          logError( e.getMessage(), e );
          return false;
        }
      } catch ( HopException e ) {
        logError( e.getMessage(), e );
        return false;
      }

      try {
        IValueMeta
            messageMeta =
            ValueMetaFactory.createValueMeta( "Message",
                ValueMetaFactory.getIdForValueMeta( m_meta.getMessageType() ) );
        if ( messageMeta.isSerializableType() && !m_meta.getAllowReadMessageOfTypeObject() ) {
          logError( BaseMessages
              .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.MessageTypeObjectButObjectNotAllowed" ) );
          return false;
        }
      } catch ( HopPluginException e ) {
        logError( e.getMessage(), e );
        return false;
      }
      m_isInit=true;
      return true;
    }

    return false;
  }

  public void dispose( ) {
    MQTTSubscriberData data = (MQTTSubscriberData) m_data;
    shutdown( data );
    super.dispose( );
  }

  public void stopRunning( ) throws HopException {
    shutdown( m_data );
    super.stopRunning( );
  }
  
  //Adding creation of persistence store 
  public MqttClientPersistence getPersistence(String path) {
        if (path==null || path.length()==0) {
            logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Message.MemoryPersitence" ) );
            return new MemoryPersistence();
        } else {
			logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG,"MQTTClientStep.Message.PathPersitence",path));
            return new MqttDefaultFilePersistence(path);
        }
    }

  protected void configureConnection( MQTTSubscriberMeta meta, MQTTSubscriberData data ) throws HopException {
    if ( data.m_client == null ) {
	  String broker = environmentSubstitute( meta.getBroker() );
      if ( org.apache.hop.core.util.Utils.isEmpty( broker ) ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.NoBrokerURL" ) );
      }
      String clientId = environmentSubstitute( meta.getClientId() );
      if ( org.apache.hop.core.util.Utils.isEmpty( clientId ) ) {
        throw new HopException( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.NoClientID" ) );
      }
      List<String> topics = meta.getTopics();
      if ( topics == null || topics.size() == 0 ) {
        throw new HopException( "No topic(s) to subscribe to provided" );
      }
      List<String> resolvedTopics = new ArrayList<>();
      for ( String topic : topics ) {
        resolvedTopics.add( environmentSubstitute( topic ) );
      }

      String qosS = environmentSubstitute( meta.getQoS() );
      int qos = 0;
      if ( !org.apache.hop.core.util.Utils.isEmpty( qosS ) ) {
        try {
          qos = Integer.parseInt( qosS );
        } catch ( NumberFormatException e ) {
          // quietly ignore
        }
      }
      int[] qoss = new int[resolvedTopics.size()];
      for ( int i = 0; i < qoss.length; i++ ) {
        qoss[i] = qos;
      }
      try {
		Boolean isCleanSession=meta.isCleanSession();
		//Handling Persistence Store if cleanSession=false
		MqttClientPersistence persistenceStore=null;
		// Handling persistenceStore in memory or on disk
		if(isCleanSession==false){
				persistenceStore=getPersistence(meta.getPath());		
		}
		//
        if(!isCleanSession){
			data.m_client = new MqttClient( broker, clientId, persistenceStore );
		}
		else data.m_client = new MqttClient( broker, clientId);
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        if ( meta.isRequiresAuth() ) {
          connectOptions.setUserName( environmentSubstitute( meta.getUsername() ) );
          connectOptions.setPassword( environmentSubstitute( meta.getPassword() ).toCharArray() );
        }
        if ( broker.startsWith( "ssl:" ) || broker.startsWith( "wss:" ) ) {
          connectOptions.setSocketFactory( SSLSocketFactoryGenerator
              .getSocketFactory( environmentSubstitute( meta.getSSLCaFile() ),
                  environmentSubstitute( meta.getSSLCertFile() ), environmentSubstitute( meta.getSSLKeyFile() ),
                  environmentSubstitute( meta.getSSLKeyFilePass() ) ) );
        }
		
		
		connectOptions.setCleanSession( isCleanSession); //Adding Clean Session option true=Clean session, false=persistence management
        
		logBasic( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.CreateMQTTClient.MessageEarly", broker, clientId,Boolean.toString(isCleanSession) ) );

        String timeout = environmentSubstitute( meta.getTimeout() );
        String keepAlive = environmentSubstitute( meta.getKeepAliveInterval() );
        try {
          connectOptions.setConnectionTimeout( Integer.parseInt( timeout ) );
        } catch ( NumberFormatException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongTimeoutValue.Message", timeout ), e );
        }

        try {
          connectOptions.setKeepAliveInterval( Integer.parseInt( keepAlive ) );
        } catch ( NumberFormatException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongKeepAliveValue.Message", keepAlive ),
              e );
        }

        logBasic( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.CreateMQTTClient.Message", broker, clientId,Boolean.toString(meta.isCleanSession()) ) );
        data.m_client.setCallback( new SubscriberCallback( data, meta ) );
        data.m_client.connect( connectOptions );
        if(data.m_client.isConnected()){ //adding this in case cleansession=flse otherwise exception
			data.m_client.subscribe( resolvedTopics.toArray( new String[resolvedTopics.size()] ), qoss );
					logBasic(BaseMessages.getString( MQTTPublisherMeta.PKG,"MQTTClientStep.DebugMessage",4));
		}

      } catch ( Exception e ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorCreateMQTTClient.Message", broker ),
            e );
      }
    } else {logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG,"MQTTClientStep.DebugMessage","data cleint not null"));}
  }

  protected class SubscriberCallback implements MqttCallback {

    protected MQTTSubscriberData m_data;
    protected MQTTSubscriberMeta m_meta;
    protected IValueMeta m_messageValueMeta;

    public SubscriberCallback( MQTTSubscriberData data, MQTTSubscriberMeta meta ) throws HopPluginException {
      m_data = data;
      m_meta = meta;
      logBasic(BaseMessages.getString( MQTTPublisherMeta.PKG,"MQTTClientStep.DebugMessage","message callback"));
      m_messageValueMeta =
          ValueMetaFactory.createValueMeta( "Message", ValueMetaFactory.getIdForValueMeta( m_meta.getMessageType() ) );
    }

    @Override public void connectionLost( Throwable throwable ) {
      // connection retry logic here
      shutdown( m_data );
      logBasic( BaseMessages
          .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.LostConnectionToBroker", throwable.getMessage() ) );
      logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.AttemptingToReconnect" ) );
      try {
		if(!m_reconnectFailed){
		   configureConnection( m_meta, m_data ); //to debug, only one reconnection try do not reconnect a thousand times
		}
      } catch ( HopException e ) {
        logError( e.getMessage(), e );
        m_reconnectFailed = true;
      }
    }

    @Override public void messageArrived( String topic, MqttMessage mqttMessage ) throws Exception {
      
	  
	  if(m_data.m_outputRowMeta==null) // if clean session=false, we might reicieve messages before processrows has been called if the broker sends back all unconsumed mesages
			{
				while(!m_isInit)
				{
				//Wait until assync init done
				}			
			}
		
		  

	  Object[] outRow = RowDataUtil.allocateRowData( m_data.m_outputRowMeta.size() );
      outRow[0] = topic;
      Object converted = null;

      byte[] raw = mqttMessage.getPayload();
      ByteBuffer buff = null;
      switch ( m_messageValueMeta.getType() ) {
        case IValueMeta.TYPE_INTEGER:
          buff = ByteBuffer.wrap( raw );
          outRow[1] = raw.length == 4 ? (long) buff.getInt() : buff.getLong();
          break;
        case IValueMeta.TYPE_STRING:
        case IValueMeta.TYPE_NONE:
          outRow[1] = new String( raw );
          break;
        case IValueMeta.TYPE_NUMBER:
          buff = ByteBuffer.wrap( raw );
          outRow[1] = raw.length == 4 ? (double) buff.getFloat() : buff.getDouble();
          break;
        case IValueMeta.TYPE_DATE:
          buff = ByteBuffer.wrap( raw );
          outRow[1] = new Date( buff.getLong() );
          break;
        case IValueMeta.TYPE_BINARY:
          outRow[1] = raw;
          break;
        case IValueMeta.TYPE_BOOLEAN:
          outRow[1] = raw[0] > 0;
          break;
        case IValueMeta.TYPE_TIMESTAMP:
          buff = ByteBuffer.wrap( raw );
          long time = buff.getLong();
          int nanos = buff.getInt();
          Timestamp t = new Timestamp( time );
          t.setNanos( nanos );
          outRow[1] = t;
          break;
        case IValueMeta.TYPE_SERIALIZABLE:
          ObjectInputStream ois = new ObjectInputStream( new ByteArrayInputStream( raw ) );
          outRow[1] = ois.readObject();
          break;
        default:
          throw new HopException( "Unhandled type" );
      }
      putRow( m_data.m_outputRowMeta, outRow );
	  
    }

    @Override public void deliveryComplete( IMqttDeliveryToken iMqttDeliveryToken ) {

    }
  }
}

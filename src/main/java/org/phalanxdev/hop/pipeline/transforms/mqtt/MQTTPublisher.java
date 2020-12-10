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
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.phalanxdev.mqtt.SSLSocketFactoryGenerator;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * MQTT m_client step publisher
 *
 * @author Michael Spector
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 */
public class MQTTPublisher extends BaseTransform<MQTTPublisherMeta, MQTTPublisherData> implements
    ITransform<MQTTPublisherMeta, MQTTPublisherData> {
  
  protected MQTTPublisherMeta m_meta;
  protected MQTTPublisherData m_data;

  public MQTTPublisher( TransformMeta transformMeta, MQTTPublisherMeta meta, MQTTPublisherData data, int copyNr, PipelineMeta pipelineMeta,
      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    m_meta = meta;
    m_data = data;
  }

  @Override public void dispose() {
    super.dispose();
    shutdown( m_data );
  }
  
  protected void configureConnection( MQTTPublisherMeta meta, MQTTPublisherData data ) throws HopException {
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

      try {
        data.m_client = new MqttClient( broker, clientId );

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
		connectOptions.setCleanSession( meta.isCleanSession() ); //adding cleanSession Managmeent
        String lwTopic=meta.getLastWillTopic();
		String lwMessage=meta.getLastWillMessage();
		if(lwTopic!=null && lwMessage!=null)
		{
		connectOptions.setWill(meta.getLastWillTopic(),meta.getLastWillMessage().getBytes(),0,meta.isLastWillRetained());//adding Lastwill
		}
        String timeout = environmentSubstitute( meta.getTimeout() );
        try {
          connectOptions.setConnectionTimeout( Integer.parseInt( timeout ) );
        } catch ( NumberFormatException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongTimeoutValue.Message", timeout ), e );
        }

        logBasic( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.CreateMQTTClient.Message", broker, clientId,Boolean.toString(meta.isCleanSession()) ) );
        data.m_client.connect( connectOptions );

      } catch ( Exception e ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorCreateMQTTClient.Message", broker ),
            e );
      }
    }
  }

  public boolean processRow( ) throws HopException {
    Object[] r = getRow();
    if ( r == null ) {
      setOutputDone();
      return false;
    }
    
    IRowMeta inputRowMeta = getInputRowMeta();

    if ( first ) {
      first = false;

      // Initialize MQTT m_client:
      configureConnection( m_meta, m_data );

      data.m_outputRowMeta = getInputRowMeta().clone();
      m_meta.getFields( m_data.m_outputRowMeta, getTransformName(), null, null, this, getMetadataProvider() );

      String inputField = environmentSubstitute( meta.getField() );

      int numErrors = 0;
      if ( org.apache.hop.core.util.Utils.isEmpty( inputField ) ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.FieldNameIsNull" ) ); //$NON-NLS-1$
        numErrors++;
      }
      data.m_inputFieldNr = inputRowMeta.indexOfValue( inputField );
      if ( data.m_inputFieldNr < 0 ) {
        logError( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.CouldntFindField", inputField ) ); //$NON-NLS-1$
        numErrors++;
      }

      if ( numErrors > 0 ) {
        setErrors( numErrors );
        stopAll();
        return false;
      }
      data.m_inputFieldMeta = inputRowMeta.getValueMeta( data.m_inputFieldNr );
      data.m_topic = environmentSubstitute( meta.getTopic() );
      if ( meta.getTopicIsFromField() ) {
        data.m_topicFromFieldIndex = inputRowMeta.indexOfValue( data.m_topic );
        if ( data.m_topicFromFieldIndex < 0 ) {
          throw new HopException(
              "Incoming stream does not seem to contain the topic field '" + data.m_topic + "'" );
        }

        if ( inputRowMeta.getValueMeta( data.m_topicFromFieldIndex ).getType() != IValueMeta.TYPE_STRING ) {
          throw new HopException( "Incoming stream field to use for setting the topic must be of type string" );
        }
      }

      String qosValue = environmentSubstitute( meta.getQoS() );
      try {
        data.m_qos = Integer.parseInt( qosValue );
        if ( data.m_qos < 0 || data.m_qos > 2 ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongQOSValue.Message", qosValue ) );
        }
      } catch ( NumberFormatException e ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongQOSValue.Message", qosValue ), e );
      }
    }

    try {
      if ( !isStopped() ) {
        Object rawMessage = r[data.m_inputFieldNr];
        byte[] message = messageToBytes( rawMessage, data.m_inputFieldMeta );
        if ( message == null ) {
          logDetailed( "Incoming message value is null/empty - skipping" );
          return true;
        }

        // String topic = environmentSubstitute( meta.getTopic() );

        if ( meta.getTopicIsFromField() ) {
          if ( r[data.m_topicFromFieldIndex] == null || org.apache.hop.core.util.Utils.isEmpty( r[data.m_topicFromFieldIndex].toString() ) ) {
            // TODO add a default topic option, and then only skip if the default is null
            logDetailed( "Incoming topic value is null/empty - skipping message: " + rawMessage );
            return true;
          }
          data.m_topic = r[data.m_topicFromFieldIndex].toString();
        }

        MqttMessage mqttMessage = new MqttMessage( message );
        mqttMessage.setQos( data.m_qos );
		mqttMessage.setRetained(meta.isRetained()); //Adding retain option
        
        logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.SendingData", data.m_topic,
            Integer.toString( data.m_qos ),Boolean.toString(meta.isRetained()) ));
		
        if ( isRowLevel() ) {
          logRowlevel( data.m_inputFieldMeta.getString( r[data.m_inputFieldNr] ) );
        }
        try {
          data.m_client.publish( data.m_topic, mqttMessage );
        } catch ( MqttException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorPublishing.Message" ), e );
        }
      }
    } catch ( HopException e ) {
      if ( !getTransformMeta().isDoingErrorHandling() ) {
        logError(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorInStepRunning", e.getMessage() ) );
        setErrors( 1 );
        stopAll();
        setOutputDone();
        return false;
      }
      putError( getInputRowMeta(), r, 1, e.toString(), null, getTransformName() );
    }
    return true;
  }

  protected void shutdown( MQTTPublisherData data ) {
    if ( data.m_client != null ) {
      try {
        if ( data.m_client.isConnected() ) {
          data.m_client.disconnect();
        }
        data.m_client.close();
        data.m_client = null;
      } catch ( MqttException e ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorClosingMQTTClient.Message" ), e );
      }
    }
  }
  
  @Override public void stopRunning() throws HopException {
    shutdown( m_data );
    super.stopRunning();
  }

  protected byte[] messageToBytes( Object message, IValueMeta messageValueMeta ) throws HopValueException {
    if ( message == null || org.apache.hop.core.util.Utils.isEmpty( message.toString() ) ) {
      return null;
    }

    byte[] result = null;
    try {
      ByteBuffer buff = null;
      switch ( messageValueMeta.getType() ) {
        case IValueMeta.TYPE_STRING:
          result = message.toString().getBytes( "UTF-8" );
          break;
        case IValueMeta.TYPE_INTEGER:
        case IValueMeta.TYPE_DATE: // send the date as a long (milliseconds) value
          buff = ByteBuffer.allocate( 8 );
          buff.putLong( messageValueMeta.getInteger( message ) );
          result = buff.array();
          break;
        case IValueMeta.TYPE_NUMBER:
          buff = ByteBuffer.allocate( 8 );
          buff.putDouble( messageValueMeta.getNumber( message ) );
          result = buff.array();
          break;
        case IValueMeta.TYPE_TIMESTAMP:
          buff = ByteBuffer.allocate( 12 );
          Timestamp ts = (Timestamp) message;
          buff.putLong( ts.getTime() );
          buff.putInt( ts.getNanos() );
          result = buff.array();
          break;
        case IValueMeta.TYPE_BINARY:
          result = messageValueMeta.getBinary( message );
          break;
        case IValueMeta.TYPE_BOOLEAN:
          result = new byte[1];
          if ( messageValueMeta.getBoolean( message ) ) {
            result[0] = 1;
          }
          break;
        case IValueMeta.TYPE_SERIALIZABLE:
          if ( !( message instanceof Serializable ) ) {
            throw new HopValueException( "Message value is not serializable!" );
          }
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream( bos );
          oos.writeObject( message );
          oos.flush();
          result = bos.toByteArray();
          break;
      }
    } catch ( Exception ex ) {
      throw new HopValueException( ex );
    }

    return result;
  }
}

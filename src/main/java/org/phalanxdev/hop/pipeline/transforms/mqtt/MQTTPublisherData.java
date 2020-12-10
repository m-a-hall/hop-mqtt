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

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.eclipse.paho.client.mqttv3.MqttClient;

/**
 * Data class for MQTTPublisher
 *
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 */
public class MQTTPublisherData extends BaseTransformData implements ITransformData {

  protected MqttClient m_client;
  protected IRowMeta m_outputRowMeta;
  protected int m_inputFieldNr;
  protected IValueMeta m_inputFieldMeta;

  protected String m_topic = "";
  protected int m_topicFromFieldIndex = -1;
  protected int m_qos = 0;
}

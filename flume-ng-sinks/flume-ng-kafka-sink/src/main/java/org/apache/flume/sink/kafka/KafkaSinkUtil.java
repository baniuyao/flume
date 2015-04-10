/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.kafka;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaSinkUtil {

  private static final Logger log =
          LoggerFactory.getLogger(KafkaSinkUtil.class);

  public static Properties getKafkaProperties(Context context) {
    String producerType = context.getString(KafkaSinkConstants.FLUME_PRODUCER_TYPE);
    Properties kafkaProperties = new Properties();
    if (producerType == null) {
      throw new ConfigurationException("producer.type is null!");
    } else {
      Map<String, String> kafkaPropertiesMap =
              context.getSubProperties(KafkaSinkConstants.KAFKA_PROPERTY_PREFIX);
      for (Map.Entry<String, String> prop : kafkaPropertiesMap.entrySet()) {
        kafkaProperties.put(prop.getKey(), prop.getValue());
      }
      if (producerType.equals("java")) {
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      }
    }
    return kafkaProperties;
  }

}

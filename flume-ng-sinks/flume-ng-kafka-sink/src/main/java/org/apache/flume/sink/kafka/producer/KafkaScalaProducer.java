/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by yaorenjie on 4/9/15.
 */
public class KafkaScalaProducer extends AbstractKafkaProducer<KeyedMessage<String, byte[]>> {
  private Producer<String, byte[]> producer = null;

  public KafkaScalaProducer(Properties properties, Integer batchSize) {
    super(batchSize);
    producer = new Producer<String, byte[]>(new ProducerConfig(properties));
  }

  @Override
  public void send() {
    this.producer.send(messageList);
    this.messageList.clear();
  }

  @Override
  public void close() {
    this.producer.close();
  }
}

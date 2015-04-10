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

package org.apache.flume.sink.kafka;

import org.apache.flume.Context;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by yaorenjie on 4/10/15.
 */
public class TestKafkaSinkUtil {
  private Properties properties;
  private Context context = new Context();

  @Before
  public void setUp() {
    context.put("kafka.metadata.broker.list", "127.0.0.1:9092");
    context.put("batch.size", "1");
  }

  @Test
  public void testScalaProducerProperties() {
    context.put("producer.type", "scala");
    properties = KafkaSinkUtil.getKafkaProperties(context);
    assertEquals("127.0.0.1:9092", properties.getProperty("metadata.broker.list"));
    assertNull(properties.getProperty("key.serializer"));
    assertNull(properties.getProperty("value.serializer"));
  }

  @Test
  public void testJavaProducerProperties() {
    context.put("producer.type", "java");
    properties = KafkaSinkUtil.getKafkaProperties(context);
    assertEquals("127.0.0.1:9092", properties.getProperty("metadata.broker.list"));
    assertEquals("org.apache.kafka.common.serialization.StringSerializer", properties.getProperty("key.serializer"));
    assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", properties.getProperty("value.serializer"));
  }
}

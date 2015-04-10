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

import kafka.producer.KeyedMessage;
import org.apache.flume.*;
import org.apache.flume.Sink.Status;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.kafka.message.KafkaScalaMessageFactory;
import org.apache.flume.sink.kafka.producer.KafkaScalaProducer;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Created by yaorenjie on 4/10/15.
 */
public class TestKafkaScalaProducerSink {
  private KafkaSink mockKafkaSink;
  private KafkaScalaProducer mockKafkaScalaProducer;
  private KafkaScalaMessageFactory mockKafkaScalaMessageFactory;
  private Channel mockChannel;
  private Event mockEvent;
  private Transaction mockTx;

  @Before
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    mockKafkaScalaProducer = mock(KafkaScalaProducer.class);
    mockKafkaScalaMessageFactory = mock(KafkaScalaMessageFactory.class);
    mockKafkaSink = new KafkaSink();
    mockChannel = mock(Channel.class);
    mockEvent = mock(Event.class);
    mockTx = mock(Transaction.class);


    Field field = AbstractSink.class.getDeclaredField("channel");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockChannel);

    field = KafkaSink.class.getDeclaredField("topic");
    field.setAccessible(true);
    field.set(mockKafkaSink, "test-topic");

    field = KafkaSink.class.getDeclaredField("producer");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockKafkaScalaProducer);


    field = KafkaSink.class.getDeclaredField("messageFactory");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockKafkaScalaMessageFactory);

    field = KafkaSink.class.getDeclaredField("flumeSendBatchSize");
    field.setAccessible(true);
    field.set(mockKafkaSink, 1);

    when(mockChannel.take()).thenReturn(mockEvent);
    when(mockChannel.getTransaction()).thenReturn(mockTx);
  }

  @Test
  public void testProcessStatusReady() throws EventDeliveryException {
    when(mockEvent.getBody()).thenReturn("frank".getBytes());
    Status status = mockKafkaSink.process();
    verify(mockChannel, times(1)).getTransaction();
    verify(mockChannel, times(1)).take();
    verify(mockKafkaScalaMessageFactory, times(1)).createMessage("test-topic", null, "frank".getBytes());
    KeyedMessage<String, byte[]> message = mockKafkaScalaMessageFactory.createMessage("test-topic", null, "frank".getBytes());
    verify(mockKafkaScalaProducer, times(1)).addMessage(message);
    verify(mockKafkaScalaProducer, times(1)).send();
    verify(mockTx, times(1)).commit();
    verify(mockTx, times(1)).close();
    assertEquals(Status.READY, status);
  }
}

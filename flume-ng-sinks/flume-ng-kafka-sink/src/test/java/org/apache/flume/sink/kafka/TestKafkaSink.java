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

import org.apache.flume.*;
import org.apache.flume.Sink.Status;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.kafka.message.AbstractMessageFactory;
import org.apache.flume.sink.kafka.message.KafkaJavaMessageFactory;
import org.apache.flume.sink.kafka.message.KafkaScalaMessageFactory;
import org.apache.flume.sink.kafka.producer.AbstractKafkaProducer;
import org.apache.flume.sink.kafka.producer.KafkaJavaProducer;
import org.apache.flume.sink.kafka.producer.KafkaScalaProducer;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.apache.flume.sink.kafka.ProducerType.*;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Created by yaorenjie on 4/10/15.
 */
public class TestKafkaSink {
  private KafkaSink mockKafkaSink;
  private AbstractKafkaProducer mockKafkaProducer;
  private AbstractMessageFactory mockMessageFactory;
  private Channel mockChannel;
  private Event mockEvent;
  private Transaction mockTx;
  private Field field;

  @Before
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    mockKafkaSink = new KafkaSink();
    mockChannel = mock(Channel.class);
    mockEvent = mock(Event.class);
    mockTx = mock(Transaction.class);


    field = AbstractSink.class.getDeclaredField("channel");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockChannel);

    field = KafkaSink.class.getDeclaredField("topic");
    field.setAccessible(true);
    field.set(mockKafkaSink, "test-topic");

    when(mockChannel.take()).thenReturn(mockEvent);
    when(mockChannel.getTransaction()).thenReturn(mockTx);
  }

  private void testProcessStatusBackOff(ProducerType producerType) throws EventDeliveryException, NoSuchFieldException, IllegalAccessException {
    switch (producerType) {
      case SCALA:
        mockKafkaProducer = mock(KafkaScalaProducer.class);
        mockMessageFactory = mock(KafkaScalaMessageFactory.class);
        break;
      case JAVA:
        mockKafkaProducer = mock(KafkaJavaProducer.class);
        mockMessageFactory = mock(KafkaJavaMessageFactory.class);
        break;
    }

    field = KafkaSink.class.getDeclaredField("producer");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockKafkaProducer);

    field = KafkaSink.class.getDeclaredField("messageFactory");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockMessageFactory);

    field = KafkaSink.class.getDeclaredField("flumeSendBatchSize");
    field.setAccessible(true);
    field.set(mockKafkaSink, 10);

    when(mockEvent.getBody()).thenThrow(new RuntimeException());
    Status status = mockKafkaSink.process();
    verify(mockChannel, times(1)).getTransaction();
    verify(mockChannel, times(1)).take();
    verify(mockMessageFactory, times(0)).createMessage("test-topic", null, "frank".getBytes());
    verify(mockKafkaProducer, times(0)).addMessage(mockMessageFactory.createMessage("test-topic", null, "frank".getBytes()));
    verify(mockKafkaProducer, times(0)).send();
    verify(mockTx, times(0)).commit();
    verify(mockTx, times(1)).close();
    verify(mockTx, times(1)).rollback();
    assertEquals(Status.BACKOFF, status);
  }

  private void testProcessStatusReady(ProducerType producerType) throws EventDeliveryException, NoSuchFieldException, IllegalAccessException {
    switch (producerType) {
      case SCALA:
        mockKafkaProducer = mock(KafkaScalaProducer.class);
        mockMessageFactory = mock(KafkaScalaMessageFactory.class);
        break;
      case JAVA:
        mockKafkaProducer = mock(KafkaJavaProducer.class);
        mockMessageFactory = mock(KafkaJavaMessageFactory.class);
        break;
    }

    field = KafkaSink.class.getDeclaredField("producer");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockKafkaProducer);

    field = KafkaSink.class.getDeclaredField("messageFactory");
    field.setAccessible(true);
    field.set(mockKafkaSink, mockMessageFactory);

    field = KafkaSink.class.getDeclaredField("flumeSendBatchSize");
    field.setAccessible(true);
    field.set(mockKafkaSink, 10);

    when(mockEvent.getBody()).thenReturn("frank".getBytes());
    Status status = mockKafkaSink.process();
    verify(mockChannel, times(1)).getTransaction();
    verify(mockChannel, times(10)).take();
    verify(mockMessageFactory, times(10)).createMessage("test-topic", null, "frank".getBytes());
    verify(mockKafkaProducer, times(10)).addMessage(mockMessageFactory.createMessage("test-topic", null, "frank".getBytes()));
    verify(mockKafkaProducer, times(1)).send();
    verify(mockTx, times(1)).commit();
    verify(mockTx, times(1)).close();
    assertEquals(Status.READY, status);
  }

  @Test
  public void testProcessStatusReadyWithJavaProducer() throws EventDeliveryException, NoSuchFieldException, IllegalAccessException {
    testProcessStatusReady(JAVA);
  }

  @Test
  public void testProcessStatusReadyWithScalaProducer() throws EventDeliveryException, NoSuchFieldException, IllegalAccessException {
    testProcessStatusReady(SCALA);
  }

//  @Test(expected=EventDeliveryException.class)
  @Test
  public void testProcessStatusBackOffWithJavaProducer() throws EventDeliveryException, NoSuchFieldException, IllegalAccessException {
    testProcessStatusBackOff(JAVA);
  }

//  @Test(expected=EventDeliveryException.class)
  @Test
  public void testProcessStatusBackOffWithScalaProducer() throws EventDeliveryException, NoSuchFieldException, IllegalAccessException {
    testProcessStatusBackOff(SCALA);
  }
}

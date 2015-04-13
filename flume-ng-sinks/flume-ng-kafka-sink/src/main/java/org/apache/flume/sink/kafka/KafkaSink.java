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

import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.kafka.message.AbstractMessageFactory;
import org.apache.flume.sink.kafka.message.KafkaJavaMessageFactory;
import org.apache.flume.sink.kafka.message.KafkaScalaMessageFactory;
import org.apache.flume.sink.kafka.producer.AbstractKafkaProducer;
import org.apache.flume.sink.kafka.producer.KafkaJavaProducer;
import org.apache.flume.sink.kafka.producer.KafkaScalaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;

/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p/>
 * Any property starting with "kafka." will be passed along to the
 * Kafka producer. And other properties will be considered as parameters
 * pass to the flume.
 * Read the Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties
 * topic - there's NO default value, and also - this can be in the event header if
 * you need to support events with different topics
 * batch.size - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * producer.type - can be "scala" or "java". Because Kafka use new API since 0.8.2,
 * the kafka sink will both support the new API and the old API. "scala" refers to
 * the old Scala API and "java" refers to the new Java API. They both get work in
 * 0.8.1 and 0.8.2.
 * <p/>
 * header properties (per event):
 * topic
 * key
 */
public class KafkaSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
  public static final String KEY_HDR = "key";
  public static final String TOPIC_HDR = "topic";
  private Properties kafkaProperties;
  private String producerType;
  private AbstractKafkaProducer producer;
  private AbstractMessageFactory messageFactory;
  private String topic;
  private int flumeSendBatchSize;

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = null;
    Event event = null;
    String eventTopic = null;
    String eventKey = null;

    try {
      long processedEvents = 0;

      transaction = channel.getTransaction();
      transaction.begin();

      for (; processedEvents < flumeSendBatchSize; processedEvents += 1) {
        event = channel.take();

        if (event == null) {
          // no events available in channel
          break;
        }

        byte[] eventBody = event.getBody();
        Map<String, String> headers = event.getHeaders();

        if ((eventTopic = headers.get(TOPIC_HDR)) == null) {
          eventTopic = topic;
        }

        eventKey = headers.get(KEY_HDR);

        if (logger.isDebugEnabled()) {
          logger.debug("{Event} " + eventTopic + " : " + eventKey + " : "
            + new String(eventBody, "UTF-8"));
          logger.debug("event #{}", processedEvents);
        }

        // create a message and add to buffer
        producer.addMessage(messageFactory.createMessage(eventTopic, eventKey, eventBody));

      }

      // publish batch and commit.
      if (processedEvents > 0) {
        producer.send();
      }

      transaction.commit();

    } catch (Exception ex) {
      String errorMsg = "Failed to publish events";
      logger.error("Failed to publish events", ex);
      result = Status.BACKOFF;
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (Exception e) {
          logger.error("Transaction rollback failed", e);
          throw Throwables.propagate(e);
        }
      }
    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }

    return result;
  }

  @Override
  public synchronized void start() {
    // instantiate the producer
    if (producerType.equals("scala")) {
      producer = new KafkaScalaProducer(kafkaProperties, flumeSendBatchSize);
      messageFactory = new KafkaScalaMessageFactory();
    } else if (producerType.equals("java")) {
      producer = new KafkaJavaProducer(kafkaProperties, flumeSendBatchSize);
      messageFactory = new KafkaJavaMessageFactory();
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    producer.close();
    super.stop();
  }


  /**
   *
   * We add the configuration users added for Kafka (parameters starting
   * with .kafka. and must be valid Kafka Producer properties
   *
   * @param context
   */
  @Override
  public void configure(Context context) {
    flumeSendBatchSize = context.getInteger(KafkaSinkConstants.FLUME_SEND_BATCH_SIZE,
      KafkaSinkConstants.DEFAULT_FLUME_SEND_BATCH_SIZE);
    producerType = context.getString(KafkaSinkConstants.FLUME_PRODUCER_TYPE);
    topic = context.getString(KafkaSinkConstants.FLUME_TOPIC);
    kafkaProperties = KafkaSinkUtil.getKafkaProperties(context);
  }
}

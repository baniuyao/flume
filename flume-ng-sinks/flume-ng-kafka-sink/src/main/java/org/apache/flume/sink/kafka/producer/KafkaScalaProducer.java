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

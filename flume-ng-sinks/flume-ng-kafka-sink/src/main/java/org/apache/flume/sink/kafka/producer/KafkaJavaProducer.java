package org.apache.flume.sink.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by yaorenjie on 4/9/15.
 */
public class KafkaJavaProducer extends AbstractKafkaProducer<ProducerRecord<String, byte[]>> {
  private KafkaProducer producer = null;

  public KafkaJavaProducer(Properties properties, Integer batchSize) {
    super(batchSize);
    this.producer = new KafkaProducer(properties);
  }

  @Override
  public void send() {
    for (int i = 0; i < this.messageList.size(); i++) {
      producer.send(messageList.get(i));
    }
    messageList.clear();
  }

  @Override
  public void close() {
    producer.close();
  }
}

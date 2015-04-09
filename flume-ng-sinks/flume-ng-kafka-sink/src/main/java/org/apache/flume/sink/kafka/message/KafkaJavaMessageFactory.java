package org.apache.flume.sink.kafka.message;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by yaorenjie on 4/9/15.
 */
public class KafkaJavaMessageFactory extends AbstractMessageFactory<ProducerRecord<String, byte[]>> {
  @Override
  public ProducerRecord<String, byte[]> createMessage(String topic, String key, byte[] value) {
    return new ProducerRecord<String, byte[]>(topic, key, value);
  }
}

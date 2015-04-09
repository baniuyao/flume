package org.apache.flume.sink.kafka.message;

import kafka.producer.KeyedMessage;

/**
 * Created by yaorenjie on 4/9/15.
 */
public class KafkaScalaMessageFactory extends AbstractMessageFactory<KeyedMessage<String, byte[]>> {

  @Override
  public KeyedMessage<String, byte[]> createMessage(String topic, String key, byte[] value) {
    return new KeyedMessage<String, byte[]>(topic, key, value);
  }
}

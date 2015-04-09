package org.apache.flume.sink.kafka.message;


/**
 * Created by yaorenjie on 4/9/15.
 */
public abstract class AbstractMessageFactory<T> {
  public abstract T createMessage(String topic, String key, byte[] value);
}

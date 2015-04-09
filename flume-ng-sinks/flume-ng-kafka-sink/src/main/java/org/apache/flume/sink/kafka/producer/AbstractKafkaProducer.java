package org.apache.flume.sink.kafka.producer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yaorenjie on 4/9/15.
 */
public abstract class AbstractKafkaProducer<M> {
  protected List<M> messageList = new ArrayList<M>();

  public AbstractKafkaProducer(Integer batchSize) {
    this.messageList = new ArrayList<M>(batchSize);
  }

  public void addMessage(M message) {
    this.messageList.add(message);
  }

  public abstract void send();

  public abstract void close();
}

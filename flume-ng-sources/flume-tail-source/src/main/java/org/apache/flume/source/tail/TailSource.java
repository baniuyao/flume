/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.tail;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link org.apache.flume.source.tail.TailSource} is a source to tail a file into channel.
 * We can use {@link org.apache.flume.source.ExecSource} with running "tail -F" to do this.
 * But using "tail -F" has a fetal problem - it cannot continously feed lines after restart
 * flume. Now this class will write offset information to an offset file to track this.
 */

public class TailSource extends AbstractSource implements Configurable, PollableSource {
  private static final Logger LOG = LoggerFactory.getLogger(TailSource.class);
  private String fileName;
  private Integer batchSize;
  private Long batchTime;
  private List<Event> eventList = new ArrayList<Event>();
  private TailProcess tailProcess;
  private String currentLine;
  private Integer maxOffsetFileSizeMB;

  @Override
  public Status process() throws EventDeliveryException {
    Event event;
    Long batchStartTime = System.currentTimeMillis();
    Long batchEndTime = batchStartTime + batchTime;
    while (eventList.size() < batchSize && System.currentTimeMillis() < batchEndTime) {
      try {
        currentLine = tailProcess.tailOneLine();
      } catch (IOException e) {
        LOG.error("tailOneLine IOException: {}", e.getMessage());
        return Status.BACKOFF;
      }
      if (currentLine != null) {
        LOG.debug("tailOneLine: {}", currentLine);
        event = EventBuilder.withBody(currentLine.getBytes());
        eventList.add(event);
        try {
          tailProcess.commit();
        } catch (IOException e) {
          LOG.error("cannot commit tail: {}", e.getMessage());
          return Status.BACKOFF;
        }
      }
    }
    if (eventList.size() > 0) {
      getChannelProcessor().processEventBatch(eventList);
      eventList.clear();
      return Status.READY;
    }
    return Status.READY;
  }

  @Override
  public void configure(Context context) {
    fileName = context.getString(TailSourceConstants.FILE_NAME);
    if (fileName == null) {
      throw new ConfigurationException("must specify file.name");
    }
    batchSize = context.getInteger(TailSourceConstants.BATCH_SIZE, TailSourceConstants.DEFAULT_BATCH_SIZE);
    batchTime = context.getLong(TailSourceConstants.BATCH_TIME_SEC, TailSourceConstants.DEFAULT_BATCH_TIME_SEC);
    maxOffsetFileSizeMB = context.getInteger(TailSourceConstants.MAX_OFFSET_FILE_SIZE_MB, TailSourceConstants.DEFAULT_MAX_OFFSET_FILE_SIZE_MB);
  }

  @Override
  public synchronized void start() {
    try {
      tailProcess = new TailProcess(fileName, this.getName(), maxOffsetFileSizeMB);
    } catch (IOException e) {
      LOG.error("IOException in tailProcess");
    }
    super.start();
  }
}

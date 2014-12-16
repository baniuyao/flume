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
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ybaniu on 12/1/14.
 */
public class TailSource extends AbstractSource implements Configurable, PollableSource {
    private String fileName;
    private Integer batchSizeUpperLimit;
    private Long batchTimeUpperLimit;
    private List<Event> eventList = new ArrayList<Event>();
    private TailProcess tailProcess;
    private String currentLine;

    private static final Logger LOG = LoggerFactory.getLogger(TailSource.class);
    @Override
    public Status process() throws EventDeliveryException {
        Event event;
        Long batchStartTime = System.currentTimeMillis();
        Long batchEndTime = batchStartTime + batchTimeUpperLimit;
        while (eventList.size() < batchSizeUpperLimit && System.currentTimeMillis() < batchEndTime) {
            try {
                currentLine = tailProcess.tailOneLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            event = EventBuilder.withBody(currentLine.getBytes());
            eventList.add(event);
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
        batchSizeUpperLimit = context.getInteger(TailSourceConstants.BATCH_SIZE_LIMIT, TailSourceConstants.DEFAULT_BATCH_SIZE_LIMIT);
        batchTimeUpperLimit = context.getLong(TailSourceConstants.BATCH_TIME_LIMIT);
    }

    @Override
    public synchronized void start() {
        try {
            tailProcess = new TailProcess(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        super.start();
    }
}

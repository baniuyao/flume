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

package org.apache.flume.source.tail;

import org.apache.commons.io.FileUtils;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.LoggerSink;
import org.apache.flume.sink.RollingFileSink;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by yaorenjie on 4/13/15.
 */
public class TestTailSource {

  private TailSource tailSource;
  private Context tailContext;
  private MemoryChannel memoryChannel;
  private Context channelContext;
  private RollingFileSink fileSink;
  private Context sinkContext;

  private DefaultSinkProcessor sinkProcessor;
  private SinkRunner sinkRunner;
  private ChannelSelector channelSelector;
  private ChannelProcessor channelProcessor;

  @Before
  public void setUp() throws InterruptedException, EventDeliveryException, IOException {
    cleanAll();
    tailContext = new Context();
    tailContext.put("file.name", "/tmp/test.log");
    tailSource = new TailSource();
    tailSource.setName("tail_test");
    tailSource.configure(tailContext);

    channelContext = new Context();
    channelContext.put("capacity", "100000");
    memoryChannel = new MemoryChannel();
    memoryChannel.setName("tailChannel");
    memoryChannel.configure(channelContext);

    fileSink = new RollingFileSink();
    sinkContext = new Context();
    sinkContext.put("sink.directory", "/tmp/flume");
    sinkContext.put("sink.rollInterval", "0");
    sinkContext.put("batchSize", "1");
    fileSink.configure(sinkContext);
    fileSink.setChannel(memoryChannel);
    fileSink.start();

    sinkProcessor = new DefaultSinkProcessor();
    sinkProcessor.setSinks(Collections.<Sink>singletonList(fileSink));
    sinkRunner = new SinkRunner(sinkProcessor);
    sinkRunner.start();

    channelSelector = new ReplicatingChannelSelector();
    channelSelector.setChannels(Collections.<Channel>singletonList(memoryChannel));
    channelProcessor = new ChannelProcessor(channelSelector);
    tailSource.setChannelProcessor(channelProcessor);
//    tailSource.start();
//    tailSource.process();
//
//    Thread.sleep(5000L);
//    tailSource.stop();
//    sinkRunner.stop();
//    fileSink.stop();
  }

  private void cleanAll() throws IOException {
    File file = new File("/tmp/test.log");
    File file2 = new File("/tmp/flume");
    if (file2.exists()) {
      file2.delete();
    }
    file2.mkdirs();
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
  }

  private void stopAll() {
    tailSource.stop();
    sinkRunner.stop();
    fileSink.stop();
  }

  private Boolean isTwoFileSame(File expectedFile, File resultFile) throws IOException {
    BufferedReader expectedBufferReader = new BufferedReader(new FileReader(expectedFile));
    BufferedReader resultBufferReader = new BufferedReader(new FileReader(resultFile));
    String s1 = "";
    String s2 = "";
    String y = "", z = "";
    while ((z = expectedBufferReader.readLine()) != null) s2 += z;
    while ((y = resultBufferReader.readLine()) != null) s1 += y;
    return s1.equals(s2);
  }
  @Test
  public void testBaseRead() throws IOException, InterruptedException, EventDeliveryException {
    List<String> expectedLines = new ArrayList<String>();
    List<String> resultLines = new ArrayList<String>();
    boolean flag = true;
    tailSource.start();
    Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/test.log", true)));
    for (int i = 0; i < 10; i ++){
      String line = "hello, world - " + i;
      writer.write(line + "\n");
      expectedLines.add(line);
    }
    writer.close();
    for (int i = 0; i< 10; i ++) {
      tailSource.process();
    }
    Thread.sleep(5000L);
    tailSource.stop();
    sinkRunner.stop();
    fileSink.stop();
    File flumeDirectory = new File("/tmp/flume");
    File resultFile = flumeDirectory.listFiles()[0];
    assert isTwoFileSame(new File("/tmp/test.log"), resultFile);
  }
}

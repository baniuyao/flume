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
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.RollingFileSink;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Collections;

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
  private File logFile = new File("/tmp/test.log");
  private File flumeDirectory = new File("/tmp/flume");
  private File toCompareFile = new File("/tmp/test_all.log");
  private File rotatedFile = new File("/tmp/test.log.2");

  @Before
  public void setUp() throws InterruptedException, EventDeliveryException, IOException {
    FileUtils.forceDeleteOnExit(logFile);
    FileUtils.forceDeleteOnExit(flumeDirectory);
    FileUtils.forceDeleteOnExit(toCompareFile);
    FileUtils.forceDeleteOnExit(rotatedFile);
    FileUtils.touch(toCompareFile);
    FileUtils.forceMkdir(flumeDirectory);
    FileUtils.touch(logFile);
    tailContext = new Context();
    tailContext.put("file.name", logFile.getAbsolutePath());
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
    sinkContext.put("sink.directory", flumeDirectory.getAbsolutePath());
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
  }

  private Boolean isTwoFileSame(File expectedFile, File resultFile) throws IOException {
    BufferedReader expectedBufferReader = new BufferedReader(new FileReader(expectedFile));
    BufferedReader resultBufferReader = new BufferedReader(new FileReader(resultFile));
    String resultLinesString = "";
    String expectedLinesString = "";
    String resultLine, expectedLine;
    while ((expectedLine = expectedBufferReader.readLine()) != null) expectedLinesString += expectedLine;
    while ((resultLine = resultBufferReader.readLine()) != null) resultLinesString += resultLine;
    System.out.println(expectedFile.getName() + ":" + expectedLinesString);
    System.out.println(resultFile.getName() + ":" + resultLinesString);
    return resultLinesString.equals(expectedLinesString);
  }

//  @Test
  public void testBaseRead() throws IOException, InterruptedException, EventDeliveryException {
    tailSource.start();
    Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile, true)));
    for (int i = 0; i < 10; i ++){
      String line = "hello, world - " + i;
      writer.write(line + "\n");
    }
    writer.close();
    for (int i = 0; i< 10; i ++) {
      tailSource.process();
    }
    Thread.sleep(5000L);
    tailSource.stop();
    sinkRunner.stop();
    fileSink.stop();
    File resultFile = flumeDirectory.listFiles()[0];
    assert isTwoFileSame(logFile, resultFile);
  }

  @Test
  public void testFileRotate() throws IOException, EventDeliveryException, InterruptedException {
    tailSource.start();
    Writer toRotateWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile, true)));
    Writer allContentWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(toCompareFile, true)));
    for (int i = 0; i < 10; i ++){
      String line = "hello, world - " + i;
      toRotateWriter.write(line + "\n");
      allContentWriter.write(line + "\n");
    }
    for (int i = 0; i< 10; i ++) {
      tailSource.process();
    }
    toRotateWriter.flush();
    FileUtils.forceDeleteOnExit(rotatedFile);
    FileUtils.moveFile(logFile, rotatedFile);
    FileUtils.touch(logFile);
    for (int i = 11; i < 20; i ++){
      String line = "hello, world - " + i;
      toRotateWriter.write(line + "\n");
      allContentWriter.write(line + "\n");
    }
    toRotateWriter.close();
    allContentWriter.close();
    for (int i = 0; i< 10; i ++) {
      tailSource.process();
    }
    Thread.sleep(1000L);
    tailSource.stop();
    sinkRunner.stop();
    fileSink.stop();
    File resultFile = flumeDirectory.listFiles()[0];
//    assert isTwoFileSame(toCompareFile, resultFile);
    System.out.println("a");
  }
}

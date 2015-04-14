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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Tests for {@link org.apache.flume.source.tail.TailProcess}
 */
public class TestTailProcess {
  private static final Logger LOG = LoggerFactory.getLogger(TestTailProcess.class);
  @Test
  public void testRestartTailProcess() throws IOException, InterruptedException {
    String result = "";
    File testRestartFile = new File("/tmp/testRestart.log");
    File testRestartOffsetFile = new File("/tmp/testRestart.log.offset_suffix");
    FileUtils.deleteQuietly(testRestartFile);
    FileUtils.touch(testRestartFile);
    FileUtils.deleteQuietly(testRestartOffsetFile);
    FileUtils.touch(testRestartOffsetFile);

    TailProcess tailProcess = new TailProcess("/tmp/testRestart.log", "suffix", 1);
    for (int i = 0; i < 10; i++) {
      String line = "hello, world - " + i + "\n";
      FileUtils.writeStringToFile(testRestartFile, line, true);
    }
    String tmpString;
    while ((tmpString = tailProcess.tailOneLine()) != null) {
      result += tmpString + "\n";
      tailProcess.commit();
    }
    for (int i = 10; i < 20; i++) {
      String line = "hello, world - " + i + "\n";
      FileUtils.writeStringToFile(testRestartFile, line, true);
    }
    tailProcess = new TailProcess("/tmp/testRestart.log", "suffix", 1);
    while ((tmpString = tailProcess.tailOneLine()) != null) {
      result += tmpString + "\n";
      tailProcess.commit();
    }
    assert FileUtils.readFileToString(testRestartFile).equals(result);
  }
}

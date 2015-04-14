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

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by yaorenjie on 4/14/15.
 */
public class TestTailSourceUtils {
  @Test
  public void testReadLastLineFromFile() throws IOException {
    File file = new File("/tmp/readLastLine.log");
    FileUtils.writeStringToFile(file, "first line\n", true);
    FileUtils.writeStringToFile(file, "second line\n", true);
    FileUtils.writeStringToFile(file, "third line\n", true);
    assertEquals("third line", TailSourceUtils.readLastLineFromFile(file));
  }
}

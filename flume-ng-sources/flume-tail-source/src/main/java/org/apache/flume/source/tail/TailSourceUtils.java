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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * static utils
 */
public class TailSourceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TailSourceUtils.class);

  public static String readLastLineFromFile(File file) {
    String lastLine = null;
    String currentLine;
    try {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      while ((currentLine = bufferedReader.readLine()) != null) {
        lastLine = currentLine;
      }
    } catch (IOException e) {
      LOG.error("readLastLineFormat IOException: {}", file.getAbsoluteFile());
    }
    return lastLine;
  }

}

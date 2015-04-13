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
 * Created by ybaniu on 12/16/14.
 */
public class TailProcess {
  private static final Logger LOG = LoggerFactory.getLogger(TailProcess.class);
  private File file;
  private FileReader fileReader;
  private BufferedReader bufferedReader;
  private TailOffsetKeeper tailOffsetKeeper = null;
  private Integer currentLineLength;

  public TailProcess(String fileName, String offsetSuffix) throws IOException {
    this.file = new File(fileName);
    this.fileReader = new FileReader(file);
    this.bufferedReader = new BufferedReader(fileReader);
    tailOffsetKeeper = new TailOffsetKeeper(fileName + ".offset_" + offsetSuffix);
    bufferedReader.skip(tailOffsetKeeper.getLatestOffset());
  }

  public void commit() throws IOException {
    tailOffsetKeeper.updateOffset(currentLineLength);
  }

  public String tailOneLine() throws IOException {
    checkFileRotate();
    String currentLine = bufferedReader.readLine();
    if (currentLine != null) {
      currentLineLength = currentLine.length();
    }
    return currentLine;
  }


  private void checkFileRotate() {
    if (!file.exists()) {
      LOG.info("file does not exist");
      LOG.debug("begin loop");
      while (true) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          // TODO: how to handle this exception?
          LOG.error("thread sleep error");
        }
        LOG.debug("wait");
        if (file.exists()) {
          LOG.debug("file appear");
          try {
            LOG.debug("refresh fileReader");
            fileReader = new FileReader(file);
            tailOffsetKeeper.rotate();
          } catch (FileNotFoundException e) {
            e.printStackTrace();
          }
          LOG.debug("refresh bufferedReader");
          bufferedReader = new BufferedReader(fileReader);
          break;
        }
      }
    }
  }
}

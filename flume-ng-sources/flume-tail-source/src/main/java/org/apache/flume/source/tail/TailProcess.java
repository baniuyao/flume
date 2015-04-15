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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * This class is used to run the main tail method. It will update offset using
 * {@link org.apache.flume.source.tail.TailOffsetKeeper}. Furthermore, it will
 * check log file rotation by checking file size.
 */
public class TailProcess {
  private static final Logger LOG = LoggerFactory.getLogger(TailProcess.class);
  private File file;
  private FileReader fileReader;
  private BufferedReader bufferedReader;
  private TailOffsetKeeper tailOffsetKeeper = null;
  private Integer currentLineLength;
  private Long fileSize;

  public TailProcess(String fileName, String offsetSuffix, Integer offsetFileMaxSizeMB) throws IOException {
    this.file = new File(fileName);
    this.fileSize = FileUtils.sizeOf(file);
    this.fileReader = new FileReader(file);
    this.bufferedReader = new BufferedReader(fileReader);
    tailOffsetKeeper = new TailOffsetKeeper(fileName + ".offset_" + offsetSuffix, offsetFileMaxSizeMB);
    bufferedReader.skip(tailOffsetKeeper.getLatestOffset());
  }

  public void commit() throws IOException {
    tailOffsetKeeper.updateOffset(currentLineLength);
    fileSize = FileUtils.sizeOf(file);
  }

  public String tailOneLine() throws IOException {
    String currentLine = bufferedReader.readLine();
    if (currentLine != null) {
      currentLineLength = currentLine.length();
    } else {
      checkFileRotate();
    }
    return currentLine;
  }


  private void checkFileRotate() {
    // flume found the log file missing, waiting for new log file.
    if (!file.exists()) {
      LOG.info("file does not exist");
      LOG.debug("begin loop");
      while (true) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          // it should not happened
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
      // flume didn't found the log file missing. it need to check the file size
    } else {
      if (FileUtils.sizeOf(file) < fileSize) {
        try {
          fileReader = new FileReader(file);
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
        tailOffsetKeeper.rotate();
        bufferedReader = new BufferedReader(fileReader);
      }
    }
  }
}

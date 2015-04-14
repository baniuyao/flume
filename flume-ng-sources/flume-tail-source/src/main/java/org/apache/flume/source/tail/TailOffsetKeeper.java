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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * {@link org.apache.flume.source.tail.TailOffsetKeeper} is used to keep offset
 * information.
 */
public class TailOffsetKeeper {
  private static final Logger LOG = LoggerFactory.getLogger(TailOffsetKeeper.class);
  private Long currentOffset = 0L;
  private File offsetFile;
  private Integer maxSizeMB;

  public TailOffsetKeeper(String offsetFileName, Integer maxSizeMB) {
    this.offsetFile = new File(offsetFileName);
    this.currentOffset = getLatestOffset();
    this.maxSizeMB = maxSizeMB;
  }
  public void updateOffset(Integer offset) throws IOException {
    // TODO: offset file rotate
    Long ts = System.currentTimeMillis();
//    if (System.currentTimeMillis() - TailSourceUtils.getFileCreationTime(offsetFile) > 30000) {
//    LOG.debug("size: {}", FileUtils.sizeOf(offsetFile));
    if (FileUtils.sizeOf(offsetFile) > 1024 * maxSizeMB) {
      LOG.debug("offset file rotated");
      FileUtils.deleteQuietly(offsetFile);
      FileUtils.touch(offsetFile);
    }
    // "+ 1" here is because of the "\n"
    currentOffset = currentOffset + offset + 1;
    FileUtils.writeStringToFile(offsetFile, System.currentTimeMillis() + "|" + currentOffset + "\n", true);
  }

  public void rotate() {
    try {
      FileUtils.deleteQuietly(offsetFile);
      FileUtils.touch(offsetFile);
      this.currentOffset = 0L;
      LOG.debug("offsetKeeper: {} rotate", offsetFile.getAbsoluteFile());
    } catch (FileNotFoundException e) {
      LOG.error("offset file not found: " + offsetFile.getAbsolutePath());
    } catch (IOException e) {
      LOG.error("IO Exception, offset file: " + offsetFile.getAbsolutePath());
    }

  }

  public Long getLatestOffset() {
    String lastLine = TailSourceUtils.readLastLineFromFile(offsetFile);
    if (lastLine == null) {
      return 0L;
    } else {
      return Long.valueOf(lastLine.split("\\|")[1]);
    }
  }
}

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
public class TailOffsetKeeper {
    private static final Logger LOG = LoggerFactory.getLogger(TailOffsetKeeper.class);
    private Long currentOffset = 0L;
    private String offsetFileName;
    public TailOffsetKeeper(String fileName) throws IOException {
        this.offsetFileName = fileName;
        this.currentOffset = getLatestOffset();
    }

    public void updateOffset(Integer offset) throws IOException {
        Writer writer = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(offsetFileName)));
        // "+ 1" here is because of the "\n"
        currentOffset = currentOffset + offset + 1;
        writer.write(System.currentTimeMillis() + "|" + currentOffset +"\n");
        writer.close();
    }

    public void rotate() {
        try {
            Writer writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(offsetFileName, true)));
            writer.close();
            this.currentOffset = 0L;
            LOG.debug("offsetKeeper: {} rotate", offsetFileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Long getLatestOffset() throws IOException {
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(offsetFileName)));
        } catch (FileNotFoundException e) {
            return 0L;
        }
        String currentLine = null;
        String tmp = null;
        while ((tmp = bufferedReader.readLine()) != null) {
            currentLine = tmp;
        }
        if (currentLine != null) {
            return Long.valueOf(currentLine.split("\\|")[1]);
        } else {
            return 0L;
        }
    }
}

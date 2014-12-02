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

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import java.io.*;
import java.util.concurrent.TimeUnit;

/** Make workbench of open file handler, write offset and close it.
 *  Result:
 *  org.apache.flume.source.tail.TestWriteFile:
 requests:
 count = 2112754
 mean rate = 61924.45 writes/s
 1-minute rate = 57390.89 writes/s
 5-minute rate = 54840.56 writes/s
 15-minute rate = 54323.34 writes/s
 * Created by ybaniu on 12/1/14.
 */
public class TestWriteFile {
    public static final Meter meter = Metrics.newMeter(TestWriteFile.class, "requests", "writes", TimeUnit.SECONDS);
    public static void main(String[] args) throws IOException, InterruptedException {
        Writer writer = null;
//        ConsoleReporter.enable(1, TimeUnit.SECONDS);
        while (true) {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/test_write.log", true)));
            writer.write("hello");
            writer.close();
            Thread.sleep(5000L);
//            meter.mark();
        }
//        writer.close();
    }
}

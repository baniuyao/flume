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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by ybaniu on 12/1/14.
 */
public class TestTailRotate {
  public static void main(String[] args) throws IOException, InterruptedException {
    File f = new File("/tmp/test.log");
    FileReader fr = new FileReader(f);
    BufferedReader br = new BufferedReader(fr);
    String currentLine = null;
    while (true) {
      if (!f.exists()) {
        while (true) {
          if (f.exists()) {
            fr = new FileReader(f);
            br = new BufferedReader(fr);
            break;
          }
        }
      }
      if ((currentLine = br.readLine()) != null) {
        System.out.println(currentLine);
      }
//            Thread.sleep(3000L);
    }
  }
}

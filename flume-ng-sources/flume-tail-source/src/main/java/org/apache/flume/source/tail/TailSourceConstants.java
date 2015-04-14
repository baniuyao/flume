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

/**
 * Constants used in flume configuration.
 */
public class TailSourceConstants {
  public static final String FILE_NAME = "file.name";
  public static final String BATCH_SIZE = "batch.size";
  public static final String BATCH_TIME_SEC = "batch.time.sec";
  public static final String MAX_OFFSET_FILE_SIZE_MB = "offset.max.size.mb";

  public static final Integer DEFAULT_BATCH_SIZE = 1;
  public static final Long DEFAULT_BATCH_TIME_SEC = 1000L;
  public static Integer DEFAULT_MAX_OFFSET_FILE_SIZE_MB = 128;
}

/**
 * Copyright 2016 Thomas Feng
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.tfeng.toolbox.kafka;

import java.io.IOException;

import org.apache.avro.generic.IndexedRecord;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import me.tfeng.toolbox.avro.AvroHelper;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class AvroEncoder<T extends IndexedRecord> implements Encoder<T> {

  public AvroEncoder() {
    this(null);
  }

  public AvroEncoder(VerifiableProperties verifiableProperties) {
  }

  @Override
  public byte[] toBytes(T record) {
    try {
      return AvroHelper.encodeRecord(record);
    } catch (IOException e) {
      throw new RuntimeException("Unable to encode Kafka event " + record);
    }
  }
}

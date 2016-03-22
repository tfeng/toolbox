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

package me.tfeng.toolbox.mongodb;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class LoggingDecoder extends Decoder {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingDecoder.class);

  private final Decoder decoder;

  public LoggingDecoder(Decoder decoder) throws IOException {
    this.decoder = decoder;
  }

  @Override
  public long arrayNext() throws IOException {
    long result = decoder.arrayNext();
    LOG.info("arrayNext() = " + result);
    return result;
  }

  @Override
  public long mapNext() throws IOException {
    long result = decoder.mapNext();
    LOG.info("mapNext() = " + result);
    return result;
  }

  @Override
  public long readArrayStart() throws IOException {
    long result = decoder.readArrayStart();
    LOG.info("readArrayStart() = " + result);
    return result;
  }

  @Override
  public boolean readBoolean() throws IOException {
    boolean result = decoder.readBoolean();
    LOG.info("readBoolean() = " + result);
    return result;
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    ByteBuffer result = decoder.readBytes(old);
    LOG.info("readBytes(...) = ...");
    return result;
  }

  @Override
  public double readDouble() throws IOException {
    double result = decoder.readDouble();
    LOG.info("readDouble() = " + result);
    return result;
  }

  @Override
  public int readEnum() throws IOException {
    int result = decoder.readEnum();
    LOG.info("readEnum() = " + result);
    return result;
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    decoder.readFixed(bytes, start, length);
    LOG.info("readFixed(..., " + start + ", " + length + ")");
  }

  @Override
  public float readFloat() throws IOException {
    float result = decoder.readFloat();
    LOG.info("readFloat() = " + result);
    return result;
  }

  @Override
  public int readIndex() throws IOException {
    int result = decoder.readIndex();
    LOG.info("readIndex() = " + result);
    return result;
  }

  @Override
  public int readInt() throws IOException {
    int result = decoder.readInt();
    LOG.info("readInt() = " + result);
    return result;
  }

  @Override
  public long readLong() throws IOException {
    long result = decoder.readLong();
    LOG.info("readLong() = " + result);
    return result;
  }

  @Override
  public long readMapStart() throws IOException {
    long result = decoder.readMapStart();
    LOG.info("readMapStart() = " + result);
    return result;
  }

  @Override
  public void readNull() throws IOException {
    decoder.readNull();
    LOG.info("readNull()");
  }

  @Override
  public String readString() throws IOException {
    String result = decoder.readString();
    LOG.info("readString() = " + result);
    return result;
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    Utf8 result = decoder.readString(old);
    LOG.info("readString(" + old + ") = " + result);
    return result;
  }

  @Override
  public long skipArray() throws IOException {
    long result = decoder.skipArray();
    LOG.info("skipArray() = " + result);
    return result;
  }

  @Override
  public void skipBytes() throws IOException {
    decoder.skipBytes();
    LOG.info("skipBytes()");
  }

  @Override
  public void skipFixed(int length) throws IOException {
    decoder.skipFixed(length);
    LOG.info("skipFixed(" + length + ")");
  }

  @Override
  public long skipMap() throws IOException {
    long result = decoder.skipMap();
    LOG.info("skipMap() = " + result);
    return result;
  }

  @Override
  public void skipString() throws IOException {
    decoder.skipString();
    LOG.info("skipString()");
  }
}

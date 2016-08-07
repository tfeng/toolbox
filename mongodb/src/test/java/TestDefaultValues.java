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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.bson.BSONObject;
import org.junit.Test;

import com.mongodb.util.JSON;

import me.tfeng.toolbox.avro.AvroHelper;
import me.tfeng.toolbox.mongodb.RecordConverter;
import test.Defaults;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class TestDefaultValues {

  @Test
  public void testDefaultValues() throws Exception {
    Schema schema = Defaults.SCHEMA$;

    GenericRecordBuilder builder = new GenericRecordBuilder(schema).set("id", "1234");
    Record record1 = builder.build();

    String json = "{\"id\": \"1234\"}";
    BSONObject object = (BSONObject) JSON.parse(json);
    Record record2 = RecordConverter.toRecord(schema, object, getClass().getClassLoader());

    assertThat(record2, is(record1));
    assertThat(AvroHelper.toSimpleJson(schema, record2), is(AvroHelper.toSimpleJson(schema, record1)));

    assertEquals(record2.get("id"), "1234");
    assertNull(record2.get("s"));
    assertTrue((Boolean) record2.get("b"));
    assertEquals(((Record) record2.get("r")).get("f"), "value");
    assertEquals(((Record) record2.get("r")).get("l"), 1234l);
  }

  @Test
  public void testMissingRequiredField() throws Exception {
    Schema schema = Defaults.SCHEMA$;
    String json = "{}";
    BSONObject object = (BSONObject) JSON.parse(json);
    Record record = RecordConverter.toRecord(schema, object, getClass().getClassLoader());
    assertNull(record.get("id"));
    assertNull(record.get("s"));
    assertTrue((Boolean) record.get("b"));
    assertEquals(((Record) record.get("r")).get("f"), "value");
    assertEquals(((Record) record.get("r")).get("l"), 1234l);
  }
}

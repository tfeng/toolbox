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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class DocumentDecoder extends Decoder {

  private class MapIterator implements Iterator<Object> {

    private Entry<String, Object> currentEntry;

    private final Iterator<Entry<String, Object>> iterator;

    @SuppressWarnings("unchecked")
    public MapIterator(Object object) {
      iterator = ((Map<String, Object>) object).entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
      return currentEntry != null || iterator.hasNext();
    }

    public boolean isNextKey() {
      return currentEntry == null;
    }

    @Override
    public Object next() {
      if (currentEntry == null) {
        currentEntry = iterator.next();
        return currentEntry.getKey();
      } else {
        Object value = currentEntry.getValue();
        currentEntry = null;
        return value;
      }
    }
  }

  private class RecordIterator implements Iterator<Object> {

    private Field currentField;

    private final Iterator<Field> fieldIterator;

    private Map<String, String> fieldNameMap;

    private final Map<String, Object> map;

    @SuppressWarnings("unchecked")
    public RecordIterator(Schema schema, Object object) {
      fieldIterator = schema.getFields().iterator();
      map = MongoDbTypeConverter.convertFromMongoDbType(Map.class, object);
      initializeFieldNameMap(schema);
    }

    public Field getCurrentField() {
      return currentField;
    }

    @Override
    public boolean hasNext() {
      return fieldIterator.hasNext();
    }

    @Override
    public Object next() {
      currentField = fieldIterator.next();
      String schemaFieldName = currentField.name();
      String dbFieldName = fieldNameMap.get(schemaFieldName);
      return map.get(dbFieldName == null ? schemaFieldName : dbFieldName);
    }

    private void initializeFieldNameMap(Schema schema) {
      Class<?> recordClass = data.getClass(schema);
      if (recordClass == null) {
        LOG.warn("Unable to load class " + SpecificData.getClassName(schema) + "; skipping java annotation processing");
        fieldNameMap = Collections.emptyMap();
      } else {
        List<Field> fields = schema.getFields();
        fieldNameMap = new HashMap<>(fields.size());
        for (Field field : fields) {
          String schemaFieldName = field.name();
          String dbFieldName = RecordConverter.getFieldName(field);
          if (!schemaFieldName.equals(dbFieldName)) {
            fieldNameMap.put(schemaFieldName, dbFieldName);
          }
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DocumentDecoder.class);

  private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);

  private final SpecificData data;

  private final Stack<Iterator<Object>> iteratorStack = new Stack<>();

  private final Stack<Schema> schemaStack = new Stack<>();

  public DocumentDecoder(Class<?> recordClass, Object object) {
    try {
      data = new SpecificData(recordClass.getClassLoader());
      Schema schema = data.getSchema(recordClass);
      pushToStacks(schema, RecordConverter.convertFromSimpleRecord(schema, object));
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize decoder", e);
    }
  }

  public DocumentDecoder(Schema schema, Object object, ClassLoader classLoader) {
    try {
      data = new SpecificData(classLoader);
      pushToStacks(schema, RecordConverter.convertFromSimpleRecord(schema, object));
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize decoder", e);
    }
  }

  @Override
  public long arrayNext() throws IOException {
    try {
      if (iteratorStack.peek().hasNext()) {
        return 1;
      } else {
        popFromStacks();
        return 0;
      }
    } finally {
      finishRead();
    }
  }

  @Override
  public long mapNext() throws IOException {
    return arrayNext();
  }

  @Override
  public long readArrayStart() throws IOException {
    jumpToNextField();
    return arrayNext();
  }

  @Override
  public boolean readBoolean() throws IOException {
    jumpToNextField();
    try {
      return (Boolean) iteratorStack.peek().next();
    } finally {
      finishRead();
    }
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    jumpToNextField();
    try {
      Object next = iteratorStack.peek().next();
      if (next == null) {
        return null;
      } else if (next instanceof Binary) {
        return ByteBuffer.wrap(((Binary) next).getData());
      } else {
        return ByteBuffer.wrap((byte[]) next);
      }
    } finally {
      finishRead();
    }
  }

  @Override
  public double readDouble() throws IOException {
    jumpToNextField();
    try {
      return ((Number) iteratorStack.peek().next()).doubleValue();
    } finally {
      finishRead();
    }
  }

  @Override
  public int readEnum() throws IOException {
    jumpToNextField();
    try {
      String value = (String) iteratorStack.peek().next();
      Schema schema = schemaStack.peek();
      if (schema.getType() != Type.ENUM) {
        throw new IOException("Enum type is expected, but the current type is " + schema.getType());
      }
      int index = schema.getEnumSymbols().indexOf(value);
      if (index < 0) {
        throw new IOException(value + " is not a valid value in enum " + schema);
      }
      return index;
    } finally {
      finishRead();
    }
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    ByteBuffer buffer = readBytes(null);
    byte[] data = buffer.array();
    if (data.length != length) {
      throw new IOException("Binary data of length " + length + " is expected; actual length is " + data.length);
    }
    System.arraycopy(data, 0, bytes, start, length);
  }

  @Override
  public float readFloat() throws IOException {
    jumpToNextField();
    try {
      return ((Number) iteratorStack.peek().next()).floatValue();
    } finally {
      finishRead();
    }
  }

  @Override
  public int readIndex() throws IOException {
    jumpToNextField();
    Object value = iteratorStack.peek().next();
    Schema schema = schemaStack.peek();

    List<Schema> types = schema.getTypes();
    if (types.size() != 2 || !types.stream().anyMatch(type -> type.getType() == Type.NULL)) {
      throw new IOException("MongoDb module can only handle union of null and one other type; schema " + schema
          + " is not supported");
    }

    Schema actualSchema;
    if (value == null) {
      actualSchema = types.get(0).getType() == Type.NULL ? types.get(0) : types.get(1);
    } else {
      actualSchema = types.get(0).getType() != Type.NULL ? types.get(0) : types.get(1);
    }

    pushToStacks(actualSchema, value);

    return actualSchema == types.get(0) ? 0 : 1;
  }

  @Override
  public int readInt() throws IOException {
    jumpToNextField();
    try {
      return ((Number) iteratorStack.peek().next()).intValue();
    } finally {
      finishRead();
    }
  }

  @Override
  public long readLong() throws IOException {
    jumpToNextField();
    try {
      Object object = iteratorStack.peek().next();
      if (object instanceof Number) {
        return ((Number) object).longValue();
      } else {
        return MongoDbTypeConverter.convertFromMongoDbType(Long.class, object);
      }
    } finally {
      finishRead();
    }
  }

  @Override
  public long readMapStart() throws IOException {
    jumpToNextField();
    return mapNext();
  }

  @Override
  public void readNull() throws IOException {
    jumpToNextField();
    try {
      if (iteratorStack.peek().next() != null) {
        throw new IOException("Null value is expected");
      }
    } finally {
      finishRead();
    }
  }

  @Override
  public String readString() throws IOException {
    jumpToNextField();
    try {
      return MongoDbTypeConverter.convertFromMongoDbType(String.class, iteratorStack.peek().next());
    } finally {
      finishRead();
    }
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    String string = readString();
    return string == null ? null : old == null ? new Utf8(string) : old.set(string);
  }

  @Override
  public long skipArray() throws IOException {
    long result = readArrayStart();
    try {
      Iterator<?> iterator = iteratorStack.peek();
      while (result > 0) {
        iterator.next();
        result = arrayNext();
      }
    } finally {
      finishRead();
    }
    return 0;
  }

  @Override
  public void skipBytes() throws IOException {
    jumpToNextField();
    try {
      iteratorStack.peek().next();
    } finally {
      finishRead();
    }
  }

  @Override
  public void skipFixed(int length) throws IOException {
    jumpToNextField();
    try {
      iteratorStack.peek().next();
    } finally {
      finishRead();
    }
  }

  @Override
  public long skipMap() throws IOException {
    long result = readMapStart();
    try {
      Iterator<?> iterator = iteratorStack.peek();
      while (result > 0) {
        iterator.next();
        iterator.next();
        result = mapNext();
      }
    } finally {
      finishRead();
    }
    return 0;
  }

  @Override
  public void skipString() throws IOException {
    jumpToNextField();
    try {
      iteratorStack.peek().next();
    } finally {
      finishRead();
    }
  }

  private void finishRead() {
    while (!iteratorStack.isEmpty() && !iteratorStack.peek().hasNext()) {
      Type type = schemaStack.peek().getType();
      if (type == Type.ARRAY || type == Type.MAP) {
        break;
      }
      popFromStacks();
    }
  }

  private void jumpToNextField() throws IOException {
    Schema schema = schemaStack.peek();
    switch (schema.getType()) {
    case ARRAY: {
      Iterator<Object> iterator = iteratorStack.peek();
      Schema element = schemaStack.peek().getElementType();
      Type type = element.getType();
      if (iterator.hasNext()) {
        pushToStacks(element, iterator.next());
        if (type == Type.RECORD) {
          jumpToNextField();
        }
      } else {
        popFromStacks();
        jumpToNextField();
      }
      break;
    }
    case MAP: {
      MapIterator iterator = (MapIterator) iteratorStack.peek();
      if (iterator.isNextKey()) {
        pushToStacks(STRING_SCHEMA, iteratorStack.peek().next());
      } else {
        Schema value = schemaStack.peek().getValueType();
        Type type = value.getType();
        pushToStacks(value, iteratorStack.peek().next());
        if (type == Type.RECORD) {
          jumpToNextField();
        }
      }
      break;
    }
    case RECORD: {
      RecordIterator iterator = (RecordIterator) iteratorStack.peek();
      if (iterator.hasNext()) {
        Object value = iterator.next();
        Field field = iterator.getCurrentField();
        Type type = field.schema().getType();
        pushToStacks(field.schema(), value);
        if (type == Type.RECORD) {
          jumpToNextField();
        }
      } else {
        popFromStacks();
        jumpToNextField();
      }
      break;
    }
    default:
      break;
    }
  }

  private void popFromStacks() {
    schemaStack.pop();
    iteratorStack.pop();
  }

  @SuppressWarnings("unchecked")
  private void pushToStacks(Schema schema, Object object) throws IOException {
    switch (schema.getType()) {
    case ARRAY:
      schemaStack.push(schema);
      iteratorStack.push(((List<Object>) object).iterator());
      break;
    case MAP:
      schemaStack.push(schema);
      iteratorStack.push(new MapIterator(object));
      break;
    case RECORD:
      schemaStack.push(schema);
      iteratorStack.push(new RecordIterator(schema, object));
      break;
    case UNION:
      schemaStack.push(schema);
      iteratorStack.push(Collections.singletonList(object).iterator());
      break;
    default:
      schemaStack.push(schema);
      iteratorStack.push(Collections.singletonList(object).iterator());
      break;
    }
  }
}

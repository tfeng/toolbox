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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.bson.BSONObject;
import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.util.JSON;

import avro.shaded.com.google.common.collect.Lists;
import me.tfeng.toolbox.avro.AvroHelper;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class RecordConverter {

  public static final String MONGO_CLASS_PROPERTY = "mongo-class";

  public static final String MONGO_NAME_PROPERTY = "mongo-name";

  public static final String MONGO_TYPE_PROPERTY = "mongo-type";

  public static Object convertFromSimpleRecord(Schema schema, Object object) throws IOException {
    if (object instanceof Map && schema.getType() == Type.RECORD) {
      Map<String, Object> node = (Map<String, Object>) object;
      Document newNode = new Document();
      for (Field field : schema.getFields()) {
        String fieldName = getFieldName(field);
        if (node.containsKey(fieldName)) {
          newNode.put(fieldName, convertFromSimpleRecord(field.schema(), node.get(fieldName)));
        } else if (field.defaultValue() != null) {
          newNode.put(fieldName, convertFromSimpleRecord(field.schema(), JSON.parse(field.defaultValue().toString())));
        }else {
          newNode.put(fieldName, null);
        }
      }
      return newNode;
    } else if (object instanceof Map && schema.getType() == Type.MAP) {
      Map<String, Object> node = (Map<String, Object>) object;
      Document newNode = new Document();
      Schema valueType = schema.getValueType();
      Iterator<Entry<String, Object>> entries = node.entrySet().iterator();
      while (entries.hasNext()) {
        Entry<String, Object> entry = entries.next();
        newNode.put(entry.getKey(), convertFromSimpleRecord(valueType, entry.getValue()));
      }
      return newNode;
    } else if (schema.getType() == Type.UNION) {
      Schema type = AvroHelper.getSimpleUnionType(schema);
      return convertFromSimpleRecord(type, object);
    } else if (object instanceof Collection && schema.getType() == Type.ARRAY) {
      Collection<Object> node = (Collection<Object>) object;
      List<Object> newNode = Lists.newArrayListWithExpectedSize(node.size());
      Iterator<Object> iterator = node.iterator();
      while (iterator.hasNext()) {
        newNode.add(convertFromSimpleRecord(schema.getElementType(), iterator.next()));
      }
      return newNode;
    } else {
      return object;
    }
  }

  public static Document toDocument(IndexedRecord record) {
    Document document = new Document();
    Schema schema = record.getSchema();
    for (Field field : schema.getFields()) {
      Object value = record.get(field.pos());
      if (value != null) {
        document.put(getFieldName(field), getDocument(field.schema(), value));
      }
    }
    return document;
  }

  public static <T extends IndexedRecord> T toRecord(Class<T> recordClass, Document document) {
    SpecificDatumReader<T> reader = new SpecificDatumReader<T>(recordClass);
    try {
      Decoder decoder = new DocumentDecoder(recordClass, document);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException("Unable to convert MongoDB document " + document + " into Avro record", e);
    }
  }

  public static Record toRecord(Schema schema, BSONObject object, ClassLoader classLoader) throws IOException {
    GenericDatumReader<Record> reader = new GenericDatumReader<>(schema);
    return reader.read(null, new DocumentDecoder(schema, object, classLoader));
  }

  public static Record toRecord(Schema schema, Document document, ClassLoader classLoader) throws IOException {
    GenericDatumReader<Record> reader = new GenericDatumReader<>(schema);
    return reader.read(null, new DocumentDecoder(schema, document, classLoader));
  }

  public static <T extends IndexedRecord> T toRecord(Class<T> recordClass, BSONObject object) {
    SpecificDatumReader<T> reader = new SpecificDatumReader<T>(recordClass);
    try {
      Decoder decoder = new DocumentDecoder(recordClass, object);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException("Unable to convert MongoDB object " + object + " into Avro record", e);
    }
  }

  protected static String getFieldName(Field field) {
    String mongoName = field.getProp(MONGO_NAME_PROPERTY);
    if (mongoName != null) {
      return mongoName;
    } else {
      return field.name();
    }
  }

  @SuppressWarnings("unchecked")
  private static Object getDocument(Schema schema, Object object) {
    if (schema.getType() == Type.UNION) {
      List<Schema> types = schema.getTypes();
      if (types.size() != 2 && types.get(0).getType() != Type.NULL && types.get(1).getType() != Type.NULL) {
        throw new RuntimeException("In a union type, only null unioned with exactly one other type is supported: "
                + schema);
      }
      if (types.get(0).getType() == Type.NULL) {
        return getDocument(types.get(1), object);
      } else {
        return getDocument(types.get(0), object);
      }
    } else if (object == null) {
      return null;
    } else if (object instanceof IndexedRecord) {
      return toDocument((IndexedRecord) object);
    } else if (object instanceof Collection) {
      return getDocuments(schema, (Collection<Object>) object);
    } else if (object instanceof Map) {
      return getDocuments(schema, (Map<String, Object>) object);
    } else if (object instanceof ByteBuffer) {
      return new Binary(((ByteBuffer) object).array());
    } else if (object.getClass().isEnum()) {
      return ((Enum<?>) object).name();
    } else {
      String mongoClassName = schema.getProp(MONGO_CLASS_PROPERTY);
      String mongoType = schema.getProp(MONGO_TYPE_PROPERTY);
      if (object instanceof CharSequence) {
        object = object.toString();
      }
      if (mongoClassName == null && mongoType == null) {
        return object;
      } else if (mongoClassName != null && mongoType != null) {
        throw new RuntimeException("mongo-class and mongo-type should not be both specified: " + schema);
      } else {
        try {
          Class<?> mongoClass;
          if (mongoClassName != null) {
            mongoClass = schema.getClass().getClassLoader().loadClass(mongoClassName);
          } else {
            mongoClass = MongoType.valueOf(mongoType).getMongoClass();
          }
          if (object instanceof String && mongoClass.isAssignableFrom(Object.class)) {
            return JSON.parse((String) object);
          } else {
            return MongoDbTypeConverter.convertToMongoDbType(mongoClass, object);
          }
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Unable to load mongo-class " + mongoClassName, e);
        }
      }
    }
  }

  private static List<Object> getDocuments(Schema schema, Collection<Object> collection) {
    return collection.stream().map(object -> getDocument(schema.getElementType(), object)).collect(Collectors.toList());
  }

  private static Map<String, Object> getDocuments(Schema schema, Map<String, Object> map) {
    Map<String, Object> newMap = new HashMap<>(map.size());
    map.entrySet().forEach(entry -> newMap.put(entry.getKey(), getDocument(schema.getValueType(), entry.getValue())));
    return newMap;
  }
}

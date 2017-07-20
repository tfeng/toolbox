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

package me.tfeng.toolbox.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.ArrayUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import me.tfeng.toolbox.common.Constants;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class AvroHelper {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static JsonNode convertFromSimpleRecord(Schema schema, JsonNode json) throws IOException {
    return convertFromSimpleRecord(schema, json, new JsonNodeFactory(false));
  }

  public static String convertFromSimpleRecord(Schema schema, String json) throws IOException {
    if (json.isEmpty()) {
      return json;
    }
    JsonNode node = MAPPER.readTree(json);
    node = convertFromSimpleRecord(schema, node);
    return node.toString();
  }

  public static JsonNode convertToSimpleRecord(Schema schema, JsonNode json) throws IOException {
    return convertToSimpleRecord(schema, json, new JsonNodeFactory(false));
  }

  public static String convertToSimpleRecord(Schema schema, String json) throws IOException {
    if (json.isEmpty()) {
      return json;
    }
    JsonNode node = MAPPER.readTree(json);
    node = convertToSimpleRecord(schema, node);
    return node.toString();
  }

  public static Object createGenericRequestFromRecord(Schema requestSchema, JsonNode record) throws IOException {
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(requestSchema, record.toString());
    return new GenericDatumReader<>(requestSchema, requestSchema).read(null, jsonDecoder);
  }

  public static <T> T createSpecificRequestFromRecord(Class<T> requestClass, JsonNode record) throws IOException {
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(getSchema(requestClass), record.toString());
    return new SpecificDatumReader<>(requestClass).read(null, jsonDecoder);
  }

  public static <T> T decodeRecord(Class<T> recordClass, byte[] data) throws IOException {
    DecoderFactory decoderFactory = DecoderFactory.get();
    BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(data, null);
    SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(getSchema(recordClass));
    return datumReader.read(null, binaryDecoder);
  }

  public static byte[] encodeRecord(IndexedRecord record) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      EncoderFactory encoderFactory = EncoderFactory.get();
      BinaryEncoder binaryEncoder = encoderFactory.binaryEncoder(stream, null);
      SpecificDatumWriter<IndexedRecord> datumWriter = new SpecificDatumWriter<>(record.getSchema());
      datumWriter.write(record, binaryEncoder);
      binaryEncoder.flush();
    } finally {
      stream.close();
    }
    return stream.toByteArray();
  }

  public static Protocol getProtocol(Class<?> interfaceClass) {
    return new SpecificData(interfaceClass.getClassLoader()).getProtocol(interfaceClass);
  }

  public static Schema getSchema(Class<?> schemaClass) {
    return new SpecificData(schemaClass.getClassLoader()).getSchema(schemaClass);
  }

  public static Schema getSimpleUnionType(Schema union) throws IOException {
    if (union.getType() != Type.UNION) {
      throw new IOException("Schema is not a union type: " + union);
    }
    List<Schema> types = union.getTypes();
    if (types.size() == 2) {
      if (types.get(0).getType() == Type.NULL && types.get(1).getType() != Type.NULL) {
        return types.get(1);
      } else if (types.get(1).getType() == Type.NULL && types.get(0).getType() != Type.NULL) {
        return types.get(0);
      }
    }
    return null;
  }

  public static JsonNode parseSimpleJson(Schema schema, byte[] data) throws IOException {
    if (ArrayUtils.isEmpty(data)) {
      // The method takes no argument; use empty data.
      data = "{}".getBytes(Constants.UTF8);
    }
    JsonNode node = MAPPER.readTree(data);
    return convertFromSimpleRecord(schema, node);
  }

  public static String toJson(IndexedRecord record) throws IOException {
    Schema schema = record.getSchema();
    return toJson(schema, record);
  }

  public static String toJson(Schema schema, Object object) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SpecificDatumWriter<Object> writer = new SpecificDatumWriter<>(schema);
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
    writer.write(object, encoder);
    encoder.flush();
    return outputStream.toString();
  }

  public static <T> T toRecord(Class<T> recordClass, String json) throws IOException {
    Schema schema = getSchema(recordClass);
    json = convertFromSimpleRecord(schema, json);
    SpecificDatumReader<T> reader = new SpecificDatumReader<>(recordClass);
    return reader.read(null, DecoderFactory.get().jsonDecoder(schema, json));
  }

  public static <T> T toRecord(Schema schema, String json) throws IOException {
    json = convertFromSimpleRecord(schema, json);
    SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
    return reader.read(null, DecoderFactory.get().jsonDecoder(schema, json));
  }

  public static String toSimpleJson(IndexedRecord record) throws IOException {
    Schema schema = record.getSchema();
    return toSimpleJson(schema, record);
  }

  public static String toSimpleJson(Schema schema, Object object) throws IOException {
    return convertToSimpleRecord(schema, toJson(schema, object));
  }

  private static JsonNode convertFromSimpleRecord(Schema schema, JsonNode json, JsonNodeFactory factory)
      throws IOException {
    if (json.isObject() && schema.getType() == Type.RECORD) {
      ObjectNode node = (ObjectNode) json;
      ObjectNode newNode = factory.objectNode();
      for (Field field : schema.getFields()) {
        String fieldName = field.name();
        if (node.has(fieldName)) {
          newNode.set(fieldName, convertFromSimpleRecord(field.schema(), node.get(fieldName), factory));
        } else if (field.defaultValue() != null) {
          newNode.set(fieldName, MAPPER.readTree(field.defaultValue().toString()));
        } else {
          newNode.set(fieldName, factory.nullNode());
        }
      }
      return newNode;
    } else if (json.isObject() && schema.getType() == Type.MAP) {
      ObjectNode node = (ObjectNode) json;
      ObjectNode newNode = factory.objectNode();
      Schema valueType = schema.getValueType();
      Iterator<Entry<String, JsonNode>> entries = node.fields();
      while (entries.hasNext()) {
        Entry<String, JsonNode> entry = entries.next();
        newNode.set(entry.getKey(), convertFromSimpleRecord(valueType, entry.getValue(), factory));
      }
      return newNode;
    } else if (schema.getType() == Type.UNION) {
      Schema type = getSimpleUnionType(schema);
      if (type == null) {
        if (json.isNull()) {
          return json;
        } else {
          ObjectNode node = (ObjectNode) json;
          Entry<String, JsonNode> entry = node.fields().next();
          for (Schema unionType : schema.getTypes()) {
            if (unionType.getFullName().equals(entry.getKey())) {
              ObjectNode newNode = factory.objectNode();
              newNode.set(entry.getKey(), convertFromSimpleRecord(unionType, entry.getValue(), factory));
              return newNode;
            }
          }
          throw new IOException("Unable to get schema for type " + entry.getKey() + " in union");
        }
      } else if (json.isNull()) {
        return json;
      } else {
        ObjectNode newNode = factory.objectNode();
        newNode.set(type.getFullName(), convertFromSimpleRecord(type, json, factory));
        return newNode;
      }
    } else if (json.isArray() && schema.getType() == Type.ARRAY) {
      ArrayNode node = (ArrayNode) json;
      ArrayNode newNode = factory.arrayNode();
      Iterator<JsonNode> iterator = node.elements();
      while (iterator.hasNext()) {
        newNode.add(convertFromSimpleRecord(schema.getElementType(), iterator.next(), factory));
      }
      return newNode;
    } else {
      return json;
    }
  }

  private static JsonNode convertToSimpleRecord(Schema schema, JsonNode json, JsonNodeFactory factory)
      throws IOException {
    if (json.isObject() && schema.getType() == Type.RECORD) {
      ObjectNode node = (ObjectNode) json;
      ObjectNode newNode = factory.objectNode();
      for (Field field : schema.getFields()) {
        String fieldName = field.name();
        if (node.has(fieldName)) {
          JsonNode value = convertToSimpleRecord(field.schema(), node.get(fieldName), factory);
          if (!value.isNull()) {
            newNode.set(fieldName, value);
          }
        }
      }
      return newNode;
    } else if (json.isObject() && schema.getType() == Type.MAP) {
      ObjectNode node = (ObjectNode) json;
      ObjectNode newNode = factory.objectNode();
      Schema valueType = schema.getValueType();
      Iterator<Entry<String, JsonNode>> entries = node.fields();
      while (entries.hasNext()) {
        Entry<String, JsonNode> entry = entries.next();
        JsonNode value = convertToSimpleRecord(valueType, entry.getValue(), factory);
        if (value.isNull()) {
          newNode.set(entry.getKey(), value);
        }
      }
      return newNode;
    } else if (schema.getType() == Type.UNION) {
      Schema type = getSimpleUnionType(schema);
      if (type == null) {
        if (json.isNull()) {
          return json;
        } else {
          ObjectNode node = (ObjectNode) json;
          Entry<String, JsonNode> entry = node.fields().next();
          for (Schema unionType : schema.getTypes()) {
            if (unionType.getFullName().equals(entry.getKey())) {
              ObjectNode newNode = factory.objectNode();
              newNode.set(entry.getKey(), convertToSimpleRecord(unionType, entry.getValue(), factory));
              return newNode;
            }
          }
          throw new IOException("Unable to get schema for type " + entry.getKey() + " in union");
        }
      } else if (json.isNull()) {
        return json;
      } else {
        return convertToSimpleRecord(type, json.get(type.getFullName()), factory);
      }
    } else if (json.isArray() && schema.getType() == Type.ARRAY) {
      ArrayNode node = (ArrayNode) json;
      ArrayNode newNode = factory.arrayNode();
      Iterator<JsonNode> iterator = node.elements();
      while (iterator.hasNext()) {
        newNode.add(convertToSimpleRecord(schema.getElementType(), iterator.next(), factory));
      }
      return newNode;
    } else {
      return json;
    }
  }
}

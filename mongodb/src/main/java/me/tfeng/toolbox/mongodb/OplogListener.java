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

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;

import me.tfeng.toolbox.spring.Startable;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class OplogListener implements Startable {

  public static final String COLLECTION_NAME = "oplog.rs";

  public static final String DB_NAME = "local";

  private static final Logger LOG = LoggerFactory.getLogger(OplogListener.class);

  private MongoCollection<Document> collection;

  private OplogItemHandler handler;

  private MongoClient mongoClient;

  private String namespace;

  private BsonTimestamp startTimestamp;

  @Override
  public void onStart() throws Throwable {
    if (mongoClient == null || handler == null) {
      throw new Exception("mongoClient and handler must both be provided");
    }

    LOG.info("Connecting to " + DB_NAME + "." + COLLECTION_NAME + " in MongoDB");
    collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
    collection.find(getQuery()).sort(getSort()).cursorType(getCursorType())
        .forEach(this::process, (result, throwable) -> {
          if (throwable != null) {
            LOG.info("Oplog listener existed with exception", throwable);
          }
        });
  }

  @Override
  public void onStop() throws Throwable {
  }

  public void setHandler(OplogItemHandler handler) {
    this.handler = handler;
  }

  public void setMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public void setStartTimestamp(BsonTimestamp startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  protected CursorType getCursorType() {
    return CursorType.TailableAwait;
  }

  protected Document getQuery() {
    Document query = new Document();
    if (startTimestamp != null) {
      query.put("ts", new Document("$gt", startTimestamp));
    }
    if (namespace != null) {
      query.put("ns", namespace);
    }
    return query;
  }

  protected Document getSort() {
    return new Document("$natural", 1);
  }

  protected void process(Document document) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received oplog item " + document);
    }

    OplogItem oplogItem = RecordConverter.toRecord(OplogItem.class, document);
    handler.handle(oplogItem);
  }
}

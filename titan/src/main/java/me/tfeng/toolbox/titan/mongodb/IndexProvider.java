/**
 * Copyright 2015 Thomas Feng
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

package me.tfeng.toolbox.titan.mongodb;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransaction;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfigurable;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexFeatures;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation.IndexRetriever;
import com.thinkaurelius.titan.diskstorage.indexing.RawQuery;
import com.thinkaurelius.titan.diskstorage.indexing.RawQuery.Result;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class IndexProvider implements com.thinkaurelius.titan.diskstorage.indexing.IndexProvider {

  private static final String COLLECTION_POSTFIX = "_index_";

  private static final IndexFeatures FEATURES = new IndexFeatures.Builder()
      .supportedStringMappings(Mapping.TEXT, Mapping.STRING)
      .supportsCardinality(Cardinality.SINGLE)
      .build();

  private static final Logger LOG = LoggerFactory.getLogger(IndexProvider.class);

  private final String dbName;

  private final MongoClient mongoClient;

  private final MongoDatabase mongoDb;

  private volatile Map<String, IndexStore> stores = new ConcurrentHashMap<>();

  public IndexProvider(Configuration configuration) {
    String factoryName = configuration.get(Configs.FACTORY_NAME);
    dbName = configuration.get(Configs.DB_NAME);

    TitanGraphFactory factory = TitanGraphFactory.get(factoryName);
    mongoClient = factory.getMongoClient();
    mongoDb = mongoClient.getDatabase(dbName);
  }

  @Override
  public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
    return new AbstractStoreTransaction(config) {
    };
  }

  @Override
  public void clearStorage() throws BackendException {
    stores.values().forEach(store -> {
      try {
        store.clear();
      } catch (Throwable e) {
        LOG.error("Unable to clear store " + store.getName() + " in store manager " + getName(), e);
      }
    });
  }

  @Override
  public void close() throws BackendException {
    stores = new ConcurrentHashMap<>();
  }

  @Override
  public IndexFeatures getFeatures() {
    return FEATURES;
  }

  public String getName() {
    return "mongo:" + mongoClient.getAddress() + ":" + dbName;
  }

  @Override
  public String mapKey2Field(String key, KeyInformation information) {
    return key;
  }

  @Override
  public void mutate(Map<String, Map<String, IndexMutation>> mutations, IndexRetriever informations, BaseTransaction tx)
      throws BackendException {
    for (Entry<String, Map<String, IndexMutation>> entry : mutations.entrySet()) {
      getStore(entry.getKey()).mutate(entry.getValue());
    }
  }

  @Override
  public List<String> query(IndexQuery query, IndexRetriever informations, BaseTransaction tx)
      throws BackendException {
    return getStore(query.getStore()).query(query);
  }

  @Override
  public Iterable<Result<String>> query(RawQuery query, IndexRetriever informations, BaseTransaction tx)
      throws BackendException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void register(String store, String key, KeyInformation information, BaseTransaction tx)
      throws BackendException {
    getStore(store).register(key, information);
  }

  @Override
  public void restore(Map<String, Map<String, List<IndexEntry>>> documents, IndexRetriever informations,
      BaseTransaction tx) throws BackendException {
    for (Entry<String, Map<String, List<IndexEntry>>> entry : documents.entrySet()) {
      getStore(entry.getKey()).restore(entry.getValue());
    }
  }

  @Override
  public boolean supports(KeyInformation information, TitanPredicate titanPredicate) {
    if (information.getCardinality()!= Cardinality.SINGLE) {
      return false;
    }

    Class<?> dataType = information.getDataType();
    Mapping mapping = Mapping.getMapping(information);
    if (mapping != Mapping.DEFAULT && !AttributeUtil.isString(dataType)) {
      return false;
    }

    if (Number.class.isAssignableFrom(dataType)) {
      return titanPredicate instanceof Cmp;
    } else if (dataType == Geoshape.class) {
      return titanPredicate instanceof Geo;
    } else if (AttributeUtil.isString(dataType)) {
      return (mapping == Mapping.DEFAULT || mapping == Mapping.STRING)
          && (titanPredicate instanceof Cmp || titanPredicate==Text.PREFIX || titanPredicate==Text.REGEX);
    } else if (dataType == Date.class || dataType == Instant.class) {
      return titanPredicate instanceof Cmp;
    } else if (dataType == Boolean.class) {
      return titanPredicate == Cmp.EQUAL || titanPredicate == Cmp.NOT_EQUAL;
    } else if (dataType == UUID.class) {
      return titanPredicate == Cmp.EQUAL || titanPredicate == Cmp.NOT_EQUAL;
    } else {
      return false;
    }
  }

  @Override
  public boolean supports(KeyInformation information) {
    if (information.getCardinality() != Cardinality.SINGLE) {
      return false;
    }

    Class<?> dataType = information.getDataType();
    Mapping mapping = Mapping.getMapping(information);
    if (Number.class.isAssignableFrom(dataType)
        || Geoshape.class.isAssignableFrom(dataType)
        || Date.class.isAssignableFrom(dataType)
        || Instant.class.isAssignableFrom(dataType)
        || Boolean.class.isAssignableFrom(dataType)
        || UUID.class.isAssignableFrom(dataType)) {
      return mapping == Mapping.DEFAULT;
    } else if (AttributeUtil.isString(dataType)) {
      return mapping == Mapping.DEFAULT || mapping == Mapping.STRING;
    } else {
      return false;
    }
  }

  private IndexStore getStore(String store) {
    String storeName = store + COLLECTION_POSTFIX;
    IndexStore indexStore = stores.get(storeName);
    if (indexStore == null) {
      indexStore = new IndexStore(mongoDb, storeName);
      stores.put(storeName, indexStore);
    }
    return indexStore;
  }
}

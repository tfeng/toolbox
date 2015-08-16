/**
 * Copyright 2015 Thomas Feng
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package me.tfeng.toolbox.titan.mongodb;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;


/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class StoreManager implements OrderedKeyValueStoreManager {

  private static final StoreFeatures FEATURES = new StandardStoreFeatures.Builder()
      .orderedScan(true)
      .keyOrdered(true)
      .persists(true)
      .distributed(true)
      .batchMutation(true)
      .multiQuery(true)
      .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
      .build();

  private static final Logger LOG = LoggerFactory.getLogger(StoreManager.class);

  private final String dbName;

  private final MongoClient mongoClient;

  private final MongoDatabase mongoDb;

  private volatile Map<String, KeyValueStore> stores = new ConcurrentHashMap<>();

  public StoreManager(Configuration configuration) {
    String factoryName = configuration.get(Configs.FACTORY_NAME);
    dbName = configuration.get(Configs.DB_NAME);

    TitanGraphFactory factory = TitanGraphFactory.get(factoryName);
    mongoClient = factory.getMongoClient();
    mongoDb = mongoClient.getDatabase(dbName);
  }

  @Override
  public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
    return new AbstractStoreTransaction(config) {};
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
    Map<String, KeyValueStore> oldStores = stores;
    stores = new ConcurrentHashMap<>();
    oldStores.values().stream().forEach(store -> {
      try {
        store.close();
      } catch (Throwable e) {
        LOG.error("Unable to close store " + store.getName() + " in store manager " + getName(), e);
      }
    });
  }

  @Override
  public StoreFeatures getFeatures() {
    return FEATURES;
  }

  @Override
  public List<KeyRange> getLocalKeyPartition() throws BackendException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return "mongo:" + mongoClient.getAddress() + ":" + dbName;
  }

  @Override
  public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
    for (Entry<String, KVMutation> entry : mutations.entrySet()) {
      openDatabase(entry.getKey()).mutate(entry.getValue(), txh);
    }
  }

  @Override
  public KeyValueStore openDatabase(String name) throws BackendException {
    KeyValueStore store = stores.get(name);
    if (store == null) {
      store = new KeyValueStore(mongoDb, name);
      stores.put(name, store);
    }
    return store;
  }
}

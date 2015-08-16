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

import java.util.Map;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;

import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanFactory.Builder;
import com.thinkaurelius.titan.core.TitanGraph;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class TitanGraphFactory implements FactoryBean, InitializingBean {

  public static final String DEFAULT_FACTORY_NAME = "default";

  private static final Map<String, TitanGraphFactory> FACTORIES = Maps.newConcurrentMap();

  static {
    Configs.MONGO_NS.toString();
  }

  public static TitanGraphFactory get(String name) {
    return FACTORIES.get(name);
  }

  private String dbName;

  private MongoClient mongoClient;

  private String name = DEFAULT_FACTORY_NAME;

  private Map<String, Object> properties;

  @Override
  public void afterPropertiesSet() throws Exception {
    FACTORIES.put(name, this);
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }

  public String getName() {
    return name;
  }

  @Override
  public Object getObject() throws Exception {
    Builder builder = TitanFactory.build()
        .set("storage.backend", StoreManager.class.getName())
        .set("storage.mongodb.factory-name", name)
        .set("storage.mongodb.db-name", dbName)
        .set("index.search.backend", IndexProvider.class.getName());
    if (properties != null) {
      properties.entrySet().forEach(entry -> builder.set(entry.getKey(), entry.getValue()));
    }
    return builder.open();
  }

  @Override
  public Class<?> getObjectType() {
    return TitanGraph.class;
  }

  @Override
  public boolean isSingleton() {
    return false;
  }

  @Required
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  @Required
  public void setMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }
}

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

import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption.Type;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class Configs {

  public static final ConfigNamespace MONGO_NS = new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "mongodb",
      "Titan MongoDB options");

  public static final ConfigOption<String> FACTORY_NAME = new ConfigOption<>(MONGO_NS, "factory-name",
      "Titan graph factory name", Type.LOCAL, String.class);

  public static final ConfigOption<String> DB_NAME = new ConfigOption<>(MONGO_NS, "db-name", "MongoDB database name",
      Type.LOCAL, String.class);
}

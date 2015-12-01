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

package me.tfeng.toolbox.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class KafkaUtils {

  public static void createTopic(String zkServers, int sessionTimeout, int connectionTimeout, String topic,
      int partitions, int replicationFactor) {
    ZkClient zkClient = new ZkClient(zkServers, sessionTimeout, connectionTimeout, new ZkStringSerializer());
    ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
    AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, new Properties());
  }

  public static void createTopic(String zkServers, String topic, int partitions, int replicationFactor) {
    createTopic(zkServers, Integer.MAX_VALUE, Integer.MAX_VALUE, topic, partitions, replicationFactor);
  }
}

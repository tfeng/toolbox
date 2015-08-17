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

package me.tfeng.toolbox.dust;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Queues;

import me.tfeng.toolbox.spring.Startable;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class JsEnginePool extends JsEngineConfig implements Startable {

  private EngineType engineType = EngineType.NASHORN;

  private volatile ConcurrentLinkedQueue<JsEngine> engines;

  private int size = 5;

  public EngineType getEngineType() {
    return engineType;
  }

  public int getSize() {
    return size;
  }

  @Override
  public void onStart() throws Throwable {
    ConcurrentLinkedQueue engines = Queues.newConcurrentLinkedQueue();
    for (int i = 0; i < size; i++) {
      engines.offer(createEngine());
    }
    this.engines = engines;
  }

  @Override
  public void onStop() throws Throwable {
    engines.forEach(JsEngine::destroy);
  }

  public String render(String template, JsonNode data) throws Exception {
    JsEngine engine = engines.poll();
    try {
      return engine.render(template, data);
    } finally {
      engines.offer(engine);
    }
  }

  public void setEngineType(EngineType engineType) {
    this.engineType = engineType;
  }

  public void setSize(int size) {
    this.size = size;
  }

  private JsEngine createEngine() throws Exception {
    JsEngine engine;
    switch (engineType) {
    case NASHORN:
      engine = new NashornEngine(this);
      break;
    case NODE:
      engine = new NodeEngine(this);
      break;
    default:
      throw new RuntimeException("Unknown engine type " + engineType);
    }

    engine.initialize();
    return engine;
  }
}

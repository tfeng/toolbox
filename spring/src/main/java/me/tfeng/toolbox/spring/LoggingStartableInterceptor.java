/**
 * Copyright 2016 Thomas Feng
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

package me.tfeng.toolbox.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class LoggingStartableInterceptor implements StartableInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingStartableInterceptor.class);

  private BeanNameRegistry beanRegistry = new BeanNameRegistry();

  @Override
  public void beginStart(Startable startable) {
    LOG.info("Starting " + getName(startable));
  }

  @Override
  public void beginStop(Startable startable) {
    LOG.info("Stopping " + getName(startable));
  }

  @Override
  public void endStart(Startable startable) {
    LOG.info("Started " + getName(startable));
  }

  @Override
  public void endStop(Startable startable) {
    LOG.info("Stopped " + getName(startable));
  }

  @Override
  public void failStart(Startable startable, Throwable t) {
    throw new RuntimeException("Unable to start " + getName(startable), t);
  }

  @Override
  public void failStop(Startable startable, Throwable t) {
    LOG.error("Unable to stop " + getName(startable), t);
  }

  private String getName(Startable startable) {
    String name = beanRegistry.getName(startable);
    return name == null ? startable.toString() : name;
  }
}

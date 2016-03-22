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

import java.util.Arrays;
import java.util.Map;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import com.google.common.collect.Maps;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class BeanNameRegistry implements ApplicationListener<ContextRefreshedEvent> {

  private static class ShadowKey {

    private int hashCode;

    private Object object;

    ShadowKey(Object object) {
      this.object = object;
      hashCode = System.identityHashCode(object);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof ShadowKey && this.object == ((ShadowKey) object).object;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private final Map<Object, String> beanMap = Maps.newHashMap();

  public boolean exists(Object bean) {
    return beanMap.containsKey(new ShadowKey(bean));
  }

  public String getName(Object bean) {
    return beanMap.get(new ShadowKey(bean));
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    ApplicationContext applicationContext = event.getApplicationContext();
    Arrays.stream(applicationContext.getBeanDefinitionNames()).forEach(name -> {
      try {
        beanMap.put(new ShadowKey(applicationContext.getBean(name)), name);
      } catch (NoSuchBeanDefinitionException e) {
        // Ignore.
      }
    });
  }
}

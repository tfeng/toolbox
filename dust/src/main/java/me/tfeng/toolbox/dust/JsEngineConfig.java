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

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Required;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class JsEngineConfig {

  private AssetLocator assetLocator = new AssetLocator();

  private String nodePath;

  private String templatePath;

  public void copy(JsEngineConfig config) {
    BeanUtils.copyProperties(config, this);
  }

  public AssetLocator getAssetLocator() {
    return assetLocator;
  }

  public String getNodePath() {
    return nodePath;
  }

  public String getTemplatePath() {
    return templatePath;
  }

  public void setAssetLocator(AssetLocator assetLocator) {
    this.assetLocator = assetLocator;
  }

  public void setNodePath(String nodePath) {
    this.nodePath = nodePath;
  }

  @Required
  public void setTemplatePath(String templatePath) {
    this.templatePath = templatePath;
  }
}

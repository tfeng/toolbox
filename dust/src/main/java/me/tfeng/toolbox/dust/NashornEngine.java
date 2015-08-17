/**
 * Copyright 2015 Thomas Feng
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package me.tfeng.toolbox.dust;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

import me.tfeng.toolbox.common.Constants;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class NashornEngine extends JsEngineConfig implements JsEngine {

  private static final Logger LOG = LoggerFactory.getLogger(NashornEngine.class);

  private static final String RENDER_SCRIPT =
      "dust.render(name, JSON.parse(json), function(err, data) {"
          + "if (err) throw new Error(err); else writer.write(data, 0, data.length); })";

  private ScriptEngine scriptEngine;

  public NashornEngine() {
  }

  public NashornEngine(JsEngineConfig config) {
    copy(config);
  }

  @Override
  public void destroy() {
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.NASHORN;
  }

  @Override
  public void initialize() throws Exception {
    scriptEngine = new ScriptEngineManager(null).getEngineByName("nashorn");
    InputStream dustJsStream = getAssetLocator().getResource(DUST_JS_NAME);
    String dustJs = readAndClose(dustJsStream);
    scriptEngine.eval(dustJs);
  }

  @Override
  public String render(String template, JsonNode data) throws Exception {
    boolean isRegistered = (Boolean) evaluate("dust.cache[template] !== undefined",
        ImmutableMap.of("template", template));

    if (!isRegistered) {
      String jsFileName = new File(getTemplatePath(), template + ".js").toString();
      LOG.info("Loading template " + jsFileName);
      InputStream jsStream = getAssetLocator().getResource(jsFileName);
      String compiledTemplate = readAndClose(jsStream);
      evaluate("dust.loadSource(source)", ImmutableMap.of("source", compiledTemplate));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Rendering template " + template);
    }

    String json = OBJECT_MAPPER.writeValueAsString(data);
    StringWriter writer = new StringWriter();
    evaluate(RENDER_SCRIPT, ImmutableMap.of("name", template, "json", json, "writer", writer));
    return writer.toString();
  }

  private Object evaluate(String script, Map<String, Object> data) throws ScriptException {
    Bindings bindings = new SimpleBindings(data);
    scriptEngine.getContext().setBindings(bindings, ScriptContext.GLOBAL_SCOPE);
    return scriptEngine.eval(script);
  }

  private String readAndClose(InputStream stream) throws IOException {
    try {
      return CharStreams.toString(new InputStreamReader(stream, Constants.UTF8));
    } finally {
      stream.close();
    }
  }
}

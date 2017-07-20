/**
 * Copyright 2016 Thomas Feng
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class NodeEngine extends JsEngineConfig implements JsEngine {

  private static final Logger LOG = LoggerFactory.getLogger(NashornEngine.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Process process;

  public NodeEngine() {
  }

  public NodeEngine(JsEngineConfig config) {
    copy(config);
  }

  @Override
  public void destroy() {
    process.destroy();
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.NODE;
  }

  @Override
  public void initialize() throws Exception {
    process = new ProcessBuilder(getNodePath(), "-i").start();
    executeCommand(process, getAssetLocator().getResource(DUST_JS_NAME));
  }

  @Override
  public String render(String template, JsonNode data) throws Exception {
    boolean isRegistered = isRegistered(process, template);

    if (!isRegistered) {
      String jsFileName = new File(getTemplatePath(), template + ".js").toString();
      LOG.info("Loading template " + jsFileName);
      InputStream jsStream = getAssetLocator().getResource(jsFileName);
      executeCommand(process, jsStream);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Rendering template " + template);
    }

    String json = OBJECT_MAPPER.writeValueAsString(data);
    return evaluate(process, template, json).textValue();
  }

  private JsonNode evaluate(Process process, String template, String data) throws IOException {
    PrintWriter writer = new PrintWriter(process.getOutputStream());
    writer.println("[");
    printRenderScript(writer, template, data);
    writer.println("]");
    writer.println("1");
    writer.flush();

    String result = readInput(process, "1");
    JsonParser parser = new JsonFactory().createParser(result).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    ArrayNode arrayNode = OBJECT_MAPPER.readValue(parser, ArrayNode.class);
    return arrayNode.get(0);
  }

  private void executeCommand(Process process, InputStream inputStream) throws IOException {
    BufferedReader inputReader = new BufferedReader(new InputStreamReader(inputStream));
    PrintWriter writer = new PrintWriter(process.getOutputStream());
    String line = inputReader.readLine();
    while (line != null) {
      writer.println(line);
      line = inputReader.readLine();
    }
    writer.println("1");
    writer.flush();
    readInput(process, "1");
  }

  private String getValue(String nodeOutputLine) throws IOException {
    if (nodeOutputLine.startsWith("> ")) {
      nodeOutputLine = nodeOutputLine.substring(2);
    }
    while (nodeOutputLine.startsWith("... ")) {
      nodeOutputLine = nodeOutputLine.substring(4);
    }
    return nodeOutputLine;
  }

  private boolean isRegistered(Process process, String template) throws IOException {
    PrintWriter writer = new PrintWriter(process.getOutputStream());
    writer.println("[");
    printIsRegisteredExpression(writer, template);
    writer.println("]");
    writer.println("1");
    writer.flush();

    String result = readInput(process, "1");
    JsonParser parser = new JsonFactory().createParser(result).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    ArrayNode arrayNode = OBJECT_MAPPER.readValue(parser, ArrayNode.class);
    return arrayNode.get(0).booleanValue();
  }

  private void printIsRegisteredExpression(PrintWriter writer, String template) {
    writer.print("dust.cache['");
    writer.print(StringEscapeUtils.escapeJson(template));
    writer.print("'] !== undefined");
  }

  private void printRenderScript(PrintWriter writer, String template, String json) {
    writer.print("(function(){var v; dust.render('");
    writer.print(StringEscapeUtils.escapeJson(template));
    writer.print("', JSON.parse('");
    writer.print(StringEscapeUtils.escapeJson(json));
    writer.print("'), function(err, data) {v = data;}); return v})()");
  }

  private String readInput(Process process, String endMark) throws IOException {
    StringBuffer result = new StringBuffer();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line = getValue(reader.readLine());
    while (!endMark.equals(line)) {
      result.append(line);
      result.append('\n');
      line = getValue(reader.readLine());
    }
    return result.toString();
  }
}

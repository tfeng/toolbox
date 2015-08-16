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

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Geoshape.Point;
import com.thinkaurelius.titan.core.attribute.Geoshape.Type;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.Not;
import com.thinkaurelius.titan.graphdb.query.condition.Or;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class IndexStore {

  private static final String ID_KEY = "_id";

  private static final Logger LOG = LoggerFactory.getLogger(IndexStore.class);

  private final MongoCollection<Document> collection;

  private final String name;

  public IndexStore(MongoDatabase mongoDb, String name) {
    this.name = name;
    collection = mongoDb.getCollection(name);
  }

  public void clear() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(name + ": clearing");
    }

    collection.drop();
  }

  public String getName() {
    return name;
  }

  public void mutate(Map<String, IndexMutation> mutations) throws BackendException {
    List<WriteModel<Document>> bulkWriteModel = Lists.newArrayListWithCapacity(mutations.size());
    for (Entry<String, IndexMutation> entry : mutations.entrySet()) {
      String id = entry.getKey();
      IndexMutation mutation = entry.getValue();
      if (mutation.isDeleted()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(name + ": deleting index - " + id);
        }
        bulkWriteModel.add(new DeleteOneModel<>(Filters.eq(ID_KEY, id)));
      } else {
        Map<String, String> deletions = mutation.getDeletions().stream()
            .collect(Collectors.toMap(deletion -> deletion.field, deletion -> ""));
        Map<String, Object> additions = mutation.getAdditions().stream()
            .filter(addition -> !deletions.containsKey(addition.field))
            .collect(Collectors.toMap(addition -> addition.field, addition -> convertValue(addition.value)));
        Bson filter = Filters.eq(ID_KEY, id);
        Document update = null;
        if (!deletions.isEmpty() && !additions.isEmpty()) {
          update = new Document(ImmutableMap.of("$unset", deletions, "$set", additions));
        } else if (!deletions.isEmpty()) {
          update = new Document(ImmutableMap.of("$unset", deletions));
        } else if (!additions.isEmpty()) {
          update = new Document(ImmutableMap.of("$set", additions));
        }
        if (update != null) {
          bulkWriteModel.add(new UpdateManyModel<>(filter, update, new UpdateOptions().upsert(true)));
        }
      }
    }
    collection.bulkWrite(bulkWriteModel, new BulkWriteOptions().ordered(false));
  }

  public List<String> query(IndexQuery query) throws BackendException {
    Bson filter = convertQuery(query.getCondition());
    return StreamSupport.stream(collection.find(filter).spliterator(), false)
        .map(document -> document.getString(ID_KEY))
        .collect(Collectors.toList());
  }

  public void register(String key, KeyInformation information) throws BackendException {
    Class<?> dataType = information.getDataType();
    Object indexType;
    if (Geoshape.class.isAssignableFrom(dataType)) {
      indexType = "2dsphere";
    } else {
      indexType = 1;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(name + ": creating index - " + key + " (index type is " + indexType + ")");
    }
    collection.createIndex(new Document(key, indexType));
  }

  public void restore(Map<String, List<IndexEntry>> documents) throws BackendException {
    List<ReplaceOneModel<Document>> bulkWriteModels = Lists.newArrayListWithCapacity(documents.size());
    for (Entry<String, List<IndexEntry>> entry : documents.entrySet()) {
      String key = entry.getKey();
      List<IndexEntry> fields = entry.getValue();
      Map<String, Object> map = Maps.newHashMapWithExpectedSize(fields.size());
      fields.forEach(field -> map.put(field.field, convertValue(field.value)));
      bulkWriteModels.add(new ReplaceOneModel<>(Filters.eq(ID_KEY, key), new Document(map),
          new UpdateOptions().upsert(true)));
    }
    collection.bulkWrite(bulkWriteModels, new BulkWriteOptions().ordered(false));
  }

  private Document convertGeoshape(Geoshape shape) {
    switch (shape.getType()) {
    case POINT:
      Point point = shape.getPoint();
      return new Document(ImmutableMap.of("type", "Point", "coordinates", ImmutableList.of(point.getLongitude(),
          point.getLatitude())));
    case BOX:
      Point lowerLeft = shape.getPoint(0);
      Point upperRight = shape.getPoint(1);
      return createPolygonFromRectangle(lowerLeft, upperRight);
    case CIRCLE:
      throw new UnsupportedOperationException("Index of circle shape is not supported");
    case POLYGON:
      return createPolygonFromShape(shape);
    default:
      throw new UnsupportedOperationException("Unsupported geo shape type " + shape.getType());
    }
  }

  private Bson convertQuery(Condition<TitanElement> condition) {
    if (condition instanceof Not) {
      Not not = (Not) condition;
      return Filters.not(convertQuery(not.getChild()));
    } else if (condition instanceof And) {
      And and = (And) condition;
      Stream<Condition<TitanElement>> stream = StreamSupport.stream(and.getChildren().spliterator(), false);
      List<Bson> children = stream.map(child -> convertQuery(child)).collect(Collectors.toList());
      return Filters.and(children);
    } else if (condition instanceof Or) {
      Or or = (Or) condition;
      Stream<Condition<TitanElement>> stream = StreamSupport.stream(or.getChildren().spliterator(), false);
      List<Bson> children = stream.map(child -> convertQuery(child)).collect(Collectors.toList());
      return Filters.or(children);
    } else if (condition instanceof PredicateCondition) {
      PredicateCondition<String, ?> atom = (PredicateCondition) condition;
      String key = atom.getKey();
      Object value = atom.getValue();
      TitanPredicate predicate = atom.getPredicate();
      if (value instanceof Number) {
        return createValueFilter((Cmp) predicate, key, value);
      } else if (value instanceof String) {
        if (predicate == Text.REGEX) {
          return Filters.regex(key, (String) value);
        } else if (predicate == Text.PREFIX) {
          return Filters.regex(key, Pattern.compile("^" + Pattern.quote((String) value) + ".*$"));
        } else if (predicate instanceof Cmp) {
          return createValueFilter((Cmp) predicate, key, value);
        } else if (predicate == Cmp.EQUAL) {
          return Filters.eq(key, value);
        } else if (predicate == Cmp.NOT_EQUAL) {
          return Filters.ne(key, value);
        }
      } else if (value instanceof Geoshape) {
        Geoshape shape = (Geoshape) value;
        if (shape.getType() != Type.BOX && shape.getType() != Type.POLYGON) {
          throw new UnsupportedOperationException("Unsupported geo shape type " + shape.getType() + " in query");
        }
        switch ((Geo) predicate) {
        case DISJOINT:
          return Filters.not(new Document("$geoIntersects", new Document("$geometry", convertGeoshape(shape))));
        case INTERSECT:
          return new Document("$geoIntersects", new Document("$geometry", convertGeoshape(shape)));
        case WITHIN:
          return new Document("$geoWithin", new Document("$geometry", convertGeoshape(shape)));
        default:
        }
      } else if (value instanceof Date) {
        return createValueFilter((Cmp) predicate, key, ((Date) value).getTime());
      } else if (value instanceof Instant) {
        return createValueFilter((Cmp) predicate, key, ((Instant) value).toEpochMilli());
      } else if (value instanceof Boolean) {
        Cmp cmp = (Cmp) predicate;
        switch (cmp) {
        case EQUAL:
          return Filters.eq(key, value);
        case NOT_EQUAL:
          return Filters.ne(key, value);
        default:
        }
      } else if (value instanceof UUID) {
        Cmp cmp = (Cmp) predicate;
        switch (cmp) {
        case EQUAL:
          return Filters.eq(key, value.toString());
        case NOT_EQUAL:
          return Filters.ne(key, value.toString());
        default:
        }
      }
      throw new UnsupportedOperationException("Unsupported predicate " + predicate + " of type " + predicate.getClass()
          + " for query value " + value + " of type " + value.getClass());
    } else {
      throw new UnsupportedOperationException("Unsupported condition " + condition + " of type "
          + condition.getClass());
    }
  }

  private Object convertValue(Object value) {
    if ((value instanceof Boolean) || (value instanceof Number) || (value instanceof String)) {
      return value;
    } else if (value instanceof Date) {
      return ((Date) value).getTime();
    } else if (value instanceof Instant) {
      return ((Instant) value).toEpochMilli();
    } else if (value instanceof UUID) {
      return value.toString();
    } else if (value instanceof Geoshape) {
      return convertGeoshape((Geoshape) value);
    } else {
      throw new UnsupportedOperationException("Unsupported index value " + value + " of type " + value.getClass());
    }
  }

  private Document createPolygonFromRectangle(Point lowerLeft, Point upperRight) {
    List<List<Float>> points = ImmutableList.of(
        ImmutableList.of(lowerLeft.getLongitude(), lowerLeft.getLatitude()),
        ImmutableList.of(lowerLeft.getLongitude(), upperRight.getLatitude()),
        ImmutableList.of(upperRight.getLongitude(), upperRight.getLatitude()),
        ImmutableList.of(upperRight.getLongitude(), lowerLeft.getLatitude()),
        ImmutableList.of(lowerLeft.getLongitude(), lowerLeft.getLatitude()));
    return new Document(ImmutableMap.of("type", "Polygon", "coordinates", ImmutableList.of(points)));
  }

  private Document createPolygonFromShape(Geoshape shape) {
    List<List<Float>> points = Lists.newArrayListWithCapacity(shape.size() + 1);
    for (int i = 0; i < shape.size(); i++) {
      Point point = shape.getPoint(i);
      points.add(ImmutableList.of(point.getLongitude(), point.getLatitude()));
    }
    points.add(ImmutableList.of(shape.getPoint(0).getLongitude(), shape.getPoint(0).getLatitude()));
    return new Document(ImmutableMap.of("type", "Polygon", "coordinates", ImmutableList.of(points)));
  }

  private Bson createValueFilter(Cmp cmp, String key, Object value) {
    switch (cmp) {
    case EQUAL:
      return Filters.eq(key, value);
    case NOT_EQUAL:
      return Filters.ne(key, value);
    case LESS_THAN:
      return Filters.lt(key, value);
    case LESS_THAN_EQUAL:
      return Filters.lte(key, value);
    case GREATER_THAN:
      return Filters.gt(key, value);
    case GREATER_THAN_EQUAL:
      return Filters.gte(key, value);
    default:
      throw new UnsupportedOperationException("Unsupported comparison operation " + cmp);
    }
  }
}

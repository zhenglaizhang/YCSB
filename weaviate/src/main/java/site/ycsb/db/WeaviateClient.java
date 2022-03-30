/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import redis.clients.jedis.Protocol;
import site.ycsb.*;
import technology.semi.weaviate.client.Config;
import technology.semi.weaviate.client.base.Result;
import technology.semi.weaviate.client.v1.data.model.WeaviateObject;

import java.util.*;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class WeaviateClient extends DB {

  private technology.semi.weaviate.client.WeaviateClient client;

  public static final String HOST_PROPERTY = "weaviate.host";
  public static final String PORT_PROPERTY = "weaviate.port";
  //public static final String PASSWORD_PROPERTY = "redis.password";
  //public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  private Random random = new Random(0);

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);
    client = new technology.semi.weaviate.client.WeaviateClient(new Config("http", String.format("%s:%s", host, port)));
  }

  public void cleanup() {
//    try {
//      // ((Closeable) client).close();
//
//    } catch (IOException e) {
//      throw new DBException("Closing connection failed.");
//    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
//    if (fields == null) {
//      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
//    } else {
//      String[] fieldArray =
//          (String[]) fields.toArray(new String[fields.size()]);
//      List<String> values = jedis.hmget(key, fieldArray);
//
//      Iterator<String> fieldIterator = fields.iterator();
//      Iterator<String> valueIterator = values.iterator();
//
//      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
//        result.put(fieldIterator.next(),
//            new StringByteIterator(valueIterator.next()));
//      }
//      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
//    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  public Float[] generateVector(int dim) {
    Float[] v = new Float[dim];
    for (int i = 0; i < dim; ++i) {
      v[i] = random.nextFloat();
    }
    return v;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    System.out.println(stringMap);

    Map<String, java.lang.Object> propertiesSchemaT = new HashMap<>();
    String objTID = "abefd256-8574-442b-9293-9205193737ee";
    propertiesSchemaT.put("name", "Hawaii");
    propertiesSchemaT.put("description", "Universally accepted to be the best pizza ever created.");
    Result<WeaviateObject> obj = client.data().creator()
        .withClassName("Soup")
        .withID(objTID)
        .withVector(generateVector(30))
        .withProperties(propertiesSchemaT)
        .run();
    System.out.println(obj.getError().getMessages());
    return obj.hasErrors() ? Status.ERROR : Status.OK;
  }


  @Override
  public Status delete(String table, String key) {
    return client.data().deleter().withID(key).run().hasErrors() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
//    return jedis.hmset(key, StringByteIterator.getStringMap(values))
//        .equals("OK") ? Status.OK : Status.ERROR;
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
//    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
//        Double.POSITIVE_INFINITY, 0, recordcount);
//
//    HashMap<String, ByteIterator> values;
//    for (String key : keys) {
//      values = new HashMap<String, ByteIterator>();
//      read(table, key, fields, values);
//      result.add(values);
//    }

    return Status.OK;
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.nessie.exp;

import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.http.v1api.HttpApiV1;

public class PreconfiguredUtilitiesStore {
  private static final ConcurrentMap<String, Object> entries = Maps.newConcurrentMap();

  private PreconfiguredUtilitiesStore() {
    // nop
  }

  static {
    // some example data - can be injected from outside
    HttpApiV1 api = HttpClientBuilder.builder()
            .withUri("http://localhost:19120/api/v1")
            .build(HttpApiV1.class);
    entries.put("exampleClient", api);
  }

  static <T> T get(String name) {
    // noinspection unchecked
    return (T) entries.get(name);
  }
}

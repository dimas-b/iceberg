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

import java.net.URI;
import java.util.function.Function;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.auth.NessieAuthentication;

public class PreconfiguredClientBuilder implements NessieClientBuilder<PreconfiguredClientBuilder> {
  private String key = "defaultKey";

  public static PreconfiguredClientBuilder builder() {
    return new PreconfiguredClientBuilder();
  }

  @Override
  public PreconfiguredClientBuilder fromSystemProperties() {
    return fromConfig(System::getProperty);
  }

  @Override
  public PreconfiguredClientBuilder fromConfig(Function<String, String> configuration) {
    key = configuration.apply("nessie.client.key");
    return this;
  }

  @Override
  public PreconfiguredClientBuilder withAuthenticationFromConfig(Function<String, String> configuration) {
    return this; // nop
  }

  @Override
  public PreconfiguredClientBuilder withAuthentication(NessieAuthentication authentication) {
    return this; // nop
  }

  @Override
  public PreconfiguredClientBuilder withUri(URI uri) {
    return this; // nop
  }

  @Override
  public <T extends NessieApi> T build(Class<T> apiContract) {
    return PreconfiguredUtilitiesStore.get(key);
  }
}

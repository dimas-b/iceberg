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
package org.apache.iceberg.nessie;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

public class TestNessieIcebergClient extends BaseTestIceberg {

  private static final String BRANCH = "test-nessie-client";

  public TestNessieIcebergClient() {
    super(BRANCH);
  }

  @Test
  public void testWithNullRefLoadsMain() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, null, null, ImmutableMap.of());
    Assertions.assertThat(client.getRef().getReference())
        .isEqualTo(api.getReference().refName("main").get());
  }

  @Test
  public void testWithNullHash() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, BRANCH, null, ImmutableMap.of());
    Assertions.assertThat(client.getRef().getReference())
        .isEqualTo(api.getReference().refName(BRANCH).get());
  }

  @Test
  public void testWithReference() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, "main", null, ImmutableMap.of());

    Assertions.assertThat(client.withReference(null, null)).isEqualTo(client);
    Assertions.assertThat(client.withReference("main", null)).isNotEqualTo(client);
    Assertions.assertThat(
            client.withReference("main", api.getReference().refName("main").get().getHash()))
        .isEqualTo(client);

    Assertions.assertThat(client.withReference(BRANCH, null)).isNotEqualTo(client);
    Assertions.assertThat(
            client.withReference(BRANCH, api.getReference().refName(BRANCH).get().getHash()))
        .isNotEqualTo(client);
  }

  @Test
  public void testWithReferenceAfterRecreatingBranch()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "branchToBeDropped";
    createBranch(branch, null);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, ImmutableMap.of());

    // just create a new commit on the branch and then delete & re-create it
    Namespace namespace = Namespace.of("a");
    client.createNamespace(namespace, ImmutableMap.of());
    Assertions.assertThat(client.listNamespaces(namespace)).isNotNull();
    client
        .getApi()
        .deleteBranch()
        .branch((Branch) client.getApi().getReference().refName(branch).get())
        .delete();
    createBranch(branch, null);

    // make sure the client uses the re-created branch
    Reference ref = client.getApi().getReference().refName(branch).get();
    Assertions.assertThat(client.withReference(branch, null).getRef().getReference())
        .isEqualTo(ref);
    Assertions.assertThat(client.withReference(branch, null)).isNotEqualTo(client);
  }

  @Test
  public void testFindImpliedNamespaces() throws NessieConflictException, NessieNotFoundException {
    String branch = "testFindImpliedNamespaces";
    createBranch(branch, null);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, ImmutableMap.of());

    Assertions.assertThat(client.findImpliedNamespaces(ContentKey.of("Table"))).isEmpty();

    Assertions.assertThat(client.findImpliedNamespaces(ContentKey.of("a", "b", "c", "Table")))
        .containsExactlyInAnyOrder(
            org.projectnessie.model.Namespace.of("a"),
            org.projectnessie.model.Namespace.of("a", "b"),
            org.projectnessie.model.Namespace.of("a", "b", "c"));

    createNamespace(branch, org.projectnessie.model.Namespace.of("a"));
    createNamespace(branch, org.projectnessie.model.Namespace.of("a", "b"));
    client.refresh();
    Assertions.assertThat(client.findImpliedNamespaces(ContentKey.of("a", "b", "c", "Table")))
        .containsExactlyInAnyOrder(org.projectnessie.model.Namespace.of("a", "b", "c"));

    createNamespace(branch, org.projectnessie.model.Namespace.of("a", "b", "c"));
    client.refresh();
    Assertions.assertThat(client.findImpliedNamespaces(ContentKey.of("a", "b", "c", "Table")))
        .isEmpty();

    createNamespace(branch, org.projectnessie.model.Namespace.of("x", "y", "z"));
    client.refresh();
    // Namespaces `x` and `x.y` are implied by the explicitly created namespace `x.y.z`
    Assertions.assertThat(client.findImpliedNamespaces(ContentKey.of("x", "y", "z", "Table")))
        .isEmpty();
  }

  @Test
  public void testFindImpliedNamespacesWrongBranch()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "testFindImpliedNamespacesWrongBranch";
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, ImmutableMap.of());

    // The Nessie Server is not contacted in this case
    Assertions.assertThat(client.findImpliedNamespaces(ContentKey.of("Table"))).isEmpty();

    Assertions.assertThatThrownBy(() -> client.findImpliedNamespaces(ContentKey.of("a", "Table")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Nessie ref 'testFindImpliedNamespacesWrongBranch' does not exist");

    createBranch(branch, null);
    client.refresh();
    api.deleteBranch().branch((Branch) api.getReference().refName(branch).get()).delete();

    Assertions.assertThatThrownBy(() -> client.findImpliedNamespaces(ContentKey.of("a", "Table")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Cannot load Namespaces for 'a.Table': ref 'testFindImpliedNamespacesWrongBranch' is no longer valid.");
  }

  @Test
  public void testFindImpliedNamespacesDisabled() {
    String branch = "testFindImpliedNamespacesDisabled";
    NessieIcebergClient client =
        new NessieIcebergClient(
            api, branch, null, ImmutableMap.of(NessieUtil.CREATE_IMPLIED_NAMESPACES, "false"));

    Assertions.assertThat(client.findImpliedNamespaces(ContentKey.of("a", "b", "Table"))).isEmpty();
  }
}

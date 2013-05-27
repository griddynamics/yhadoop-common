/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.client;


import java.nio.ByteBuffer;

import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestYarnApiClasses {
  private final org.apache.hadoop.yarn.factories.RecordFactory recordFactory = RecordFactoryProvider
          .getRecordFactory(null);

  /**
   * Simple test Resource request.
   * Test hashCode, equals and compare.
   */
  @Test(timeout = 1000)
  public void testResourceRequest() {

    Resource resource = recordFactory.newRecordInstance(Resource.class);
    Priority priority = recordFactory.newRecordInstance(Priority.class);

    ResourceRequest original = recordFactory.newRecordInstance(ResourceRequest.class);
    original.setHostName("localhost");
    original.setNumContainers(2);
    original.setPriority(priority);
    original.setCapability(resource);

    ResourceRequest copy = recordFactory.newRecordInstance(ResourceRequest.class);
    copy.setHostName("localhost");
    copy.setNumContainers(2);
    copy.setPriority(priority);
    copy.setCapability(resource);

    assertTrue(original.equals(copy));
    assertEquals(0, original.compareTo(copy));
    assertTrue(original.hashCode() == copy.hashCode());

    copy.setNumContainers(1);

    assertFalse(original.equals(copy));
    assertNotSame(0, original.compareTo(copy));
    assertFalse(original.hashCode() == copy.hashCode());

  }

  /**
  * Test CancelDelegationTokenRequestPBImpl.
  * Test a transformation to prototype and back
  */
  @Test(timeout = 500)
  public void testCancelDelegationTokenRequestPBImpl() {

    DelegationToken token = getDelegationToken();

    CancelDelegationTokenRequestPBImpl original = new CancelDelegationTokenRequestPBImpl();
    original.setDelegationToken(token);
    CancelDelegationTokenRequestProto protoType = original.getProto();

    CancelDelegationTokenRequestPBImpl copy = new CancelDelegationTokenRequestPBImpl(protoType);
    assertNotNull(copy.getDelegationToken());
    //compare source and converted
    assertEquals(token, copy.getDelegationToken());

  }

  /**
  * Test RenewDelegationTokenRequestPBImpl.
  * Test a transformation to prototype and back
  */

  @Test(timeout = 500)
  public void testRenewDelegationTokenRequestPBImpl() {

    DelegationToken token = getDelegationToken();

    RenewDelegationTokenRequestPBImpl original = new RenewDelegationTokenRequestPBImpl();
    original.setDelegationToken(token);
    RenewDelegationTokenRequestProto protoType = original.getProto();

    RenewDelegationTokenRequestPBImpl copy = new RenewDelegationTokenRequestPBImpl(protoType);
    assertNotNull(copy.getDelegationToken());
    //compare source and converted
    assertEquals(token, copy.getDelegationToken());

  }

 
  private DelegationToken getDelegationToken() {
    DelegationToken token = recordFactory.newRecordInstance(DelegationToken.class);
    token.setKind("");
    token.setService("");
    token.setIdentifier(ByteBuffer.allocate(0));
    token.setPassword(ByteBuffer.allocate(0));
    return token;
  }

  private ApplicationAttemptId getApplicationAttemptId() {
    ApplicationAttemptId appAttemptId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(getApplicationId());
    appAttemptId.setAttemptId(5);
    return appAttemptId;
  }

  private ApplicationId getApplicationId() {
    ApplicationId appId = recordFactory.newRecordInstance(ApplicationId.class);
    appId.setId(4);
    return appId;
  }
}

package org.apache.hadoop.yarn.client;


import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ClientToken;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationMasterPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStatusPBImpl;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;
import static org.junit.Assert.*;


public class TestYarnApiClasses {
  private final org.apache.hadoop.yarn.factories.RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  /*
   * Simple test Resource request.
   * Test hashCode, equals and compare.
   */
  @Test (timeout=1000)
  public void testResourceRequest(){

    Resource resource =recordFactory.newRecordInstance(Resource.class);
    Priority priority=recordFactory.newRecordInstance(Priority.class);

    ResourceRequest original= recordFactory.newRecordInstance(ResourceRequest.class);
    original.setHostName("localhost");
    original.setNumContainers(2);
    original.setPriority(priority);
    original.setCapability(resource);
    
    ResourceRequest copy= recordFactory.newRecordInstance(ResourceRequest.class);
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
  
  /*
   * Test CancelDelegationTokenRequestPBImpl. 
   * Test a transformation to prototype and back 
   */
  @Test (timeout=500)
  public void testCancelDelegationTokenRequestPBImpl(){
    
    DelegationToken token=getDelegationToken();
    
    CancelDelegationTokenRequestPBImpl original = new CancelDelegationTokenRequestPBImpl();
    original.setDelegationToken(token);
    CancelDelegationTokenRequestProto protoType=original.getProto();
    
    CancelDelegationTokenRequestPBImpl copy= new CancelDelegationTokenRequestPBImpl(protoType);
    assertNotNull(copy.getDelegationToken());
    //compare source and converted
    assertEquals(token, copy.getDelegationToken());
    
  }

  /*
  * Test RenewDelegationTokenRequestPBImpl.
  * Test a transformation to prototype and back
  */

  @Test (timeout=500)
  public void testRenewDelegationTokenRequestPBImpl(){
    
    DelegationToken token=getDelegationToken();

    RenewDelegationTokenRequestPBImpl original = new RenewDelegationTokenRequestPBImpl();
    original.setDelegationToken(token);
    RenewDelegationTokenRequestProto protoType =original.getProto();

    RenewDelegationTokenRequestPBImpl copy = new RenewDelegationTokenRequestPBImpl(protoType);
    assertNotNull(copy.getDelegationToken());
    //compare source and converted
    assertEquals(token, copy.getDelegationToken());
    
  }

  /*
  * Test ApplicationMasterPBImpl.
  * Test a transformation to prototype and back
  */

  @Test (timeout=500)
  public void testApplicationMasterPBImpl (){
    
    ClientToken clientToken= recordFactory.newRecordInstance(ClientToken.class);
    clientToken.setKind("");
    clientToken.setService("");
    clientToken.setIdentifier(ByteBuffer.allocate(0));
    clientToken.setPassword(ByteBuffer.allocate(0));
    
    ApplicationStatus appStatus=recordFactory.newRecordInstance(ApplicationStatus.class);
    appStatus.setApplicationAttemptId(getApplicationAttemptId());
    appStatus.setProgress(0.6f);
    appStatus.setResponseId(6);
    
    ApplicationMasterPBImpl original= new ApplicationMasterPBImpl();
    original.setAMFailCount(1);
    original.setApplicationId(getApplicationId());
    original.setClientToken(clientToken);
    original.setContainerCount(2);
    original.setDiagnostics("diagnostics");
    original.setHost("localhost");
    original.setRpcPort(5);
    original.setState(YarnApplicationState.NEW);
    original.setStatus(appStatus);
    original.setTrackingUrl("TrackingUrl");

    // get copy
    ApplicationMasterPBImpl copy= new ApplicationMasterPBImpl(original.getProto());
    // test copy
    assertEquals(original.getAMFailCount(), copy.getAMFailCount());
    assertEquals(original.getApplicationId(), copy.getApplicationId());
    assertEquals(original.getClientToken(), copy.getClientToken());
    assertEquals(original.getContainerCount(), copy.getContainerCount());
    assertEquals(original.getDiagnostics(), copy.getDiagnostics());
    assertEquals(original.getHost(), copy.getHost());
    assertEquals(original.getRpcPort(), copy.getRpcPort());
    assertEquals(original.getState(), copy.getState());
    assertEquals(original.getStatus(), copy.getStatus());
    assertEquals(original.getTrackingUrl(), copy.getTrackingUrl());
    
  }

  /*
  * Test ApplicationStatusPBImpl.
  * Test a transformation to prototype and back
  */

  @Test (timeout=500)
  public void testApplicationStatusPBImpl(){
    
    ApplicationStatusPBImpl original = new ApplicationStatusPBImpl();
    original.setApplicationAttemptId(getApplicationAttemptId());
    original.setProgress(0.4f);
    original.setResponseId(1);
    
    ApplicationStatusPBImpl copy = new ApplicationStatusPBImpl(  original.getProto());
    assertEquals(original.getApplicationAttemptId(), copy.getApplicationAttemptId());
    assertEquals(original.getProgress(), copy.getProgress(),0.0001);
    assertEquals(original.getResponseId(), copy.getResponseId());
  
  }

  
  
  
 
  private ApplicationAttemptId getApplicationAttemptId(){
    ApplicationAttemptId appAttemptId=recordFactory.newRecordInstance(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(getApplicationId());
    appAttemptId.setAttemptId(5);
    return appAttemptId;
  }

  private ApplicationId getApplicationId(){
    ApplicationId appId=recordFactory.newRecordInstance(ApplicationId.class);
    appId.setId(4);
    return appId;
  }
}

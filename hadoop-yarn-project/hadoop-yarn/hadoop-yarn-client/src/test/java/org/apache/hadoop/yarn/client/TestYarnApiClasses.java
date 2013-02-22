package org.apache.hadoop.yarn.client;


import java.nio.ByteBuffer;

import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
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

  /*
   * simple test Resource request
   */
  @Test
  public void testResiurceRequest(){

    Resource resource =recordFactory.newRecordInstance(Resource.class);
    Priority priority=recordFactory.newRecordInstance(Priority.class);

    ResourceRequest test1= recordFactory.newRecordInstance(ResourceRequest.class);
    test1.setHostName("localhost");
    test1.setNumContainers(2);
    test1.setPriority(priority);
    test1.setCapability(resource);
    
    ResourceRequest test2= recordFactory.newRecordInstance(ResourceRequest.class);
    test2.setHostName("localhost");
    test2.setNumContainers(2);
    test2.setPriority(priority);
    test2.setCapability(resource);
    
   assertTrue( test1.equals(test2));
   assertEquals(0, test1.compareTo(test2));
   assertTrue( test1.hashCode()==test2.hashCode());

   test2.setNumContainers(1);
   
   assertFalse( test1.equals(test2));
   assertNotSame(0, test1.compareTo(test2));
   assertFalse( test1.hashCode()==test2.hashCode());
    
  }
  
  @Test 
  public void testCancelDelegationTokenRequestPBImpl(){
    
    DelegationToken token=getDelegationToken();
    
    CancelDelegationTokenRequestPBImpl test = new CancelDelegationTokenRequestPBImpl();
    test.setDelegationToken(token);
    CancelDelegationTokenRequestProto proto=test.getProto();
    
    CancelDelegationTokenRequestPBImpl test2= new CancelDelegationTokenRequestPBImpl(proto);
    assertNotNull(test2.getDelegationToken());
    //compare source and converted
    assertEquals(token, test2.getDelegationToken());
    
  }
  
  
  
  @Test 
  public void testRenewDelegationTokenRequestPBImpl(){
    
    DelegationToken token=getDelegationToken();

    RenewDelegationTokenRequestPBImpl test = new RenewDelegationTokenRequestPBImpl();
    test.setDelegationToken(token);
    RenewDelegationTokenRequestProto proto=test.getProto();
    
    RenewDelegationTokenRequestPBImpl test2= new RenewDelegationTokenRequestPBImpl(proto);
    assertNotNull(test2.getDelegationToken());
    //compare source and converted
    assertEquals(token, test2.getDelegationToken());
    
  }
  
  private DelegationToken getDelegationToken(){
    DelegationToken token=recordFactory.newRecordInstance(DelegationToken.class);
    token.setKind("");
    token.setService("");
    token.setIdentifier(ByteBuffer.allocate(0));
    token.setPassword(ByteBuffer.allocate(0));
    return token;
  }
}

package org.apache.hadoop.util.bloom;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.junit.Assert;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;
/*
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class FilterTester<T extends Filter> {    
  
  private final int hashType;
  private final int numInsertions;    
  
  private final ImmutableList.Builder<T> builder = 
      ImmutableList.builder(); 
  
  private ImmutableSet<FilterTestStrategy> filterTestStrateges;
  
  private final PreAssertionHelper preAssertionHelper;
  
  public static <T extends Filter> FilterTester<T> of(int hashId, int numInsertions) {    
    return new FilterTester<T>(hashId, numInsertions);
  }

  public FilterTester<T> withFilterInstance(T filter) {    
    builder.add(filter);
    return this;
  }
  
  private FilterTester(int hashId, int numInsertions) {
    this.hashType = hashId;
    this.numInsertions = numInsertions;
    
    this.preAssertionHelper = new PreAssertionHelper() {
      
      @Override
      public ImmutableSet<Integer> falsePositives(int hashId) {                
        switch (hashId) {
          case Hash.JENKINS_HASH: { return ImmutableSet.of(17, 165, 235, 249, 289, 323, 575, 641, 
              671, 801, 803, 913); }
          case Hash.MURMUR_HASH: { return ImmutableSet.of(57, 91, 117, 139, 231, 245, 261, 271, 
              381, 541, 581, 615, 812, 821, 837); }
          default : {
            // fail with unknown hash error !!!
            Assert.assertFalse("unknown hash error", true);        
            return ImmutableSet.of();
          }               
        }               
      }
    };
  }
  
  public FilterTester<T> withTestCases(ImmutableSet<FilterTestStrategy> filterTestStrateges) {
    this.filterTestStrateges = ImmutableSet.copyOf(filterTestStrateges);
    return this;
  }
  
  public void test() {
    final ImmutableList<T> filtersList = builder.build();
    final ImmutableSet<Integer> falsePositives = preAssertionHelper.falsePositives(hashType);       
    
    for (T filter: filtersList) {
      for (FilterTestStrategy strategy: filterTestStrateges) {
        strategy.getStrategy().assertWhat(filter, numInsertions, hashType, falsePositives);
        filter.and(getSymmetricFilter(filter.getClass(), numInsertions, hashType));
      }          
    }    
  }
  
  interface FilterTesterStrategy {    
    void assertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet<Integer> falsePositives);    
  }
  
  private static Filter getSymmetricFilter(Class<?> filterClass, int numInsertions, int hashType) {        
    int bitSetSize = TestBloomFIlter.optimalNumOfBits(numInsertions, 0.03);
    
    if (filterClass == BloomFilter.class) {
      return new BloomFilter(bitSetSize, 5, hashType);
    } else if (filterClass == CountingBloomFilter.class)  {
      return new CountingBloomFilter(bitSetSize, 5, hashType);              
    } else {
      return null;
    } 
  }
  
  
  public enum FilterTestStrategy {                       
    
    ODD_EVEN_ABSENT_STRATEGY(new FilterTesterStrategy() {            
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet<Integer> falsePositives) {                       
        
        // add all even keys
        for (int i = 0; i < numInsertions * 2; i += 2) {
          filter.add(new Key(Integer.toString(i).getBytes()));
        }

        // check on present even key 
        for (int i = 0; i < numInsertions * 2; i += 2) {
          Assert.assertTrue(" filter might contains " + i, 
              filter.membershipTest(new Key(Integer.toString(i).getBytes())));
        }                   
                
        // check on absent odd in event 
        for (int i = 1; i < 1000; i += 2) {
          if (!falsePositives.contains(i)) {        
            assertFalse(" filter should not contain " + i,
                filter.membershipTest(new Key(Integer.toString(i).getBytes())));
          }
        }        
        logger.debug("pass" + ODD_EVEN_ABSENT_STRATEGY + "with " + filter.getClass().getCanonicalName());
      }           
    }),
    
    WRITE_READ_STRATEGY(new FilterTesterStrategy() {
      
      private int slotSize; 
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId,
          ImmutableSet<Integer> falsePositives) {
        
        final Random rnd = new Random();
        final DataOutputBuffer out = new DataOutputBuffer();
        final DataInputBuffer in = new DataInputBuffer();        
        try {
          Filter tempFilter = getSymmetricFilter(filter.getClass(), numInsertions, hashId);            
          ImmutableList.Builder<Integer> blist = ImmutableList.builder();
          for (int i = 0; i < slotSize; i++) {
            blist.add(rnd.nextInt(numInsertions * 2));
          }
          
          ImmutableList<Integer> list = blist.build();
          
          //mark bits for later check
          for(Integer slot: list) {
            filter.add(new Key(String.valueOf(slot).getBytes()));
          }
                              
          filter.write(out);
          in.reset(out.getData(), out.getLength());
          tempFilter.readFields(in);
          
          for(Integer slot: list) {
            assertTrue("read/write mask check filter error on " + slot,
                  filter.membershipTest(new Key(String.valueOf(slot).getBytes())));
          }          
                    
        } catch(IOException ex) {
          Assert.fail("error ex !!!" + ex);
        }
        
        logger.debug("pass " + WRITE_READ_STRATEGY + " with " + filter.getClass().getCanonicalName());
      }      
    }),
    
    FILTER_XOR_STRATEGY(new FilterTesterStrategy() {
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId,
          ImmutableSet<Integer> falsePositives) { 
        Filter oddFilter = getSymmetricFilter(filter.getClass(), numInsertions, hashId);                
        
        //add all even keys
        for (int i = 0; i < numInsertions * 2; i += 2) {
          filter.add(new Key(Integer.toString(i).getBytes()));
        }
                
        //add all odd keys
        for (int i = 1; i < numInsertions * 2; i += 2) {
          oddFilter.add(new Key(Integer.toString(i).getBytes()));
        }
        
        try {
          filter.xor(oddFilter);
        } catch(UnsupportedOperationException ex){}
        /*
        // check on present even key 
        for (int i = 0; i < numInsertions * 2; i ++) {
          Assert.assertTrue(" filter might contains " + i, 
              filter.membershipTest(new Key(Integer.toString(i).getBytes())));
        }                   
        */
        logger.debug("pass " + FILTER_XOR_STRATEGY + " with " + filter.getClass().getCanonicalName());
      }      
    }),
    
    FILTER_OR_STRATEGY(new FilterTesterStrategy() {         
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet<Integer> falsePositives) {                 
        Filter evenFilter = getSymmetricFilter(filter.getClass(), numInsertions, hashId);
        
        // add all even
        for (int i = 0; i < numInsertions * 2; i += 2) {
          evenFilter.add(new Key(Integer.toString(i).getBytes()));
        }
        
        // add all odd
        for (int i = 1; i < numInsertions * 2; i += 2) {
          filter.add(new Key(Integer.toString(i).getBytes()));
        }
        
        // union odd with even
        filter.or(evenFilter);
                               
        // check on present all key 
        for (int i = 0; i < numInsertions * 2; i++) {
          Assert.assertTrue(" filter might contains " + i, 
              filter.membershipTest(new Key(Integer.toString(i).getBytes())));
        }
      }        
    });        
    
    private final FilterTesterStrategy testerStrategy;
    private static final Logger logger = Logger.getLogger(FilterTestStrategy.class);
    
    FilterTestStrategy(FilterTesterStrategy testerStrategy) {
      this.testerStrategy = testerStrategy;
    }
    
    public FilterTesterStrategy getStrategy() {
      return testerStrategy;
    }
    
  }       
    
  interface PreAssertionHelper {
    public ImmutableSet<Integer> falsePositives(int hashId);
  }
  
}

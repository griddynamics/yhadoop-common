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
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
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

public class BloomFilterTester<T extends BloomFilter> {    
  
  private static final double LN2 = Math.log(2);
  private static final double LN2_SQUARED = LN2 * LN2;  
 
  private final int hashType;
  private final int numInsertions;    
  
  private final ImmutableList.Builder<T> builder = 
      ImmutableList.builder(); 
    
  private ImmutableSet<BloomFilterTestStrategy> filterTestStrateges;
  
  private final PreAssertionHelper preAssertionHelper;
  
  static int optimalNumOfBits(int n, double p) {
    return (int) (-n * Math.log(p) / LN2_SQUARED);
  }
  
  public static <T extends BloomFilter> BloomFilterTester<T> of(int hashId, int numInsertions) {    
    return new BloomFilterTester<T>(hashId, numInsertions);
  }

  public BloomFilterTester<T> withFilterInstance(T filter) {    
    builder.add(filter);
    return this;
  }
  
  private BloomFilterTester(int hashId, int numInsertions) {
    this.hashType = hashId;
    this.numInsertions = numInsertions;
    
    this.preAssertionHelper = new PreAssertionHelper() {
      
      @Override
      public ImmutableSet<Integer> falsePositives(int hashId) {                
        switch (hashId) {
          case Hash.JENKINS_HASH: {
            //// false pos for odd and event under 1000
            return ImmutableSet.of(99, 963); 
          }
          case Hash.MURMUR_HASH: { 
            // false pos for odd and event under 1000
            return ImmutableSet.of(769, 772, 810, 874); 
          } 
          default : {
            // fail with unknown hash error !!!
            Assert.assertFalse("unknown hash error", true);        
            return ImmutableSet.of();
          }               
        }               
      }
    };
  }
  
  public BloomFilterTester<T> withTestCases(ImmutableSet<BloomFilterTestStrategy> filterTestStrateges) {
    this.filterTestStrateges = ImmutableSet.copyOf(filterTestStrateges);
    return this;
  }
  
  public void test() {
    final ImmutableList<T> filtersList = builder.build();
    final ImmutableSet<Integer> falsePositives = preAssertionHelper.falsePositives(hashType);       
    
    for (T filter: filtersList) {
      for (BloomFilterTestStrategy strategy: filterTestStrateges) {
        strategy.getStrategy().assertWhat(filter, numInsertions, hashType, falsePositives);                
        filter.and(getSymmetricFilter(filter.getClass(), numInsertions, hashType));
      }          
    }    
  }
  
  interface FilterTesterStrategy {
    final Logger logger = Logger.getLogger(FilterTesterStrategy.class);    
    abstract void assertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet<Integer> falsePositives);    
  }
  
  private static Filter getSymmetricFilter(Class<?> filterClass, int numInsertions, int hashType) {        
    int bitSetSize = optimalNumOfBits(numInsertions, 0.03);
    
    if (filterClass == BloomFilter.class) {
      return new BloomFilter(bitSetSize, 5, hashType);
    } else if (filterClass == CountingBloomFilter.class)  {
      return new CountingBloomFilter(bitSetSize, 5, hashType);              
    } else if (filterClass == RetouchedBloomFilter.class) {
      return new RetouchedBloomFilter(bitSetSize, 5, hashType);
    } else {
      return null;
    }
  }
  
  
  public enum BloomFilterTestStrategy {    
    
    ADD_KEYS_STRATEGY(new FilterTesterStrategy() {

      private final ImmutableList<Key> keys = ImmutableList.of(
          new Key(new byte[] {49, 48, 48}), 
          new Key(new byte[] {50, 48, 48}));
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId,
          ImmutableSet<Integer> falsePositives) {               
        
        filter.add(keys);
        
        assertTrue(" might contain key error ", 
            filter.membershipTest(new Key("100".getBytes())));
        assertTrue(" might contain key error ", 
            filter.membershipTest(new Key("200".getBytes())));
        
        filter.add(keys.toArray(new Key[]{}));
        
        assertTrue(" might contain key error ", 
            filter.membershipTest(new Key("100".getBytes())));
        assertTrue(" might contain key error ", 
            filter.membershipTest(new Key("200".getBytes())));
        
        filter.add(new AbstractCollection<Key>() {
          
          @Override
          public Iterator<Key> iterator() {
            return keys.iterator();
          }

          @Override
          public int size() {
            return keys.size();
          }
          
        });
        
        assertTrue(" might contain key error ", 
            filter.membershipTest(new Key("100".getBytes())));
        assertTrue(" might contain key error ", 
            filter.membershipTest(new Key("200".getBytes())));
      }      
    }),
    
    KEY_TEST_STRATEGY(new FilterTesterStrategy() {
      
      private void checkOnKeyMethods() {
        String line = "werabsdbe";
        
        Key key = new Key(line.getBytes());
        assertTrue("default key weight error ",  key.getWeight() == 1d);
        
        key.set(line.getBytes(), 2d);
        assertTrue(" setted key weight error ", key.getWeight() == 2d);
        
        Key sKey = new Key(line.getBytes(), 2d);
        assertTrue("", key.equals(sKey));
        assertTrue("", key.hashCode() == sKey.hashCode());
                
        sKey = new Key(line.concat("a").getBytes(), 2d);
        assertFalse("", key.equals(sKey));                
        assertFalse("", key.hashCode() == sKey.hashCode());
        
        sKey = new Key(line.getBytes(), 3d);
        assertFalse("", key.equals(sKey));
        assertFalse("", key.hashCode() == sKey.hashCode());    
        
        key.incrementWeight();
        assertTrue("", key.getWeight() == 3d);
        
        key.incrementWeight(2d);
        assertTrue("", key.getWeight() == 5d);                    
      }
         
      private void checkOnReadWrite() {
        String line = "qryqeb354645rghdfvbaq23312fg";
        DataOutputBuffer out = new DataOutputBuffer();  
        DataInputBuffer in = new DataInputBuffer();
        Key originKey = new Key(line.getBytes(), 100d);
        try {
          originKey.write(out);
          in.reset(out.getData(), out.getData().length);
          Key restoredKey = new Key(new byte[] {0});
          assertFalse("", restoredKey.equals(originKey));
          restoredKey.readFields(in); 
          assertTrue("", restoredKey.equals(originKey));
          out.reset();          
        } catch(Exception ioe) {
          Assert.fail("");
        }        
      }
      
      private void checkSetOnIAE() {
        Key key = new Key();
        try {          
          key.set(null, 0);
        } catch(IllegalArgumentException ex) {
          //expected
        } catch (Exception e) {
          Assert.fail("");
        }
      }
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId,
          ImmutableSet<Integer> falsePositives) {       
        checkOnKeyMethods();
        checkOnReadWrite();
        checkSetOnIAE();
        //logger.debug("pass" + KEY_TEST_STRATEGY + "with " + filter.getClass().getCanonicalName());
        System.out.println("pass" + KEY_TEST_STRATEGY + "with " + filter.getClass().getCanonicalName());
      }      
      
    }),        
    
    EXCEPTIONS_CHECK_STRATEGY(new FilterTesterStrategy() {
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId,
          ImmutableSet<Integer> falsePositives) {                
        checkAddOnNPE(filter);  
        checkTestMembershipOnNPE(filter);
        checkAndOnIAE(filter);
        //logger.debug("pass" + EXCEPTIONS_CHECK_STRATEGY + "with " + filter.getClass().getCanonicalName());
        System.out.println("pass" + EXCEPTIONS_CHECK_STRATEGY + "with " + filter.getClass().getCanonicalName());
      }
      
      private void checkAndOnIAE(Filter filter) {
        Filter tfilter = null;
        
        try {
          Collection<Key> keys = null;
          filter.add(keys);
        } catch (IllegalArgumentException ex) {
          //
        } catch (Exception e) {
          Assert.fail("" + e);
        }        
                
        try {
          Key[] keys = null;
          filter.add(keys);
        } catch (IllegalArgumentException ex) {
          //
        } catch (Exception e) {
          Assert.fail("" + e);
        }
        
        try {
          ImmutableList<Key> keys = null;
          filter.add(keys);
        } catch (IllegalArgumentException ex) {
          //
        } catch (Exception e) {
          Assert.fail("" + e);
        }
        
        try {          
          filter.and(tfilter);
        } catch (IllegalArgumentException ex) {
          //expected
        } catch(Exception e) {
          Assert.fail("" + e);
        }
        
        try {
          filter.or(tfilter);
        } catch (IllegalArgumentException ex) {
          //expected
        } catch(Exception e) {
          Assert.fail("" + e);
        }
        
        try {
          filter.xor(tfilter);
        } catch(IllegalArgumentException ex) {
          //expected
        } catch(UnsupportedOperationException unex) {
          //
        }catch(Exception e) {
          Assert.fail("" + e);
        }
        
      }

      private void checkTestMembershipOnNPE(Filter filter) {
        try {
          Key nullKey = null;
          filter.membershipTest(nullKey);
        } catch (NullPointerException ex) {
          // expected
        } catch(Exception e) {
          Assert.fail("" + e);
        }
      }
      
      private void checkAddOnNPE(Filter filter) {
        try {
          Key nullKey = null;
          filter.add(nullKey);
        } catch(NullPointerException ex) {
          //expected
        } catch (Exception e) {
          Assert.fail("" + e);
        }                
      }      
    }),
    
    ODD_EVEN_ABSENT_STRATEGY(new FilterTesterStrategy() {            
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet<Integer> falsePositives) {                       
        
        // add all even keys
        for (int i = 0; i < numInsertions; i += 2) {
          filter.add(new Key(Integer.toString(i).getBytes()));
        }

        // check on present even key 
        for (int i = 0; i < numInsertions; i += 2) {
          Assert.assertTrue(" filter might contains " + i, 
              filter.membershipTest(new Key(Integer.toString(i).getBytes())));
        }                   
                
        // check on absent odd in event 
        for (int i = 1; i < numInsertions; i += 2) {
          if (!falsePositives.contains(i)) {        
            assertFalse(" filter should not contain " + i,
                filter.membershipTest(new Key(Integer.toString(i).getBytes())));
          }
        }        
        
        System.out.println("pass" + ODD_EVEN_ABSENT_STRATEGY + "with " + filter.getClass().getCanonicalName());
        //logger.debug("pass" + ODD_EVEN_ABSENT_STRATEGY + "with " + filter.getClass().getCanonicalName());
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
        
        System.out.println("pass " + WRITE_READ_STRATEGY + " with " + filter.getClass().getCanonicalName());
        //logger.debug("pass " + WRITE_READ_STRATEGY + " with " + filter.getClass().getCanonicalName());
      }      
    }),
    
    FILTER_XOR_STRATEGY(new FilterTesterStrategy() {
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId,
          ImmutableSet<Integer> falsePositives) { 
        Filter symmetricFilter = getSymmetricFilter(filter.getClass(), numInsertions, hashId);                        
        // 0 xor 0 -> 0 
        filter.xor(symmetricFilter);
        // check on present all key
        for (int i = 0; i < numInsertions; i ++) {
          Assert.assertFalse(" filter might contains " + i, 
              filter.membershipTest(new Key(Integer.toString(i).getBytes())));
        }
        
        //add all even keys
        for (int i = 0; i < numInsertions; i += 2) {
          filter.add(new Key(Integer.toString(i).getBytes()));
        }
                
        //add all odd keys
        for (int i = 0; i < numInsertions; i += 2) {
          symmetricFilter.add(new Key(Integer.toString(i).getBytes()));
        }
        
        try {
          filter.xor(symmetricFilter);
          // 1 xor 1 -> 0
          // check on absent all key 
          for (int i = 0; i < numInsertions; i ++) {
            Assert.assertFalse(" filter might not contains " + i, 
                filter.membershipTest(new Key(Integer.toString(i).getBytes())));
          }
           
        } catch(UnsupportedOperationException ex) {
          // not all Filter's implements this method 
          return;
        } finally {
          System.out.println("pass " + FILTER_XOR_STRATEGY + " with " + filter.getClass().getCanonicalName());
          //logger.debug("pass " + FILTER_XOR_STRATEGY + " with " + filter.getClass().getCanonicalName());
        }
      }      
    }),
    
    FILTER_AND_STRATEGY(new FilterTesterStrategy() {      
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId,
          ImmutableSet<Integer> falsePositives) {
        
        int startIntersection = numInsertions - (numInsertions - 100);
        int endIntersection = numInsertions - 100;
        
        Filter partialFilter = getSymmetricFilter(filter.getClass(), numInsertions, hashId);
        
        for (int i = 0; i < numInsertions; i++) {
          String digit = Integer.toString(i);
          filter.add(new Key(digit.getBytes()));
          if (i >= startIntersection && i<= endIntersection ) {
            partialFilter.add(new Key(digit.getBytes()));
          }
        }
       
        //do logic AND
        filter.and(partialFilter);               
        
        for (int i = 0; i < numInsertions; i++) {
          if (i >= startIntersection && i<= endIntersection ) {
            Assert.assertTrue(" filter might contains " + i, 
              filter.membershipTest(new Key(Integer.toString(i).getBytes())));
          }
        }        
        
        System.out.println("pass " + FILTER_AND_STRATEGY + " with " + filter.getClass().getCanonicalName());
        //logger.debug("pass " + FILTER_AND_STRATEGY + " with " + filter.getClass().getCanonicalName());
      }
    }),
    
    FILTER_OR_STRATEGY(new FilterTesterStrategy() {         
      
      @Override
      public void assertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet<Integer> falsePositives) {                 
        Filter evenFilter = getSymmetricFilter(filter.getClass(), numInsertions, hashId);
        
        // add all even
        for (int i = 0; i < numInsertions; i += 2) {
          evenFilter.add(new Key(Integer.toString(i).getBytes()));
        }
        
        // add all odd
        for (int i = 1; i < numInsertions; i += 2) {
          filter.add(new Key(Integer.toString(i).getBytes()));
        }
        
        // union odd with even
        filter.or(evenFilter);
                               
        // check on present all key 
        for (int i = 0; i < numInsertions; i++) {
          Assert.assertTrue(" filter might contains " + i, 
              filter.membershipTest(new Key(Integer.toString(i).getBytes())));
        }
        
        System.out.println("pass " + FILTER_OR_STRATEGY + " with " + filter.getClass().getCanonicalName());
      }        
    });        
    
    private final FilterTesterStrategy testerStrategy;    
    
    BloomFilterTestStrategy(FilterTesterStrategy testerStrategy) {
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

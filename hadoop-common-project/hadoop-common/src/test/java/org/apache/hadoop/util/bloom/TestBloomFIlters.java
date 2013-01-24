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

import java.util.AbstractCollection;
import java.util.Iterator;

import org.apache.hadoop.util.bloom.BloomFilterTester.BloomFilterTestStrategy;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestBloomFIlters {    
  
  @Test
  public void testRetouchedBloomFilterAddFalsePositive() {
    int hashId = Hash.JENKINS_HASH;
    int numInsertions = 1000;
    int hashFunctionNumber = 5;
   
    int bitSize = BloomFilterTester.optimalNumOfBits(numInsertions, 0.03);
    RetouchedBloomFilter filter = new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId);        
    
    filter.addFalsePositive(new AbstractCollection<Key>() {
      final ImmutableList<Key> falsePositive = 
          ImmutableList.<Key>of(new Key("99".getBytes()), new Key("963".getBytes()));
      
      @Override
      public Iterator<Key> iterator() {
        return falsePositive.iterator();
      }

      @Override
      public int size() {
        return falsePositive.size();
      }      
    });
    
    for (int i = 0; i < numInsertions; i+= 2) {
      filter.add(new Key(Integer.toString(i).getBytes()));
    }
    
    filter.selectiveClearing(new Key("99".getBytes()), RemoveScheme.MAXIMUM_FP);
    filter.selectiveClearing(new Key("963".getBytes()), RemoveScheme.MAXIMUM_FP);    
    
    for (int i = 1; i < numInsertions; i+= 2) {
      assertFalse(" testRetouchedBloomFilterAddFalsePositive error " + i, 
          filter.membershipTest(new Key(Integer.toString(i).getBytes())));
    }    
  }  
  
  @Test
  public void testFiltersWithJenkinsHash() {    
    int numInsertions = 1000;
    int hashId = Hash.JENKINS_HASH;    
    int bitSize = BloomFilterTester.optimalNumOfBits(numInsertions, 0.03);
    // for 3%, we always get 5 hash functions
    int hashFunctionNumber = 5;
        
    BloomFilterTester.of(hashId, numInsertions)            
      .withFilterInstance(new BloomFilter(bitSize, hashFunctionNumber, hashId))
      .withFilterInstance(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId))
      .withTestCases(ImmutableSet.of(        
          BloomFilterTestStrategy.KEY_TEST_STRATEGY,
          BloomFilterTestStrategy.ADD_KEYS_STRATEGY,
          BloomFilterTestStrategy.EXCEPTIONS_CHECK_STRATEGY,
          BloomFilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY,          
          BloomFilterTestStrategy.WRITE_READ_STRATEGY,                              
          BloomFilterTestStrategy.FILTER_OR_STRATEGY,                              
          BloomFilterTestStrategy.FILTER_AND_STRATEGY,          
          BloomFilterTestStrategy.FILTER_XOR_STRATEGY                        
          ))
        .test();
  }
  
  @Test
  public void testFiltersWithMurmurHash() {
    int numInsertions = 1000;        
    int hashFunctionNumber = 5;
    int hashId = Hash.MURMUR_HASH;
    
    int bitSize = BloomFilterTester.optimalNumOfBits(numInsertions, 0.03);
    
    BloomFilterTester.of(hashId, numInsertions)
      .withFilterInstance(new BloomFilter(bitSize, hashFunctionNumber, hashId))
      .withFilterInstance(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId))           
      .withTestCases(ImmutableSet.of(
          BloomFilterTestStrategy.KEY_TEST_STRATEGY,
          BloomFilterTestStrategy.ADD_KEYS_STRATEGY,
          BloomFilterTestStrategy.EXCEPTIONS_CHECK_STRATEGY,
          BloomFilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY,          
          BloomFilterTestStrategy.WRITE_READ_STRATEGY,          
          BloomFilterTestStrategy.FILTER_OR_STRATEGY,          
          BloomFilterTestStrategy.FILTER_AND_STRATEGY,          
          BloomFilterTestStrategy.FILTER_XOR_STRATEGY          
          ))
        .test();
  }        
}

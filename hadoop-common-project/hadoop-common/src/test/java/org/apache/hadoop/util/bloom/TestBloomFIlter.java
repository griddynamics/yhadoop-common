package org.apache.hadoop.util.bloom;

import junit.framework.Assert;

import org.apache.hadoop.util.bloom.FilterTester.FilterTestStrategy;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Ignore;
import org.junit.Test;
import com.google.common.collect.ImmutableSet;

public class TestBloomFIlter {  
  
  private static final double LN2 = Math.log(2);
  private static final double LN2_SQUARED = LN2 * LN2;

  static int optimalNumOfBits(int n, double p) {
    return (int) (-n * Math.log(p) / LN2_SQUARED);
  }
  
  @Test
  public void testFiltersWithJenkinsHash() {
    int numInsertions = 1000000;
    int hashId = Hash.JENKINS_HASH;    
    int bitSize = optimalNumOfBits(numInsertions, 0.03);
    // for 3%, we always get 5 hash functions
    int hashFunctionNumber = 5;
    
    FilterTester.of(hashId, numInsertions)            
      .withFilterInstance(new BloomFilter(bitSize, hashFunctionNumber, hashId))     
      .withFilterInstance(new CountingBloomFilter(bitSize, hashFunctionNumber, hashId))      
      //.withFilterInstance(new DynamicBloomFilter(numInsertions * hashFunctionNumber, hashFunctionNumber, Hash.JENKINS_HASH, 2))
      .withTestCases(ImmutableSet.of(
          FilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY,
          FilterTestStrategy.FILTER_OR_STRATEGY, 
          FilterTestStrategy.WRITE_READ_STRATEGY,
          FilterTestStrategy.FILTER_XOR_STRATEGY
          ))
        .test();
  }
  
  @Test
  public void testFiltersWithMurmurHash() {
    int numInsertions = 1000000;        
    int hashFunctionNumber = 5;
    int hashId = Hash.MURMUR_HASH;
    
    int bitSize = optimalNumOfBits(numInsertions, 0.03);
    
    FilterTester.of(hashId, numInsertions)
      .withFilterInstance(new BloomFilter(bitSize, hashFunctionNumber, hashId))
      // can't do this
      //.withFilterInstance(new BloomFilter(numInsertions * hashFunctionNumber, hashFunctionNumber, Hash.JENKINS_HASH))
      .withFilterInstance(new CountingBloomFilter(bitSize, hashFunctionNumber, hashId))      
      .withTestCases(ImmutableSet.of(
          FilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY,
          FilterTestStrategy.FILTER_OR_STRATEGY, 
          FilterTestStrategy.WRITE_READ_STRATEGY,
          FilterTestStrategy.FILTER_XOR_STRATEGY
          ))
        .test();
  }
      
  @Ignore
  public void testBloomFilter() {
    
    int numInsertions = 1000000;        
    int hashFunctionNumber = 5;
    int bitSize = optimalNumOfBits(numInsertions, 0.03);       
    
    int hashType = Hash.MURMUR_HASH;
    BloomFilter evenBloomFilter = new BloomFilter(bitSize, hashFunctionNumber, hashType);                    
    BloomFilter oddBloomFilter = new BloomFilter(bitSize, hashFunctionNumber, hashType);
    
    System.out.println(evenBloomFilter.getVectorSize());
    
    for (int i = 0; i < numInsertions *2; i += 2) {
      evenBloomFilter.add(new Key(Integer.toString(i).getBytes()));
    }
    
    // Assert that the BF "might" have all of the even numbers.
    for (int i = 0; i < numInsertions * 2; i += 2) {
      Assert.assertTrue("BF might contains " + i, evenBloomFilter.membershipTest(new Key(Integer.toString(i).getBytes())));
    } 
    
    // Now we check for known false positives using a set of known false positives.
    // (These are all of the false positives under 2400.)
    ImmutableSet<Integer> falsePositives = ImmutableSet.of(57, 91, 117, 139, 231, 245, 261, 271, 381, 541, 581, 615, 812, 821, 837);            
    
    for (int i = 1; i < 1000; i += 2) { // 500 el
      if (!falsePositives.contains(i)) {        
        Assert.assertFalse("BF should not contain " + i, evenBloomFilter.membershipTest(new Key(Integer.toString(i).getBytes())));
      }
    }
    
    for (int i = 1; i < numInsertions; i += 2) {
      oddBloomFilter.add(new Key(Integer.toString(i).getBytes()));
    }        
    
    evenBloomFilter.or(oddBloomFilter);
    for (int i = 0; i < numInsertions; i += 2) {
      Assert.assertTrue(" OR BF might contains " + i, evenBloomFilter.membershipTest(new Key(Integer.toString(i).getBytes())));
    }                         
  }
}

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
package org.apache.hadoop.mapred;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.junit.Test;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;

/**
 * TestCounters checks the sanity and recoverability of Queue
 */
public class TestMergeSorter {

  @Test
  public void testMergeSorter() throws IOException {
   
    MergeSorter sorter= new MergeSorter();
 /*  sorter.compare(i, j);
sorter.addKeyValue(recordOffset, keyLength, valLength)
sorter.setInputBuffer(buffer);
sorter.configure(conf)
sorter.setInputBuffer(buffer)
*/
    ByteArrayOutputStream bout=new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());
    sorter.addKeyValue(2, 2, 3);
    org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator  iterator= sorter.sort();
    while (iterator.next()){
    ValueBytes bt=  iterator.getValue();
    bt.writeUncompressedBytes(out);
    }
    
    
    System.out.println(""+bout.toString());
    }
  
}

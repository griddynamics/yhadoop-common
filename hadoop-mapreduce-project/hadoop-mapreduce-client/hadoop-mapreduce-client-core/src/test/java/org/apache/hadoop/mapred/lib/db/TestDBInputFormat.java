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

package org.apache.hadoop.mapred.lib.db;


import java.lang.reflect.Field;
import java.sql.Connection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.ConnectionForTest;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestDBInputFormat {

  @Test
  public void testDBInputFormat() throws Exception{
    
    JobConf configuration= new JobConfForTest(); 
    DBInputFormat<NullDBWritable> format = new DBInputFormatForTest();
    format.setConf(configuration);
    DBInputFormat.DBInputSplit splitter= new DBInputFormat.DBInputSplit(1,11);
    Reporter reporter = mock(Reporter.class);
    RecordReader<LongWritable,NullDBWritable> reader  =format.getRecordReader(splitter,configuration,reporter);
    
    assertEquals("org.apache.hadoop.mapred.lib.db.DBInputFormat$DBRecordReaderWrapper",reader.getClass().getName() );
    
    configuration.setInt(MRJobConfig.NUM_MAPS, 3);
    InputSplit[]  lSplits=format.getSplits(configuration, 3);
    assertEquals(5,lSplits[0].getLength());
    assertEquals(3,lSplits.length);
    
    // test reader
    
    assertEquals(LongWritable.class, reader.createKey().getClass());
    assertEquals(0,reader.getPos());
    assertEquals(0,reader.getProgress(),0.001);
    
    
    
  }
  @Test
  public void testSetInput(){
    JobConf configuration= new JobConfForTest(); 

    String[] fieldNames= {"field1","field2"};
    DBInputFormatForTest.setInput(configuration, NullDBWritable.class,
        "table","conditions","orderBy",  fieldNames) ;
    assertEquals("org.apache.hadoop.mapred.lib.db.DBInputFormat$NullDBWritable", configuration.getClass(DBConfiguration.INPUT_CLASS_PROPERTY, null).getName());
    assertEquals("table", configuration.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, null));
    
    String[] fields=configuration.getStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
    assertEquals("field1", fields[0]);
    assertEquals("field2", fields[1]);
    
    assertEquals("conditions", configuration.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY, null));
    assertEquals("orderBy", configuration.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY, null));
    
     configuration= new JobConfForTest(); 
    
    
     DBInputFormatForTest.setInput(configuration, NullDBWritable.class,
        "query", "countQuery") ;
     assertEquals("query", configuration.get(DBConfiguration.INPUT_QUERY, null));
     assertEquals("countQuery", configuration.get(DBConfiguration.INPUT_COUNT_QUERY, null));
    
  } 
  private class DBInputFormatForTest extends DBInputFormat<NullDBWritable>{

    @Override
    public Connection getConnection() {
      Connection result= new ConnectionForTest();
     try {
      Field field= org.apache.hadoop.mapreduce.lib.db.DBInputFormat.class.getDeclaredField("connection");
      field.setAccessible(true);
      field.set(this , result);
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
      
      return result;
    }
    
  }
  
  private class JobConfForTest extends JobConf{
    
  }
}

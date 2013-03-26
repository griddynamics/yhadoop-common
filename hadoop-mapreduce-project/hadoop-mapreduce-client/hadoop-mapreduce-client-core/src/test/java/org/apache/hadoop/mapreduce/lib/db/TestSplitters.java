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

package org.apache.hadoop.mapreduce.lib.db;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Test Splitters. Splitters should build parts of sql sentences for split
 * result.
 * 
 */
public class TestSplitters {
  /**
   * test BooleanSplitter .
   */
  @Test(timeout = 2000)
  public void testBooleanSplitter() throws Exception {
    BooleanSplitter splitter = new BooleanSplitter();
    Configuration configuration = new Configuration();
    ResultSet result = mock(ResultSet.class);
    when(result.getString(1)).thenReturn("result1");
    ByteArrayOutputStream data = new ByteArrayOutputStream();

    List<InputSplit> splits = splitter.split(configuration, result, "column");
    assertEquals(2, splits.size());
    DBInputSplit split = (DBInputSplit) splits.get(0);
    split.write(new DataOutputStream(data));
    assertEquals("column = FALSEcolumn = FALSE", data.toString());
    data.reset();
    split = (DBInputSplit) splits.get(1);
    split.write(new DataOutputStream(data));
    assertEquals("column IS NULLcolumn IS NULL", data.toString());
    data.reset();

    when(result.getString(1)).thenReturn("result1");
    when(result.getString(2)).thenReturn("result2");
    when(result.getBoolean(1)).thenReturn(true);
    when(result.getBoolean(2)).thenReturn(false);

    splits = splitter.split(configuration, result, "column");

    assertEquals(0, splits.size());

    when(result.getString(1)).thenReturn("result1");
    when(result.getString(2)).thenReturn("result2");
    when(result.getBoolean(1)).thenReturn(false);
    when(result.getBoolean(2)).thenReturn(true);

    splits = splitter.split(configuration, result, "column");
    assertEquals(2, splits.size());

    split = (DBInputSplit) splits.get(0);
    split.write(new DataOutputStream(data));
    assertEquals("column = FALSEcolumn = FALSE", data.toString());
    data.reset();
    split = (DBInputSplit) splits.get(1);
    split.write(new DataOutputStream(data));
    assertTrue(data.toString().contains("column = TRUE"));

  }

  /**
   * test FloatSplitter.
   */
  @Test(timeout = 2000)
  public void testFloatSplitter() throws Exception {
    Configuration configuration = new Configuration();
    ResultSet result = mock(ResultSet.class);
    ByteArrayOutputStream data = new ByteArrayOutputStream();

    FloatSplitter splitter = new FloatSplitter();
    List<InputSplit> splits = splitter.split(configuration, result, "column");
    assertEquals(1, splits.size());
    DBInputSplit split = (DBInputSplit) splits.get(0);
    split.write(new DataOutputStream(data));
    assertTrue(data.toString().contains("column IS NULL"));

    when(result.getString(1)).thenReturn("result1");
    when(result.getString(2)).thenReturn("result2");
    when(result.getDouble(1)).thenReturn(5.0);
    when(result.getDouble(2)).thenReturn(7.0);

    splits = splitter.split(configuration, result, "column1");
    assertEquals(1, splits.size());
    split = (DBInputSplit) splits.get(0);
    data.reset();
    split.write(new DataOutputStream(data));
    assertEquals("column1 >= 7.0column1 <= 7.0", data.toString());

  }

  /**
   * test BigDecimalSplitter.
   */
  @Test(timeout = 2000)
  public void testBigDecimalSplitter() throws Exception {

    BigDecimalSplitter splitter = new BigDecimalSplitter();
    Configuration configuration = new Configuration();
    ResultSet result = mock(ResultSet.class);
    ByteArrayOutputStream data = new ByteArrayOutputStream();

    List<InputSplit> splits = splitter.split(configuration, result, "column");
    assertEquals(1, splits.size());
    DBInputSplit split = (DBInputSplit) splits.get(0);
    split.write(new DataOutputStream(data));
    assertTrue(data.toString().contains("column IS NULL"));

    when(result.getString(1)).thenReturn("result1");
    when(result.getString(2)).thenReturn("result2");
    when(result.getBigDecimal(1)).thenReturn(new BigDecimal(10));
    when(result.getBigDecimal(2)).thenReturn(new BigDecimal(11));

    splits = splitter.split(configuration, result, "column1");
    assertEquals(1, splits.size());
    split = (DBInputSplit) splits.get(0);
    data.reset();
    split.write(new DataOutputStream(data));
    assertTrue(data.toString().contains("column1 >= 10"));
    assertTrue(data.toString().contains("column1 <= 11"));

  }

  /**
   * test IntegerSplitter
   */
  @Test(timeout = 2000)
  public void testIntegerSplitter() throws Exception {
    IntegerSplitter splitter = new IntegerSplitter();
    Configuration configuration = new Configuration();
    ResultSet result = mock(ResultSet.class);
    ByteArrayOutputStream data = new ByteArrayOutputStream();

    List<InputSplit> splits = splitter.split(configuration, result, "column");
    assertEquals(1, splits.size());
    DBInputSplit split = (DBInputSplit) splits.get(0);
    split.write(new DataOutputStream(data));
    assertTrue(data.toString().contains("column IS NULL"));

    when(result.getString(1)).thenReturn("result1");
    when(result.getString(2)).thenReturn("result2");
    when(result.getLong(1)).thenReturn(8L);
    when(result.getLong(2)).thenReturn(9L);

    splits = splitter.split(configuration, result, "column1");
    assertEquals(1, splits.size());
    split = (DBInputSplit) splits.get(0);
    data.reset();
    split.write(new DataOutputStream(data));
    assertTrue(data.toString().contains("column1 >= 8"));
    assertTrue(data.toString().contains("column1 <= 9"));
  }

  /**
   * test TextSplitter
   */
  @Test(timeout = 2000)
  public void testTextSplitter() throws Exception {
    TextSplitter splitter = new TextSplitter();
    Configuration configuration = new Configuration();
    ResultSet result = mock(ResultSet.class);
    ByteArrayOutputStream data = new ByteArrayOutputStream();

    List<InputSplit> splits = splitter.split(configuration, result, "column");
    assertEquals(1, splits.size());
    DBInputSplit split = (DBInputSplit) splits.get(0);
    split.write(new DataOutputStream(data));
    assertTrue(data.toString().contains("column IS NULL"));

    when(result.getString(1)).thenReturn("result1");
    when(result.getString(2)).thenReturn("result2");

    splits = splitter.split(configuration, result, "column1");
    assertEquals(1, splits.size());
    split = (DBInputSplit) splits.get(0);
    data.reset();
    split.write(new DataOutputStream(data));
    assertEquals("column1 >= 'result1'column1 <= 'result2'", data.toString());
  }

  /**
   * test OracleDateSplitter.
   */
  @Test(timeout = 2000)
  public void testDBSplitter() throws Exception {
    OracleDataDrivenDBInputFormat<NullDBWritable> format = new OracleDataDrivenDBInputFormatForTest();
    assertEquals(OracleDateSplitter.class, format.getSplitter(Types.TIMESTAMP)
        .getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.INTEGER)
        .getClass());

    Configuration configuration = new Configuration();
    DBInputSplit inputSplit = new DBInputSplit(1, 10);
    RecordReader<LongWritable, NullDBWritable> recorder = format
        .createDBRecordReader(inputSplit, configuration);
    assertEquals(OracleDataDrivenDBRecordReader.class, recorder.getClass());

  }

  @Test(timeout = 2000)
  public void testDateSplitter() throws Exception {
    ByteArrayOutputStream data = new ByteArrayOutputStream();

    DateSplitter splitter = new DateSplitter();
    Configuration configuration = new Configuration();
    configuration.setInt(MRJobConfig.NUM_MAPS, 3);

    ResultSet results = mock(ResultSet.class);
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);
    when(metadata.getColumnType(1)).thenReturn(Types.TIMESTAMP);

    when(results.getMetaData()).thenReturn(metadata);
    when(results.getTimestamp(1)).thenReturn(new Timestamp(Long.MIN_VALUE));
    when(results.getTimestamp(2)).thenReturn(new Timestamp(Long.MIN_VALUE));

    List<InputSplit> splitters = splitter.split(configuration, results,
        "columnName");

    assertEquals(1, splitters.size());
    DBInputSplit split = (DBInputSplit) splitters.get(0);
    split.write(new DataOutputStream(data));

    assertTrue(data.toString().contains("columnName IS NULL"));

    data.reset();
    when(results.getTimestamp(1)).thenReturn(new Timestamp(100));
    when(results.getTimestamp(2)).thenReturn(new Timestamp(200));

    splitters = splitter.split(configuration, results, "columnName");
    assertEquals(4, splitters.size());
    split = (DBInputSplit) splitters.get(0);
    split.write(new DataOutputStream(data));
    assertEquals(
        "%columnName >= '1970-01-01 03:00:00.1'&columnName < '1970-01-01 03:00:00.133'",
        data.toString());

    when(metadata.getColumnType(1)).thenReturn(Types.DATE);

    when(results.getDate(1)).thenReturn(new Date(100));
    when(results.getDate(2)).thenReturn(new Date(200));

    splitters = splitter.split(configuration, results, "columnName");
    assertEquals(4, splitters.size());
    split = (DBInputSplit) splitters.get(0);
    split.write(new DataOutputStream(data));
    assertEquals(
        "%columnName >= '1970-01-01 03:00:00.1'&columnName < '1970-01-01 03:00:00.133'columnName " +
        ">= '1970-01-01'columnName < '1970-01-01'",
        data.toString());

    when(metadata.getColumnType(1)).thenReturn(Types.TIME);

    when(results.getTime(1)).thenReturn(new Time(100));
    when(results.getTime(2)).thenReturn(new Time(200));

    splitters = splitter.split(configuration, results, "columnName");
    assertEquals(4, splitters.size());
    split = (DBInputSplit) splitters.get(0);
    split.write(new DataOutputStream(data));
    assertEquals(
        "%columnName >= '1970-01-01 03:00:00.1'&columnName < '1970-01-01 03:00:00.133'columnName " +
        ">= '1970-01-01'columnName < '1970-01-01'columnName >= '03:00:00'columnName < '03:00:00'",
        data.toString());

  }

  private class OracleDataDrivenDBInputFormatForTest extends
      OracleDataDrivenDBInputFormat<NullDBWritable> {

    @Override
    public DBConfiguration getDBConf() {

      DBConfiguration result = new DBConfiguration(new Configuration());
      result.setInputClass(NullDBWritable.class);
      return result;
    }

    @Override
    public Connection getConnection() {

      return new ConnectionForTest();
    }

  }
}

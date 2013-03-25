package org.apache.hadoop.mapreduce.lib.db;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit;
import org.junit.Test;
import static org.junit.Assert.*; 
import static org.mockito.Mockito.*;

public class TestDbClasses {

  @Test
  public void testDataDrivenDBInputFormatSplitter(){
    DataDrivenDBInputFormat<NullDBWritable> format = new DataDrivenDBInputFormat<NullDBWritable>();
    assertEquals(BigDecimalSplitter.class, format.getSplitter(Types.DECIMAL).getClass());
    assertEquals(BigDecimalSplitter.class, format.getSplitter(Types.NUMERIC).getClass());
    assertEquals(BooleanSplitter.class, format.getSplitter(Types.BOOLEAN).getClass());
    assertEquals(BooleanSplitter.class, format.getSplitter(Types.BIT).getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.BIGINT).getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.TINYINT).getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.SMALLINT).getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.INTEGER).getClass());
    assertEquals(FloatSplitter.class, format.getSplitter(Types.DOUBLE).getClass());
    assertEquals(FloatSplitter.class, format.getSplitter(Types.REAL).getClass());
    assertEquals(FloatSplitter.class, format.getSplitter(Types.FLOAT).getClass());
    assertEquals(TextSplitter.class, format.getSplitter(Types.LONGVARCHAR).getClass());
    assertEquals(TextSplitter.class, format.getSplitter(Types.CHAR).getClass());
    assertEquals(TextSplitter.class, format.getSplitter(Types.VARCHAR).getClass());
    assertEquals(DateSplitter.class, format.getSplitter(Types.TIMESTAMP).getClass());
    assertEquals(DateSplitter.class, format.getSplitter(Types.DATE).getClass());
    assertEquals(DateSplitter.class, format.getSplitter(Types.TIME).getClass());
    assertNull( format.getSplitter(Types.BINARY));
   
    
  }
  @Test
  public void testDataDrivenDBInputFormat() throws Exception{
    ByteArrayOutputStream data= new ByteArrayOutputStream();

    JobContext jobContext = mock(JobContext.class);
    Configuration configuration = new Configuration();
    
    when(jobContext.getConfiguration()).thenReturn(configuration);
    DataDrivenDBInputFormat<NullDBWritable> format = new DataDrivenDBInputFormat<NullDBWritable>();
    List<InputSplit> stplits =format.getSplits(jobContext);
    assertEquals(1, stplits.size());
    DataDrivenDBInputSplit split= (DataDrivenDBInputSplit)stplits.get(0);
    split.write(new DataOutputStream(data));
    assertEquals("1=11=1", data.toString());
    
    //2 
    configuration.setInt(MRJobConfig.NUM_MAPS, 2);
    
    DataDrivenDBInputFormat.setBoundingQuery(configuration, "query");
    assertEquals( "query",configuration.get(DBConfiguration.INPUT_BOUNDING_QUERY));
    
    Job job = mock(Job.class);
    when(job.getConfiguration()).thenReturn(configuration);
    DataDrivenDBInputFormat.setInput(job, NullDBWritable.class, "query", "Bounding Query");
    
    assertEquals("Bounding Query", configuration.get(DBConfiguration.INPUT_BOUNDING_QUERY));
  }
  
  @Test
  public void testOracleDataDrivenDBInputFormat() throws Exception{
    
    Configuration configuration = new Configuration();
    OracleDataDrivenDBInputFormat<NullDBWritable> format = new OracleDataDrivenDBInputFormatForTest();
    assertEquals(OracleDateSplitter.class, format.getSplitter(Types.TIMESTAMP).getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.INTEGER).getClass());
    
    assertEquals(OracleDataDrivenDBRecordReader.class, format.createDBRecordReader(new DBInputFormat.DBInputSplit(1,10),configuration).getClass());
  }
  
  private class OracleDataDrivenDBInputFormatForTest extends OracleDataDrivenDBInputFormat<NullDBWritable>{

    @Override
    public DBConfiguration getDBConf() {
      
      String[] names={"field1","field2"};
      DBConfiguration result = mock(DBConfiguration.class);
//      when(result.getInputClass()).thenReturn( (Class<?>) NullDBWritable.class);
      when(result.getInputConditions()).thenReturn("conditions");
      when(result.getInputFieldNames()).thenReturn(names);
      when(result.getInputTableName()).thenReturn("table");
      return result;
    }

    @Override
    public Connection getConnection() {
      return new ConnectionForTest();
    }
    
    
  }
}

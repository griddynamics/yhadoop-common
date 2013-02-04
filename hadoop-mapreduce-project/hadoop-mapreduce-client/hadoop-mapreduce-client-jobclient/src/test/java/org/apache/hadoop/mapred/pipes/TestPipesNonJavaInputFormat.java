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
package org.apache.hadoop.mapred.pipes;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.pipes.TestPipeApplication.FakeSplit;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestPipesNonJavaInputFormat {
  private static File workSpace = new File("target",
          TestPipesNonJavaInputFormat.class.getName() + "-workSpace");

  // test PipesNonJavaInputFormat
  @Test
  public void testFormat() throws IOException {

    PipesNonJavaInputFormat inputFormat = new PipesNonJavaInputFormat();
    JobConf conf = new JobConf();

    RecordReader<FloatWritable, NullWritable> reader = inputFormat
            .getRecordReader(new FakeSplit(), conf, new TestTaskReporter());
    assertEquals(0.0f, reader.getProgress(), 0.001);

    // input and output files
    File input1 = new File(workSpace + File.separator + "input1");
    if (!input1.getParentFile().exists()) {
      input1.getParentFile().mkdirs();
    }

    if (!input1.exists()) {
      input1.createNewFile();
    }

    File input2 = new File(workSpace + File.separator + "input2");
    if (!input2.exists()) {
      input2.createNewFile();
    }
// set data for splits
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
            input1.getAbsolutePath() + "," + input2.getAbsolutePath());
    InputSplit[] splits = inputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);

    PipesNonJavaInputFormat.PipesDummyRecordReader dummyRecordReader = new PipesNonJavaInputFormat.PipesDummyRecordReader(
            conf, splits[0]);
    assertNull(dummyRecordReader.createKey());
    assertNull(dummyRecordReader.createValue());
    assertEquals(0, dummyRecordReader.getPos());

    assertEquals(0.0, dummyRecordReader.getProgress(), 0.001);
    assertTrue(dummyRecordReader.next(new FloatWritable(2.0f), NullWritable.get()));
    assertEquals(2.0, dummyRecordReader.getProgress(), 0.001);
    dummyRecordReader.close();
  }

  private class TestTaskReporter implements Reporter {
    private int recordNum = 0; // number of records processed
    private String status = null;
    private Counters counters = new Counters();
    private InputSplit split = new FakeSplit();

    @Override
    public void progress() {

      recordNum++;
    }

    @Override
    public void setStatus(String status) {
      this.status = status;

    }

    @SuppressWarnings("unused")
    public String getStatus() {
      return this.status;

    }

    public Counters.Counter getCounter(String group, String name) {
      Counters.Counter counter = null;
      if (counters != null) {
        counter = counters.findCounter(group, name);
        if (counter == null) {
          Group grp = counters.addGroup(group, group);
          counter = grp.addCounter(name, name, 10);
        }
      }
      return counter;
    }

    public Counters.Counter getCounter(Enum<?> name) {
      return counters == null ? null : counters.findCounter(name);
    }

    public void incrCounter(Enum<?> key, long amount) {
      if (counters != null) {
        counters.incrCounter(key, amount);
      }
    }

    public void incrCounter(String group, String counter, long amount) {

      if (counters != null) {
        counters.incrCounter(group, counter, amount);
      }

    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return split;
    }

    @Override
    public float getProgress() {
      return recordNum;
    }

  }
}

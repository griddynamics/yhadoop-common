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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * NLineInputFormat which splits N lines of input as one split.
 *
 * In many "pleasantly" parallel applications, each process/mapper 
 * processes the same input file (s), but with computations are 
 * controlled by different parameters.(Referred to as "parameter sweeps").
 * One way to achieve this, is to specify a set of parameters 
 * (one set per line) as input in a control file 
 * (which is the input path to the map-reduce application,
 * where as the input dataset is specified 
 * via a config variable in JobConf.).
 * 
 * The NLineInputFormat can be used in such applications, that splits 
 * the input file such that by default, one line is fed as
 * a value to one map task, and key is the offset.
 * i.e. (k,v) is (LongWritable, Text).
 * The location hints will span the whole mapred cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NLineInputFormat extends FileInputFormat<LongWritable, Text> 
                              implements JobConfigurable { 
  private int N = 1;

  public RecordReader<LongWritable, Text> getRecordReader(
                                            InputSplit genericSplit,
                                            JobConf job,
                                            Reporter reporter) 
  throws IOException {
    reporter.setStatus(genericSplit.toString());
    return new LineRecordReader(job, (FileSplit) genericSplit);
  }

  /** 
   * Logically splits the set of input files for the job, splits N lines
   * of the input as one split.
   * 
   * @see org.apache.hadoop.mapred.FileInputFormat#getSplits(JobConf, int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits)
  throws IOException {
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
    for (FileStatus status : listStatus(job)) {
      for (org.apache.hadoop.mapreduce.lib.input.FileSplit split : 
          org.apache.hadoop.mapreduce.lib.input.
          NLineInputFormat.getSplitsForFile(status, job, N)) {
        splits.add(new FileSplit(split));
      }
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  public void configure(JobConf conf) {
    N = conf.getInt("mapreduce.input.lineinputformat.linespermap", 1);
  }
  
  /**
   * NLineInputFormat uses LineRecordReader, which always reads
   * (and consumes) at least one character out of its upper split
   * boundary. So to make sure that each mapper gets N lines, we
   * move back the upper split limits of each split 
   * by one character here.
   * @param fileName  Path of file
   * @param begin  the position of the first byte in the file to process
   * @param length  number of bytes in InputSplit
   * @return  FileSplit
   */
  protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
    return (begin == 0) 
    ? new FileSplit(fileName, begin, length - 1, new String[] {})
    : new FileSplit(fileName, begin - 1, length, new String[] {});
  }
}

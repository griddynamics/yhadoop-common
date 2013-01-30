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
package org.apache.hadoop.metrics2.lib;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.annotation.Metric.Type;
import org.apache.hadoop.metrics2.lib.MetricsServlet2.TagsMetricsPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestMetricsServlet2 {

  // The 2 sample metric classes:
  @Metrics(name="testRecord", context="test1")
  static class MyMetrics1 {
    @Metric(value={"testTag1", ""}, type=Type.TAG) 
    String testTag1() { return "testTagValue1"; }
    
    @Metric(value={"testTag2", ""}, type=Type.TAG) 
    String gettestTag2() { return "testTagValue2"; }
    
    @Metric({"testMetric1", "An integer gauge"}) 
    MutableGaugeInt testMetric1;
    
    @Metric({"testMetric2", "An integer gauge"}) 
    MutableGaugeInt testMetric2;

    public MyMetrics1 registerWith(MetricsSystem ms) {
      return ms.register("m1", null, this);
    }
  }
  
  @Metrics(name="testRecord", context="test2")
  static class MyMetrics2 {
    
    @Metric(value={"testTag22", ""}, type=Type.TAG) 
    String testTag1() { return "testTagValue22"; }

    public MyMetrics2 registerWith(MetricsSystem ms) {
      return ms.register("m2", null, this);
    }
  }
  
  private MyMetrics1 myMetrics1; 
  private MyMetrics2 myMetrics2;
  private final MetricsServlet2 metricsServlet2 = new MetricsServlet2() {
    @Override
    protected ServletSink createServletSink() {
      return new ServletSink() {
        @Override
        public void putMetrics(MetricsRecord record) {
          // NB: ignore the default metricssystem context metrics for the test: 
          if ("metricssystem".equals(record.context())) {
            return;
          }
          super.putMetrics(record);
        }
      };
    }
  };
  
  /**
   * Initializes, for testing, two NoEmitMetricsContext's, and adds one value 
   * to the first of them.
   */
  @Before
  public void before() throws IOException {
    final MetricsSystem ms = DefaultMetricsSystem.instance();
    
    myMetrics1 = new MyMetrics1().registerWith(ms);
    myMetrics1.testMetric1.set(1);
    myMetrics1.testMetric2.set(33);
    
    myMetrics2 = new MyMetrics2().registerWith(ms);
  }

  @After
  public void after() {
    // NB: we need complete cleanup in order to 
    // register all the same metric sources again:
    DefaultMetricsSystem.shutdown();
  }
  
  @Test
  public void testSeveralServletInstances() {
    // see if we can create another instance of the servlet,
    // and that does not break anything:
    MetricsServlet2 servlet2 = new MetricsServlet2(); 
  }
  
  @Test
  public void testTagsMetricsPair() throws IOException {
    Map<String, Map<String, List<TagsMetricsPair>>> m = metricsServlet2.makeMap();
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);
    metricsServlet2.printJson(out, m);
    out.close();
    final String actualJson = sw.toString();
    assertEquals(
        "{\"test1\":{\"testRecord\":[[{\"testTag1\":\"testTagValue1\"," +
        "\"testTag2\":\"testTagValue2\"},{\"testMetric1\":1," +
        "\"testMetric2\":33}]]}," +
        "\"test2\":{\"testRecord\":" +
        "[[{\"testTag22\":\"testTagValue22\"},{}]]}}", actualJson);
  }
  
  @Test
  public void testGetMap() throws IOException {
    Map<String, Map<String, List<TagsMetricsPair>>> m = metricsServlet2.makeMap();
    assertEquals("Map missing contexts", 2, m.size());
    assertTrue(m.containsKey("test1"));
   
    Map<String, List<TagsMetricsPair>> m2 = m.get("test1");
    
    assertEquals("Missing records", 1, m2.size());
    assertTrue(m2.containsKey("testRecord"));
    assertEquals("Wrong number of tags-values pairs.", 1, m2.get("testRecord").size());
  }
  
  @Test
  public void testPrintMap() throws IOException {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);
    metricsServlet2.printMap(out, metricsServlet2.makeMap());
    out.close();
    final String actual = sw.toString();
    String EXPECTED = "" +
      "test1\n" +
      "  testRecord\n" +
      "    {testTag1=testTagValue1,testTag2=testTagValue2}:\n" +
      "      testMetric1=1\n" +
      "      testMetric2=33\n" +
      "test2\n" +
      "  testRecord\n"+
      "    {testTag22=testTagValue22}:\n";
    assertEquals(EXPECTED, actual);
  }
}

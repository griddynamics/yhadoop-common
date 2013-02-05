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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
    
    @Metric(value={"testMetric1", "An integer gauge"},always=true) 
    MutableGaugeInt testMetric1;
    
    @Metric(value={"testMetric2", "An integer gauge"},always=true) 
    MutableGaugeInt testMetric2;

    public MyMetrics1 registerWith(MetricsSystem ms) {
      return ms.register("m1", null, this);
    }
  }
  
  @Metrics(name="testRecord", context="test2")
  static class MyMetrics2 {
    // NB: new metrics system does not allow @Metrics classes
    // without any tag or metric inside, so, let's put 
    // a tag there: 
    @Metric(value={"testTag22", ""}, type=Type.TAG) 
    String testTag1() { return "testTagValue22"; }

    public MyMetrics2 registerWith(MetricsSystem ms) {
      return ms.register("m2", null, this);
    }
  }
  
  private static class MetricsServlet2NoMetricsSystemContext 
    extends MetricsServlet2 {
    @Override
    protected ServletSink createServletSink() {
      return new ServletSink(true) {
        @Override
        public void putMetrics(MetricsRecord record) {
          // NB: ignore the default metricssystem context metrics for the test: 
          if (!"metricssystem".equals(record.context())) {
            super.putMetrics(record);
          }
        }
      };
    }
  };
  
  private static MyMetrics1 myMetrics1; 
  private static MyMetrics2 myMetrics2;
  private final MetricsServlet2 metricsServlet2 
    = new MetricsServlet2NoMetricsSystemContext();
  
  /**
   * Initializes, for testing, two NoEmitMetricsContext's, and adds one value 
   * to the first of them.
   */
  @BeforeClass
  public static void beforeClass() {
    final MetricsSystem ms = DefaultMetricsSystem.initialize("servlettest");
    
    myMetrics1 = new MyMetrics1().registerWith(ms);
    myMetrics1.testMetric1.set(1);
    myMetrics1.testMetric2.set(33);
    
    myMetrics2 = new MyMetrics2().registerWith(ms);
  }
  
  private void changeMetricsImpl() {
    myMetrics1.testMetric1.incr();
    myMetrics1.testMetric1.decr();
    
    myMetrics1.testMetric2.incr();
    myMetrics1.testMetric2.decr();
  }
  
  @Before
  public void before() {
    // NB: need to change the metrics, see HADOOP-9269:
    changeMetricsImpl();
  }
  
  @AfterClass
  public static void afterClass() {
    DefaultMetricsSystem.shutdown();
  }
  
  @Test
  public void testGetMap() {
    Map<String, Map<String, List<TagsMetricsPair>>> m = metricsServlet2.makeMap();
    assertEquals("Map missing contexts", 2, m.size());
    assertTrue(m.containsKey("test1"));
   
    Map<String, List<TagsMetricsPair>> m2 = m.get("test1");
    
    assertEquals("Missing records", 1, m2.size());
    assertTrue(m2.containsKey("testRecord"));
    assertEquals("Wrong number of tags-values pairs.", 1, m2.get("testRecord").size());
    
    TagsMetricsPair pair = m2.get("testRecord").get(0);
    assertEquals(2, pair.metricMap.size());
  }
  
  @Test
  public void testPrintMap() {
    testPrintMapImpl(metricsServlet2);
  }
  
  private void testPrintMapImpl(final MetricsServlet2 servlet) {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);
    servlet.printMap(out, servlet.makeMap());
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
  
  @Test
  public void testPrintJson() {
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

  /*
   * See if we can create another instance of the servlet,
   * and both the instances will work correctly.
   */
  @Test
  public void testSeveralMetricsServlet2Instances() {
    MetricsServlet2 metricsServlet22 
      = new MetricsServlet2NoMetricsSystemContext();
    // NB: need to change the metrics (see HADOOP-9269)
    before();
    testPrintMapImpl(metricsServlet2);
    testPrintMapImpl(metricsServlet22);
    
    // change again:
    before();
    // (this time sinks are queried in reversed order):
    testPrintMapImpl(metricsServlet22);
    testPrintMapImpl(metricsServlet2);
  }
}

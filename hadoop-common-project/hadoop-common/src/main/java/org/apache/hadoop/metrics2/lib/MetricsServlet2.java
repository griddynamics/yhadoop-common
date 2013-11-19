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
import java.io.Serializable;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.mortbay.util.ajax.JSON;
import org.mortbay.util.ajax.JSON.Output;

import com.google.common.annotations.VisibleForTesting;

/**
 * A servlet to print out metrics data. By default, the servlet returns a 
 * textual representation (no promises are made for parseability), and
 * users can use "?format=json" for parseable output.
 * 
 * This implementation is a re-implemented version of 
 * org.apache.hadoop.metrics.MetricsServlet that uses 
 * the new metrics API (org.apache.hadoop.metrics2). 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetricsServlet2 extends HttpServlet {

  private static final MetricsSystem metricsSystem 
    = DefaultMetricsSystem.initialize("MetricsServlet2");
  
  private final ServletSink servletSink;
  
  public MetricsServlet2() {
    servletSink = createServletSink();
    final String sinkName = servletSink.getSinkName(); 
    metricsSystem.register(sinkName, sinkName, servletSink);
  }

  /*
   * This method is visible for testing, but may be re-defined also for 
   * another need.
   */
  @VisibleForTesting
  protected ServletSink createServletSink() {
    return new ServletSink(true);
  }
  
  protected static class ServletSink implements MetricsSink, Serializable {
    protected final boolean filterOutMetricsSystemTags;
    private static int numInstances = 0;
    private final String sinkName;
    // map to accumulate the metrics data:
    private Map<String, Map<String, List<TagsMetricsPair>>> metricsMap;
    
    public ServletSink(boolean filterOutMsTags) {
      numInstances++;
      filterOutMetricsSystemTags = filterOutMsTags;
      // NB: register sinks with unique names to avoid collisions,
      // because several instances of this servlet *may* in principle
      // be created:
      sinkName = "MetricsServlet2-Sink-"+numInstances;
      clear();
    }
    
    protected final String getSinkName() {
      return sinkName;
    } 
    
    @Override
    public void init(SubsetConfiguration conf) {
      // noop
    }
    
    /*
     * Collects all metric data, and returns a map:
     *   contextName -> recordName -> [ (tag->tagValue), (metric->metricValue) ].
     * The values are either String or Number.  The final value is implemented
     * as a list of TagsMetricsPair.
     */
    @Override
    public synchronized void putMetrics(final MetricsRecord record) {
      final String recordContext = record.context();
      final String recordName = record.name();

      Map<String, List<TagsMetricsPair>> records = metricsMap.get(recordContext);
      if (records == null) {
        records = new TreeMap<String, List<TagsMetricsPair>>();
        metricsMap.put(recordContext, records);
      }
     
      List<TagsMetricsPair> tagsMetricsPairList = records.get(recordName);
      if (tagsMetricsPairList == null) {
        final TreeMap<String,String> tagMap = new TreeMap<String,String>();
        final TreeMap<String,Number> metricMap = new TreeMap<String,Number>();
        // NB: ordinary List would grow infinitely, so we use singleton list there.
        // However, List interface is still needed to provide the 
        // expected JSON serialization:
        tagsMetricsPairList = Collections.singletonList(new TagsMetricsPair(tagMap, metricMap));
        records.put(recordName, tagsMetricsPairList);
      }
      
      for (final MetricsTag metricsTag: record.tags()) {
        // NB: may ignore pre-defined tags (like "Context" or "Hostname")  
        // to provide backwards compatibility with the
        // legacy servlet:
        if (!filterOutMetricsSystemTags  
            || metricsTag.info().getClass() != MsInfo.class) {
          String tagValue = metricsTag.value();
          if (tagValue == null) {
            tagValue = "";
          }
          tagsMetricsPairList.get(0).tagMap.put(metricsTag.name(), tagValue);
        }
      }
      
      for (AbstractMetric metric: record.metrics()) {
        tagsMetricsPairList.get(0).metricMap.put(metric.name(), metric.value());
      }
    }
    
    protected synchronized final Map<String, Map<String, List<TagsMetricsPair>>> getMetricsMap() {
      final Map<String, Map<String, List<TagsMetricsPair>>> result = metricsMap;
      clear();
      return result;
    } 
     
    @Override
    public void flush() {
      // noop
    }
    
    /*
     * clears the data stored in the sink
     */
    protected synchronized void clear() {
      // re-create the metrics accumulator:
      metricsMap = new TreeMap<String, Map<String, List<TagsMetricsPair>>>();
    }
  }
  
  /**
   * A helper class to hold a TagMap and MetricMap.
   */
  static class TagsMetricsPair implements JSON.Convertible {
    final TreeMap<String,String> tagMap;
    final TreeMap<String,Number> metricMap;
    
    public TagsMetricsPair(TreeMap<String,String> tagMap, TreeMap<String,Number> metricMap) {
      this.tagMap = tagMap;
      this.metricMap = metricMap;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void fromJSON(Map map) {
      throw new UnsupportedOperationException();
    }

    /** Converts to JSON by providing an array. */
    @Override
    public void toJSON(Output out) {
      out.add(new Object[] { tagMap, metricMap });
    }
  }
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    // Do the authorization
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                   request, response)) {
      return;
    }
    
    final Map<String, Map<String, List<TagsMetricsPair>>> metricsMap = 
        makeMap();

    final String format = request.getParameter("format");
    final PrintWriter out = response.getWriter();
    if ("json".equals(format)) {
      response.setContentType("application/json; charset=utf-8");
      try {
        printJson(out, metricsMap);
      } finally {
        out.close();
      }
    } else {
      try {
        printMap(out, metricsMap);
      } finally {
        out.close();
      }
    }
  }

  @VisibleForTesting
  Map<String, Map<String, List<TagsMetricsPair>>> makeMap() {
    // drop the metrics to sinks:   
    metricsSystem.publishMetricsNow();
    // take the collected metrics data:
    final Map<String, Map<String, List<TagsMetricsPair>>> metricsMap 
      = servletSink.getMetricsMap();
    return metricsMap;
  }
  
  @VisibleForTesting
  void printJson(PrintWriter out, Map<String, Map<String, List<TagsMetricsPair>>> metricsMap) {
     // Uses Jetty's built-in JSON support to convert the map into JSON.
     out.print(new JSON().toJSON(metricsMap));
  }
  
  /**
   * Prints metrics data in a multi-line text form.
   */
  @VisibleForTesting
  void printMap(PrintWriter out, Map<String, Map<String, List<TagsMetricsPair>>> map) {
    for (Map.Entry<String, Map<String, List<TagsMetricsPair>>> context : map.entrySet()) {
      out.println(context.getKey());
      for (Map.Entry<String, List<TagsMetricsPair>> record : context.getValue().entrySet()) {
        indent(out, 1);
        out.println(record.getKey());
        for (TagsMetricsPair pair : record.getValue()) {
          indent(out, 2);
          // Prints tag values in the form "{key=value,key=value}:"
          out.print("{");
          boolean first = true;
          for (Map.Entry<String, String> tagValue : pair.tagMap.entrySet()) {
            if (first) {
              first = false;
            } else {
              out.print(",");
            }
            out.print(tagValue.getKey());
            out.print("=");
            out.print(tagValue.getValue().toString());
          }
          out.println("}:");
          
          // Now print metric values, one per line
          for (Map.Entry<String, Number> metricValue : 
              pair.metricMap.entrySet()) {
            indent(out, 3);
            out.print(metricValue.getKey());
            out.print("=");
            out.println(metricValue.getValue().toString());
          }
        }
      }
    }    
  }
  
  private void indent(PrintWriter out, int indent) {
    for (int i = 0; i < indent; ++i) {
      out.append("  ");
    }
  }
}

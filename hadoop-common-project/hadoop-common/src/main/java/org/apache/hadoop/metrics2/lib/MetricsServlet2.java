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
import java.util.ArrayList;
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
import org.mortbay.util.ajax.JSON;
import org.mortbay.util.ajax.JSON.Output;

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

  private final MetricsSystem metricsSystem;
  private final ServletSink servletSink;
  
  public MetricsServlet2() {
    final MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.init("");
    metricsSystem = ms;
    servletSink = new ServletSink();
    metricsSystem.register("MetricsServlet", "Metrics Servlet", servletSink);
  }
  
  private static class ServletSink implements MetricsSink {
    // metrics data:
    private final Map<String, Map<String, List<TagsMetricsPair>>> metricsMap = 
        new TreeMap<String, Map<String, List<TagsMetricsPair>>>();
    
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
    public void putMetrics(final MetricsRecord record) {
      final String recordContext = record.context();
      final String recordName = record.name();
      
      Map<String, List<TagsMetricsPair>> records = metricsMap.get(recordContext);
      if (records == null) {
        records = new TreeMap<String, List<TagsMetricsPair>>();
        metricsMap.put(recordContext, records);
      }
     
      List<TagsMetricsPair> metricsAndTagsList = records.get(recordName);
      if (metricsAndTagsList == null) {
        metricsAndTagsList = new ArrayList<TagsMetricsPair>();
        records.put(recordName, metricsAndTagsList);
      }
      
      final TreeMap<String,String> tagMap = new TreeMap<String,String>();
      for (final MetricsTag metricsTag: record.tags()) {
        String tagValue = metricsTag.value();
        if (tagValue == null) {
          tagValue = "";
        }
        tagMap.put(metricsTag.name(), tagValue);
      }
      
      final TreeMap<String,Number> metricMap = new TreeMap<String,Number>();
      for (AbstractMetric metric: record.metrics()) {
        metricMap.put(metric.name(), metric.value());
      }
      
      metricsAndTagsList.add(new TagsMetricsPair(tagMap, metricMap));
    }
    
    Map<String, Map<String, List<TagsMetricsPair>>> getMetricsMap() {
      return metricsMap;
    } 
     
    @Override
    public void flush() {
      // noop
    }
    /*
     * clears the data stored in the sink
     */
    void clear() {
      metricsMap.clear();
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

    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                   request, response)) {
      return;
    }

    // clear the data stored in the sink:
    servletSink.clear();
    // drop the metrics to sinks:   
    metricsSystem.publishMetricsNow();
    // take the collected metrics data:
    final Map<String, Map<String, List<TagsMetricsPair>>> metricsMap 
      = servletSink.getMetricsMap();

    final String format = request.getParameter("format");
    if ("json".equals(format)) {
      response.setContentType("application/json; charset=utf-8");
      PrintWriter out = response.getWriter();
      try {
        // Uses Jetty's built-in JSON support to convert the map into JSON.
        out.print(new JSON().toJSON(metricsMap));
      } finally {
        out.close();
      }
    } else {
      PrintWriter out = response.getWriter();
      try {
        printMap(out, metricsMap);
      } finally {
        out.close();
      }
    }
    
    servletSink.clear();
  }
  
  /**
   * Prints metrics data in a multi-line text form.
   */
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

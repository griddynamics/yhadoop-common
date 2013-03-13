package org.apache.hadoop.yarn.webapp.log;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;

public class AggregatedLogsBlockForTest extends AggregatedLogsBlock {

  final private Map<String, String> params = new HashMap<String, String>();
  private HttpServletRequest request;
  public AggregatedLogsBlockForTest(Configuration conf) {
    super(conf);
  }

  @Override
  public void render(Block html) {
    super.render(html);
  }

  public Map<String, String> moreParams() {
    return params;
  }

  public HttpServletRequest request() {
    return request;
  }
  public  void setRequest(HttpServletRequest request) {
     this.request=request;
  }

}

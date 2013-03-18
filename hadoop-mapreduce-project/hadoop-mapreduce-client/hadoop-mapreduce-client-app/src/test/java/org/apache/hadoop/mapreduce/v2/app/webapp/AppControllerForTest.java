package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.View;
import static org.mockito.Mockito.*;

public class AppControllerForTest extends AppController{
//  private Map<String,String> properties;
  final  private static Map<String,String>  properties = new HashMap<String, String>();
  
  private ResponseInfo responceInfo= new ResponseInfo();
  private View view= new ViewForTest();
  private Class<? extends Object>  clazz;

  protected AppControllerForTest(App app, Configuration conf, RequestContext ctx) {
    super(app, conf, ctx);
  }
  
  public Class<? extends Object> getClazz(){
    return clazz;
  }
  @SuppressWarnings("unchecked")
  public <T> T getInstance(Class<T> cls) {
    clazz=cls;
    if(cls.equals(ResponseInfo.class)){
      return (T) responceInfo;
    }
    return (T) view;
  }
  public ResponseInfo getResponceInfo() {
    return responceInfo;
  }
  public void setResponceInfo(ResponseInfo responceInfo) {
    this.responceInfo = responceInfo;
  }
  public View getView() {
    return view;
  }
  public void setView(View view) {
    this.view = view;
  }
  
  public String get(String key, String defaultValue) {
    String result =properties.get(key);
    if(result==null){
      result=defaultValue;
    }
    return result;
  }
  public void set(String key, String value) {
    properties.put(key, value);
  }

  public HttpServletRequest request() { 
    HttpServletRequest result= mock(HttpServletRequest.class);
    when(result.getRemoteUser()).thenReturn("user");
    
    
    return result; 
    }
  
  
  
  @Override
  public HttpServletResponse response() {
    if(responce==null){
     responce =mock(HttpServletResponse.class);
    }
    return responce;
  }
  private HttpServletResponse responce;
  public  Map<String,String> getProperty(){
    return properties;
  }
  
  OutputStream data= new ByteArrayOutputStream();
  PrintWriter writer= new PrintWriter(data);
  public String getData(){
    writer.flush();
    return data.toString();
  }
  protected PrintWriter writer() {
    if(writer==null){
     writer= new PrintWriter(data);
    }
    return writer;
  }
}

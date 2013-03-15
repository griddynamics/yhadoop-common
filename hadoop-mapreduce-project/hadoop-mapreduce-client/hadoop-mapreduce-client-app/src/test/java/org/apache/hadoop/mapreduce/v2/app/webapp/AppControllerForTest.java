package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.View;
import org.glassfish.grizzly.servlet.HttpServletRequestImpl;
import org.mockito.Mockito;

public class AppControllerForTest extends AppController{

  protected AppControllerForTest(App app, Configuration conf, RequestContext ctx) {
    super(app, conf, ctx);
  }
  private ResponseInfo responceInfo= new ResponseInfo();
  private View view= new ViewForTest();
  final private Map<String,String> property= new HashMap<String, String>();
  private Class<? extends Object>  clazz;
  
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
    String result =property.get(key);
    if(result==null){
      result=defaultValue;
    }
    return result;
  }
  public HttpServletRequest request() { 
    HttpServletRequest result= Mockito.mock(HttpServletRequest.class);
    Mockito.when(result.getRemoteUser()).thenReturn("user");
    
    
    return result; 
    }
  
  
  
  @Override
  public HttpServletResponse response() {
    return super.response();
  }
  public  Map<String,String> getProperty(){
    return property;
  }
}

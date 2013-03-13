package org.apache.hadoop.yarn.webapp.view;

public class HtmlBlockForTest extends HtmlBlock{

  @Override
  protected void render(Block html) {
    info("test!");
    
  }

}

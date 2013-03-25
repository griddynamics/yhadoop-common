package org.apache.hadoop.mapred.lib.db;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.lib.db.ConnectionForTest;

public class DriverForTest implements Driver{

  @Override
  public boolean acceptsURL(String arg0) throws SQLException {
    return "testUrl".equals(arg0);
  }

  @Override
  public Connection connect(String arg0, Properties arg1) throws SQLException {
    
    return new ConnectionForTest();
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
      throws SQLException {

    return null;
  }

  @Override
  public boolean jdbcCompliant() {
    return true;
  }
  
}
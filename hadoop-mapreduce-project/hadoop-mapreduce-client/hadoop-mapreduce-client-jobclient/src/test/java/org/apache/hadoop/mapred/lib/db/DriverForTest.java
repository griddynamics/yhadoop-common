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
package org.apache.hadoop.mapred.lib.db;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
/**
 * class emulates a connection to database
 *
 */
public class DriverForTest implements Driver {

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

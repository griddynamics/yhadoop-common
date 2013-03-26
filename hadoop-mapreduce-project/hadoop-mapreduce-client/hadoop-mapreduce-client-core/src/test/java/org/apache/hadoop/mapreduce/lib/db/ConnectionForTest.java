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

/**
 * class emulation connection to database 
 */
package org.apache.hadoop.mapreduce.lib.db;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.*;
public class ConnectionForTest implements Connection {
  String sessionTimeZone;

  public String getSessionTimeZone() {
    return sessionTimeZone;
  }

  public void setSessionTimeZone(String input) {
    sessionTimeZone = input;
  }


  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public void commit() throws SQLException {

  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public Statement createStatement() throws SQLException {
    Statement result= mock(Statement.class);
    ResultSet results =mock(ResultSet.class);
    ResultSetMetaData metadata=mock(ResultSetMetaData.class);
    when(results.getMetaData()).thenReturn(metadata);
    when(metadata.getColumnType(1)).thenReturn(Types.TIMESTAMP);
    when(results.getTimestamp(1)).thenReturn(new Timestamp(150));
    when(results.getTimestamp(2)).thenReturn(new Timestamp(250));
    
    when(results.getLong(1)).thenReturn(15L);
    when(result.executeQuery(any(String.class))).thenReturn(results);
    
    return result;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public Statement createStatement(int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    return null;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public String getCatalog() throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    
    DatabaseMetaData result =mock(DatabaseMetaData.class);
    when(result.getDatabaseProductName()).thenReturn("Test");
    return result;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return false;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return  mock(PreparedStatement.class);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    PreparedStatement result= mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when( resultSet.next()).thenReturn(false);
    when(result.executeQuery()).thenReturn(resultSet);
    return result;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
  }

  @Override
  public void rollback() throws SQLException {
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {

  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {

  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

  }

}

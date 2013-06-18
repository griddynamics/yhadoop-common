package org.apache.hadoop.mapreduce.lib.db;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

public class FakeConnectionFactory {

  public static Connection getConnection() {

    Connection result = mock(FakeOracleConnection.class);
    try {
      Statement statement = mock(Statement.class);
      
      ResultSet results = mock(ResultSet.class);
      ResultSetMetaData metadata = mock(ResultSetMetaData.class);
      when(results.getMetaData()).thenReturn(metadata);
      when(metadata.getColumnType(1)).thenReturn(Types.TIMESTAMP);
      when(results.getTimestamp(1)).thenReturn(new Timestamp(150));
      when(results.getTimestamp(2)).thenReturn(new Timestamp(250));
      when(results.getLong(1)).thenReturn(15L);
      when(statement.executeQuery(any(String.class))).thenReturn(results);
      when(result.createStatement()).thenReturn(statement);

      DatabaseMetaData metaData = mock(DatabaseMetaData.class);
      when(metaData.getDatabaseProductName()).thenReturn("Test");
      when(result.getMetaData()).thenReturn(metaData);

      PreparedStatement preparedStatement0= mock(PreparedStatement.class);
      when(result.prepareStatement(anyString())).thenReturn(
          preparedStatement0);

      PreparedStatement preparedStatement = mock(PreparedStatement.class);
      ResultSet resultSet = mock(ResultSet.class);
      when(resultSet.next()).thenReturn(false);
      when(preparedStatement.executeQuery()).thenReturn(resultSet);

      when(result.prepareStatement(anyString(), anyInt(), anyInt()))
          .thenReturn(preparedStatement);
    } catch (SQLException e) {

    }
    return result;
  }
  private interface FakeOracleConnection extends Connection{
    public void setSessionTimeZone(String arg);
  }

}

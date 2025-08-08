package com.google.cloud.bigtable.jdbc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.sql.BoundStatement;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.util.Date;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BigtablePreparedStatementTest {

  private static final String SQL = "SELECT * FROM table WHERE id = ?";
  @Mock private BigtableDataClient mockDataClient;

  @Mock
  private com.google.cloud.bigtable.data.v2.models.sql.PreparedStatement mockPreparedStatement;

  @Mock private BoundStatement.Builder mockBoundStatementBuilder;
  @Mock private BoundStatement mockBoundStatement;
  @Mock private ResultSet mockResultSet;

  private AutoCloseable closeable;

  @Before
  public void openMocks() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @After
  public void releaseMocks() throws Exception {
    closeable.close();
  }

  private BigtablePreparedStatement createStatement() {
    return new BigtablePreparedStatement(SQL, mockDataClient);
  }

  @Test
  public void testExecuteQuery() throws SQLException {
    when(mockDataClient.prepareStatement(any(), any())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind()).thenReturn(mockBoundStatementBuilder);
    when(mockBoundStatementBuilder.build()).thenReturn(mockBoundStatement);
    when(mockDataClient.executeQuery(mockBoundStatement)).thenReturn(mockResultSet);

    PreparedStatement statement = createStatement();
    statement.setLong(1, 123L);
    java.sql.ResultSet resultSet = statement.executeQuery();
    assertNotNull(resultSet);
  }

  @Test
  public void testSetString() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setString(1, "test");
  }

  @Test
  public void testSetBoolean() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setBoolean(1, true);
  }

  @Test
  public void testSetDouble() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setDouble(1, 1.23);
  }

  @Test
  public void testSetDate() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setDate(1, new java.sql.Date(new Date().getTime()));
  }

  @Test
  public void testSetTimestamp() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setTimestamp(1, new Timestamp(new Date().getTime()));
  }

  @Test
  public void testSetBytes() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setBytes(1, "test".getBytes());
  }

  @Test
  public void testClearParameters() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setString(1, "test");
    statement.clearParameters();
  }

  @Test
  public void testExecute() throws SQLException {
    when(mockDataClient.prepareStatement(any(), any())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind()).thenReturn(mockBoundStatementBuilder);
    when(mockBoundStatementBuilder.build()).thenReturn(mockBoundStatement);
    when(mockDataClient.executeQuery(mockBoundStatement)).thenReturn(mockResultSet);

    PreparedStatement statement = createStatement();
    statement.setLong(1, 123L);
    statement.execute();
    assertNotNull(statement.getResultSet());
  }

  @Test
  public void testUnsupportedFeatures() {
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          PreparedStatement statement = createStatement();
          statement.executeUpdate();
        });
  }

  @Test
  public void testSetNull() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setNull(1, java.sql.Types.VARCHAR);
  }

  @Test
  public void testSetArray() throws SQLException {
    PreparedStatement statement = createStatement();
    java.sql.Array mockArray = org.mockito.Mockito.mock(java.sql.Array.class);
    when(mockArray.getBaseType()).thenReturn(java.sql.Types.VARCHAR);
    try {
      when(mockArray.getArray()).thenReturn(new String[] {"a", "b"});
    } catch (SQLException e) {
      // This should not happen in a mock
    }
    statement.setArray(1, mockArray);
  }

  @Test
  public void testSetDateWithCalendar() throws SQLException {
    PreparedStatement statement = createStatement();
    java.util.Calendar cal = java.util.Calendar.getInstance();
    statement.setDate(1, new java.sql.Date(new Date().getTime()), cal);
  }

  @Test
  public void testSetTimestampWithCalendar() throws SQLException {
    PreparedStatement statement = createStatement();
    java.util.Calendar cal = java.util.Calendar.getInstance();
    statement.setTimestamp(1, new Timestamp(new Date().getTime()), cal);
  }

  @Test
  public void testExecuteWithNoSql() {
    assertThrows(
        SQLException.class,
        () -> {
          BigtablePreparedStatement statement = new BigtablePreparedStatement(null, mockDataClient);
          statement.execute();
        });
  }

  @Test
  public void testClosedStatement() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.close();

    assertThrows(SQLException.class, () -> statement.executeQuery());
    assertThrows(SQLException.class, () -> statement.setLong(1, 123L));
    assertThrows(SQLException.class, () -> statement.setString(1, "test"));
    assertThrows(SQLException.class, () -> statement.setNull(1, java.sql.Types.VARCHAR));
    assertThrows(SQLException.class, () -> statement.setArray(1, null));
    assertThrows(SQLException.class, () -> statement.setDate(1, null, null));
    assertThrows(SQLException.class, () -> statement.setTimestamp(1, null, null));
    assertThrows(SQLException.class, () -> statement.clearParameters());
  }

  @Test
  public void testSetFloat() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setFloat(1, 1.23f);
  }

  @Test
  public void testSetInt() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setInt(1, 123);
  }

  @Test
  public void testSetShort() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.setShort(1, (short) 123);
  }

  @Test
  public void testSetBigDecimal() {
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          PreparedStatement statement = createStatement();
          statement.setBigDecimal(1, new java.math.BigDecimal("123.45"));
        });
  }

  @Test
  public void testSetParameterOnClosedStatement() throws SQLException {
    PreparedStatement statement = createStatement();
    statement.close();
    assertThrows(SQLException.class, () -> statement.setString(1, "test"));
  }

  @Test
  public void testSetLargeMaxRows() {
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          PreparedStatement statement = createStatement();
          statement.setLargeMaxRows(100L);
        });
  }

  @Test
  public void testSetQueryTimeout() {
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          PreparedStatement statement = createStatement();
          statement.setQueryTimeout(100);
        });
  }
}

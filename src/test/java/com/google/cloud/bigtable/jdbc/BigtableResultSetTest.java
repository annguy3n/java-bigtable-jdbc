package com.google.cloud.bigtable.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.data.v2.models.sql.ResultSet;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSetMetadata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class BigtableResultSetTest {
  private ResultSet mockedBigtableResultSet;
  private ResultSetMetadata mockedMetadata;
  private BigtableResultSet resultSet;

  @Before
  public void setUp() {
    mockedBigtableResultSet = mock(ResultSet.class);
    mockedMetadata = mock(ResultSetMetadata.class);

    when(mockedBigtableResultSet.getMetadata()).thenReturn(mockedMetadata);

    List<Map<String, Object>> rows = new ArrayList<>();

    resultSet = new BigtableResultSet(mockedBigtableResultSet, rows);
  }

  @Test
  public void testNextAndGetString() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedMetadata.getColumnIndex("col2")).thenReturn(1);

    when(mockedBigtableResultSet.getString(0)).thenReturn("value1").thenReturn("value2");

    when(mockedBigtableResultSet.getString(1)).thenReturn("123").thenReturn("456");

    assertTrue(resultSet.next());
    assertEquals("value1", resultSet.getString("col1"));
    assertEquals("123", resultSet.getString("col2"));

    assertTrue(resultSet.next());
    assertEquals("value2", resultSet.getString("col1"));
    assertEquals("456", resultSet.getString("col2"));

    assertFalse(resultSet.next());
  }

  @Test
  public void testGetStringReturnsNullIfValueIsNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);
    when(mockedBigtableResultSet.getString(0)).thenReturn(null);

    assertTrue(resultSet.next());
    assertNull(resultSet.getString("col1"));
    assertTrue(resultSet.wasNull());
  }

  @Test(expected = SQLException.class)
  public void testGetStringThrowsExceptionIfColumnInvalid() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("invalid"))
        .thenThrow(new IllegalArgumentException("Invalid column"));

    assertTrue(resultSet.next());
    resultSet.getString("invalid");
  }

  @Test(expected = SQLException.class)
  public void testGetStringWithNonExistentColumn() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("non_existent_column"))
        .thenThrow(new IllegalArgumentException("Column not found"));

    assertTrue(resultSet.next());
    resultSet.getString("non_existent_column");
  }

  @Test
  public void testGetBoolean() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getBoolean(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertTrue(resultSet.getBoolean("col1"));
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetBooleanWithNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertFalse(resultSet.getBoolean("col1"));
    assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetByte() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getLong(0)).thenReturn(50L);

    assertTrue(resultSet.next());
    assertEquals((byte) 50, resultSet.getByte("col1"));
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetByteWithNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getByte("col1"));
    assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetShort() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getLong(0)).thenReturn(1000L);

    assertTrue(resultSet.next());
    assertEquals((short) 1000, resultSet.getShort("col1"));
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetShortWithNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getShort("col1"));
    assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetInt() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getLong(0)).thenReturn(100000L);

    assertTrue(resultSet.next());
    assertEquals(100000, resultSet.getInt("col1"));
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetIntWithNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getInt("col1"));
    assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetLong() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getLong(0)).thenReturn(1000000000L);

    assertTrue(resultSet.next());
    assertEquals(1000000000L, resultSet.getLong("col1"));
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetLongWithNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertEquals(0L, resultSet.getLong("col1"));
    assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetFloat() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getFloat(0)).thenReturn(3.14f);

    assertTrue(resultSet.next());
    assertEquals(3.14f, resultSet.getFloat("col1"), 0.0);
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetFloatWithNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertEquals(0.0f, resultSet.getFloat("col1"), 0.0);
    assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetDouble() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getDouble(0)).thenReturn(3.14159);

    assertTrue(resultSet.next());
    assertEquals(3.14159, resultSet.getDouble("col1"), 0.0);
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetDoubleWithNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.isNull(0)).thenReturn(true);

    assertTrue(resultSet.next());
    assertEquals(0.0, resultSet.getDouble("col1"), 0.0);
    assertTrue(resultSet.wasNull());
  }

  @Test
  public void testClose() throws Exception {
    resultSet.close();
    verify(mockedBigtableResultSet).close();
  }

  @Test(expected = SQLException.class)
  public void testThrowsExceptionIfClosed() throws SQLException {
    resultSet.close();
    resultSet.next();
  }

  @Test
  public void testIsBeforeFirst() throws SQLException {
    assertTrue(resultSet.isBeforeFirst());
    when(mockedBigtableResultSet.next()).thenReturn(true);
    resultSet.next();
    assertFalse(resultSet.isBeforeFirst());
  }

  @Test
  public void testIsAfterLast() throws SQLException {
    assertFalse(resultSet.isAfterLast());
    when(mockedBigtableResultSet.next()).thenReturn(false);
    resultSet.next();
    assertTrue(resultSet.isAfterLast());
  }

  @Test
  public void testFindColumn() throws SQLException {
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(1);
    assertEquals(1, resultSet.findColumn("col1"));
  }

  @Test(expected = SQLException.class)
  public void testFindColumnThrowsException() throws SQLException {
    when(mockedMetadata.getColumnIndex("invalid"))
        .thenThrow(new RuntimeException("Column not found"));
    resultSet.findColumn("invalid");
  }
}

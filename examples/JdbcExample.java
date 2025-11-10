/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

/*
 * To run: mvn exec:java -Dexec.mainClass="JdbcExample"
 * -Dexec.args="test-project test-instance test-table rowkey"
 */
public class JdbcExample {

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Usage: JdbcExample <project_id> <instance_id> <table_name> <row_key>");
      System.exit(1);
    }
    // Replace with your project and instance IDs
    String projectId = args[0];
    String instanceId = args[1];
    String tableName = args[2]; // Replace with your table name
    String rowKey = args[3]; // Replace with a row key from your table

    String url = String.format("jdbc:bigtable:/projects/%s/instances/%s", projectId, instanceId);

    System.out.println(LocalDateTime.now() + "Connecting to " + url);

    try {
      // Load the Bigtable JDBC driver
      Class.forName("com.google.cloud.bigtable.jdbc.BigtableDriver");
    } catch (ClassNotFoundException e) {
      System.err.println("Failed to load Bigtable JDBC driver: " + e.getMessage());
      e.printStackTrace();
      return;
    }

    String sql = "SELECT * FROM " + tableName + " WHERE _key = ?";

    Statement statement = null;
    ResultSet rs = null;
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(url);
      //
      statement = connection.createStatement();

      System.out.println("Connection successful!" + LocalDateTime.now());

      // Set a parameter for the prepared statement
      // statement.setBytes(1, rowKey.getBytes());

      System.out.println(
          "Executing query: SELECT * FROM " + tableName + " WHERE _key = '" + rowKey + "'");

      rs = statement.executeQuery("SELECT * FROM " + tableName + " WHERE _key = '" + rowKey + "'");
      System.out.println("Query executed successfully." + LocalDateTime.now());

      while (rs.next()) {
        // Integer result = rs.getInt(1);
        System.out.println("--- Row ---" + LocalDateTime.now());
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          String columnName = rs.getMetaData().getColumnName(i);
          Object value = rs.getObject(i);
          System.out.println(columnName + ": " + value);
        }
        System.out.println("===================" + LocalDateTime.now());
      }
    } catch (SQLException e) {
      System.err.println("An SQL error occurred: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    System.out.println("DONE!" + LocalDateTime.now());
    System.exit(0);
  }
}

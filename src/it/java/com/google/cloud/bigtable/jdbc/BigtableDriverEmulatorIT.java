/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigtable.jdbc;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;

@RunWith(JUnit4.class)
public class BigtableDriverEmulatorIT {

  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR = BigtableEmulatorRule.create();

  private static BigtableEmulatorClientWrapper emulatorWrapper;

  private static String getProperty(String key, String defaultValue) {
    String value = System.getProperty(key);
    return (value == null || value.isEmpty()) ? defaultValue : value;
  }

  private static final String PROJECT = getProperty("google.bigtable.project.id", "fakeProject");
  private static final String INSTANCE = getProperty("google.bigtable.instance.id", "fakeInstance");

  static final String KEY1 = "key1";
  static final String KEY2 = "key2";
  static final String BOOL_COLUMN = "boolColumn";
  static final String LONG_COLUMN = "longColumn";
  static final String STRING_COLUMN = "stringColumn";
  static final String DOUBLE_COLUMN = "doubleColumn";
  static final String FAMILY_TEST = "familyTest";

  static final long NOW = 5_000_000_000L;
  static final long LATER = NOW + 1_000L;


  @BeforeClass
  public static void setUp() throws Exception {
    emulatorWrapper =
        new BigtableEmulatorClientWrapper(PROJECT, INSTANCE, BIGTABLE_EMULATOR.getPort(), null);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    emulatorWrapper.closeSession();
  }

  @Test
  public void testValidConnection() throws Exception {
    Class.forName("com.google.cloud.bigtable.jdbc.BigtableDriver");
    String url =
        String.format(
            "jdbc:bigtable://localhost:%d/projects/%s/instances/%s",
            BIGTABLE_EMULATOR.getPort(), PROJECT, INSTANCE);
    try (Connection connection = DriverManager.getConnection(url)) {
      assertTrue(connection.isValid(0));
    }
  }
}

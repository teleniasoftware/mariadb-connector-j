/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.function.Executable;

public class Common {

  protected static String mDefUrl;

  static {
    try (InputStream inputStream =
        Common.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);
      mDefUrl =
          String.format(
              "jdbc:mariadb://%s:%s/%s?user=%s&password=%s&%s",
              get("DB_HOST", prop),
              get("DB_PORT", prop),
              get("DB_DATABASE", prop),
              get("DB_USER", prop),
              get("DB_PASSWORD", prop),
              get("DB_OTHER", prop));

    } catch (IOException io) {
      io.printStackTrace();
    }
  }

  public static Connection sharedConn;

  private static String get(String name, Properties prop) {
    String val = System.getenv(name);
    if (val == null) val = prop.getProperty(name);
    return val;
  }

  public Connection createCon() throws SQLException {
    return (Connection) DriverManager.getConnection(mDefUrl);
  }
  public Connection createCon(String option) throws SQLException {
    return (Connection) DriverManager.getConnection(mDefUrl + "&" + option);
  }


  @BeforeAll
  public static void beforeAll() throws Exception {
    sharedConn = (Connection) DriverManager.getConnection(mDefUrl);
  }

  @AfterEach
  public void afterEach1() throws SQLException {
    sharedConn.isValid(2000);
  }

  @AfterAll
  public static void afterEAll() throws SQLException {
    sharedConn.close();
  }

  public void assertThrows(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
  }

  //  @RegisterExtension public Extension watcher = new Follow();

  public static boolean isMariaDBServer() {
    return sharedConn.getContext().getVersion().isMariaDBServer();
  }

  public static boolean minVersion(int major, int minor, int patch) {
    return sharedConn.getContext().getVersion().versionGreaterOrEqual(major, minor, patch);
  }

  //  public boolean haveSsl(MariadbConnection connection) {
  //    return connection
  //        .createStatement("select @@have_ssl")
  //        .execute()
  //        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
  //        .blockLast()
  //        .equals("YES");
  //  }

  private static Instant initialTest;

  private class Follow implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
      System.out.println(Duration.between(initialTest, Instant.now()).toString());
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
      initialTest = Instant.now();
      System.out.print("       test : " + extensionContext.getTestMethod().get() + " ");
    }
  }
}

package org.mariadb.jdbc.integration;

import java.sql.*;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class ConnectionTest extends Common {

  @Test
  public void isValid() throws SQLException {
    Connection sharedConn = (Connection) DriverManager.getConnection(mDefUrl);
    Assertions.assertTrue(sharedConn.isValid(2000));
    sharedConn.close();
    Assertions.assertFalse(sharedConn.isValid(2000));
  }

  @Test
  void isValidWrongValue() {
    try {
      sharedConn.isValid(-2000);
      Assertions.fail("most have thrown an error");
    } catch (SQLException e) {
      Assertions.assertTrue(e.getMessage().contains("the value supplied for timeout is negative"));
    }
  }

  @Test
  public void autoCommit() throws SQLException {
    Connection con = (Connection) DriverManager.getConnection(mDefUrl);
    Assertions.assertTrue(con.getAutoCommit());
    con.setAutoCommit(false);
    Assertions.assertFalse(con.getAutoCommit());
    con.setAutoCommit(false);
    Assertions.assertFalse(con.getAutoCommit());
    con.setAutoCommit(true);
    Assertions.assertTrue(con.getAutoCommit());
    con.setAutoCommit(true);
    Assertions.assertTrue(con.getAutoCommit());
    Statement stmt = con.createStatement();
    stmt.execute("SET autocommit=false");
    Assertions.assertFalse(con.getAutoCommit());
    con.close();
  }

  @Test
  public void nativeSQL() throws SQLException {
    String[] inputs =
        new String[] {
          "select {fn timestampdiff(SQL_TSI_HOUR, {fn convert('SQL_', SQL_INTEGER)})}",
          "{call foo({fn now()})}",
          "{ call foo({fn now()})}",
          "{\r\n call foo({fn now()})}",
          "{call foo(/*{fn now()}*/)}",
          "{call foo({fn now() /* -- * */ -- test \n })}",
          "{?=call foo({fn now()})}",
          "SELECT 'David_' LIKE 'David|_' {escape '|'}",
          "select {fn dayname ({fn abs({fn now()})})}",
          "{d '1997-05-24'}",
          "{d'1997-05-24'}",
          "{t '10:30:29'}",
          "{t'10:30:29'}",
          "{ts '1997-05-24 10:30:29.123'}",
          "{ts'1997-05-24 10:30:29.123'}",
          "'{string data with { or } will not be altered'",
          "`{string data with { or } will not be altered`",
          "--  Also note that you can safely include { and } in comments",
          "SELECT * FROM {oj TABLE1 LEFT OUTER JOIN TABLE2 ON DEPT_NO = 003420930}"
        };
    String[] outputs =
        new String[] {
          "select timestampdiff(HOUR, convert('SQL_', INTEGER))",
          "call foo(now())",
          "call foo(now())",
          "call foo(now())",
          "call foo(/*{fn now()}*/)",
          "call foo(now() /* -- * */ -- test \n )",
          "?=call foo(now())",
          "SELECT 'David_' LIKE 'David|_' escape '|'",
          "select dayname (abs(now()))",
          "'1997-05-24'",
          "'1997-05-24'",
          "'10:30:29'",
          "'10:30:29'",
          "'1997-05-24 10:30:29.123'",
          "'1997-05-24 10:30:29.123'",
          "'{string data with { or } will not be altered'",
          "`{string data with { or } will not be altered`",
          "--  Also note that you can safely include { and } in comments",
          "SELECT * FROM TABLE1 LEFT OUTER JOIN TABLE2 ON DEPT_NO = 003420930"
        };
    for (int i = 0; i < inputs.length; i++) {
      Assertions.assertEquals(sharedConn.nativeSQL(inputs[i]), outputs[i]);
    }

    try {
      sharedConn.nativeSQL("{call foo({fn now())}");
      Assertions.fail("most have thrown an error");
    } catch (SQLException e) {
      Assertions.assertTrue(
          e.getMessage().contains("Invalid escape sequence , missing closing '}' character in '"));
    }

    try {
      sharedConn.nativeSQL("{call foo({unknown} )}");
      Assertions.fail("most have thrown an error");
    } catch (SQLException e) {
      Assertions.assertTrue(e.getMessage().contains("unknown escape sequence {unknown}"));
    }
  }

  @Test
  public void nativeSqlTest() throws SQLException {
    String exp;
    if (sharedConn.isMariaDbServer() || minVersion(8, 0, 17)) {
      exp =
          "SELECT convert(foo(a,b,c), SIGNED INTEGER)"
              + ", convert(convert(?, CHAR), SIGNED INTEGER)"
              + ", 1=?"
              + ", 1=?"
              + ", convert(?,   SIGNED INTEGER   )"
              + ",  convert (?,   SIGNED INTEGER   )"
              + ", convert(?, UNSIGNED INTEGER)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, DOUBLE)"
              + ", convert(?, DOUBLE)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DATETIME)"
              + ", convert(?, DATETIME)";
    } else {
      exp =
          "SELECT convert(foo(a,b,c), SIGNED INTEGER)"
              + ", convert(convert(?, CHAR), SIGNED INTEGER)"
              + ", 1=?"
              + ", 1=?"
              + ", convert(?,   SIGNED INTEGER   )"
              + ",  convert (?,   SIGNED INTEGER   )"
              + ", convert(?, UNSIGNED INTEGER)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", 0.0+?"
              + ", 0.0+?"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DATETIME)"
              + ", convert(?, DATETIME)";
    }

    Assertions.assertEquals(
        exp,
        sharedConn.nativeSQL(
            "SELECT {fn convert(foo(a,b,c), SQL_BIGINT)}"
                + ", {fn convert({fn convert(?, SQL_VARCHAR)}, SQL_BIGINT)}"
                + ", {fn convert(?, SQL_BOOLEAN )}"
                + ", {fn convert(?, BOOLEAN)}"
                + ", {fn convert(?,   SMALLINT   )}"
                + ", {fn  convert (?,   TINYINT   )}"
                + ", {fn convert(?, SQL_BIT)}"
                + ", {fn convert(?, SQL_BLOB)}"
                + ", {fn convert(?, SQL_VARBINARY)}"
                + ", {fn convert(?, SQL_LONGVARBINARY)}"
                + ", {fn convert(?, SQL_ROWID)}"
                + ", {fn convert(?, SQL_BINARY)}"
                + ", {fn convert(?, SQL_NCHAR)}"
                + ", {fn convert(?, SQL_CLOB)}"
                + ", {fn convert(?, SQL_NCLOB)}"
                + ", {fn convert(?, SQL_DATALINK)}"
                + ", {fn convert(?, SQL_VARCHAR)}"
                + ", {fn convert(?, SQL_NVARCHAR)}"
                + ", {fn convert(?, SQL_LONGVARCHAR)}"
                + ", {fn convert(?, SQL_LONGNVARCHAR)}"
                + ", {fn convert(?, SQL_SQLXML)}"
                + ", {fn convert(?, SQL_LONGNCHAR)}"
                + ", {fn convert(?, SQL_CHAR)}"
                + ", {fn convert(?, SQL_FLOAT)}"
                + ", {fn convert(?, SQL_DOUBLE)}"
                + ", {fn convert(?, SQL_DECIMAL)}"
                + ", {fn convert(?, SQL_REAL)}"
                + ", {fn convert(?, SQL_NUMERIC)}"
                + ", {fn convert(?, SQL_TIMESTAMP)}"
                + ", {fn convert(?, SQL_DATETIME)}"));
  }

  @Test
  public void readOnly() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testReadOnly");
    stmt.execute("CREATE TABLE testReadOnly(id int)");
    sharedConn.setAutoCommit(false);
    sharedConn.setReadOnly(true);
    stmt.execute("INSERT INTO testReadOnly values (1)");
    sharedConn.commit();

    try (Connection con = createCon("assureReadOnly=true")) {
      final Statement stmt2 = con.createStatement();

      con.setAutoCommit(false);
      con.setReadOnly(true);
      assertThrows(SQLException.class,
          () -> stmt2.execute("INSERT INTO testReadOnly values (2)"),
          "Cannot execute statement in a READ ONLY transaction");
      con.setReadOnly(false);
      stmt2.execute("DROP TABLE testReadOnly");
    }
  }

  @Test
  public void databaseStateChange() throws SQLException {
    Assumptions.assumeTrue(
            (isMariaDBServer() && minVersion(10, 2, 0)) || (!isMariaDBServer() && minVersion(5, 7, 0)));
    try (Connection connection = createCon()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("drop database if exists _test_db");
        stmt.execute("create database _test_db");
        ResultSet rs = stmt.executeQuery("SELECT DATABASE()");
        rs.next();
        Assertions.assertEquals(rs.getString(1), connection.getCatalog());
        stmt.execute("USE _test_db");
        Assertions.assertEquals("_test_db", connection.getCatalog());
        stmt.execute("drop database _test_db");
      }
    }
  }

  @Test
  public void checkFixedData() throws SQLException {
    sharedConn.unwrap(java.sql.Connection.class);
    assertThrows(
            SQLException.class,
            () -> sharedConn.unwrap(String.class),
            "The receiver is not a wrapper for java.lang.String");
    Assertions.assertEquals(sharedConn, sharedConn.getConnection());
  }
  @Test
  public void networkTimeoutTest() throws SQLException {
    try (Connection connection = createCon()) {
      Assertions.assertEquals(0, connection.getNetworkTimeout());
      int timeout = 1000;
      SQLPermission sqlPermission = new SQLPermission("setNetworkTimeout");
      SecurityManager securityManager = System.getSecurityManager();
      if (securityManager != null) {
        try {
          securityManager.checkPermission(sqlPermission);
        } catch (SecurityException se) {
          System.out.println("test 'setNetworkTimeout' skipped  due to missing policy");
          return;
        }
      }
      Executor executor = Runnable::run;
      connection.setNetworkTimeout(executor, timeout);
      connection.isValid(2);
      Assertions.assertEquals(timeout, connection.getNetworkTimeout());

      try {
        Statement stmt = connection.createStatement();
        stmt.execute("select sleep(2)");
        Assertions.fail("Network timeout is " + timeout / 1000 + "sec, but slept for 2 sec");
      } catch (SQLException sqlex) {
        Assertions.assertTrue(connection.isClosed());
      }
    }
  }
}

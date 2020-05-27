package org.mariadb.jdbc.integration;

import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StatementTest extends Common {

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn
            .createStatement()
            .execute("DROP TABLE StatementTest");
  }

  @BeforeEach
  public void beforeEach() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS StatementTest");
    stmt.execute("CREATE TABLE StatementTest (t1 int not null primary key auto_increment, t2 int)");
  }

  @Test
  public void execute() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertTrue(stmt.execute("SELECT 1"));
    Assertions.assertEquals(-1, stmt.getUpdateCount());
    Assertions.assertFalse(stmt.getMoreResults());
    Assertions.assertEquals(-1, stmt.getUpdateCount());
    Assertions.assertFalse(stmt.execute("DO 1"));
    Assertions.assertEquals(0, stmt.getUpdateCount());
    Assertions.assertFalse(stmt.getMoreResults());
    Assertions.assertEquals(-1, stmt.getUpdateCount());
    stmt.close();
  }

  @Test
  public void executeGenerated() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertFalse(stmt.execute("INSERT INTO StatementTest(t2) values (100)"));
    try {
      stmt.getGeneratedKeys();
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(sqle.getMessage().contains("Cannot return generated keys"));
    }
    Assertions.assertFalse(stmt.execute("INSERT INTO StatementTest(t2) values (100)", Statement.RETURN_GENERATED_KEYS));
    ResultSet rs = stmt.getGeneratedKeys();
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(2, rs.getInt(1));
  }


  @Test
  public void executeUpdate() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO StatementTest(t1, t2) values (1, 110), (2, 120)");
    Assertions.assertEquals(2, stmt.executeUpdate("UPDATE StatementTest SET t2 = 130 WHERE t2 > 100"));
    Assertions.assertEquals(2, stmt.getUpdateCount());
    Assertions.assertFalse(stmt.getMoreResults());
    Assertions.assertEquals(-1, stmt.getUpdateCount());

    try {
      stmt.executeUpdate("SELECT 1");
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(sqle.getMessage().contains("the given SQL statement produces an unexpected ResultSet object"));
    }
    Assertions.assertEquals(0, stmt.executeUpdate("DO 1"));
  }

  @Test
  public void executeQuery() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1");
    Assertions.assertTrue(rs.next());

    rs = stmt.executeQuery("DO 1");
    Assertions.assertFalse(rs.next());
  }

  @Test
  public void close() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    stmt.close();

    try {
      stmt.getMoreResults();
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
          sqle.getMessage().contains("Cannot do an operation on a closed statement"));
    }
    try {
      stmt.execute("ANY");
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
              sqle.getMessage().contains("Cannot do an operation on a closed statement"));
    }
    try {
      stmt.executeUpdate("ANY");
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
              sqle.getMessage().contains("Cannot do an operation on a closed statement"));
    }
    try {
      stmt.getMoreResults(1);
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
              sqle.getMessage().contains("Cannot do an operation on a closed statement"));
    }
    try {
      stmt.executeQuery("ANY");
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
              sqle.getMessage().contains("Cannot do an operation on a closed statement"));
    }
    try {
      stmt.executeBatch();
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
              sqle.getMessage().contains("Cannot do an operation on a closed statement"));
    }
    try {
      stmt.getConnection();
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
              sqle.getMessage().contains("Cannot do an operation on a closed statement"));
    }
    stmt.close();
  }

}

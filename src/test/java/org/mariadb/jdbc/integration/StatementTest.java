package org.mariadb.jdbc.integration;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Statement;

public class StatementTest extends Common {

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE StatementTest");
  }

  @BeforeEach
  public void beforeEach() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS StatementTest");
    stmt.execute("CREATE TABLE StatementTest (t1 int not null primary key auto_increment, t2 int)");
  }

  @Test
  public void getConnection() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertEquals(sharedConn, stmt.getConnection());
  }

  @Test
  public void execute() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertTrue(stmt.execute("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    ResultSet rs = stmt.getGeneratedKeys();
    Assertions.assertFalse(rs.next());
    Assertions.assertNotNull(stmt.getResultSet());
    Assertions.assertEquals(-1, stmt.getUpdateCount());
    Assertions.assertFalse(stmt.getMoreResults());
    Assertions.assertEquals(-1, stmt.getUpdateCount());
    Assertions.assertFalse(stmt.execute("DO 1"));
    Assertions.assertNull(stmt.getResultSet());
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
    Assertions.assertFalse(
        stmt.execute(
            "INSERT INTO StatementTest(t2) values (100)", Statement.RETURN_GENERATED_KEYS));
    ResultSet rs = stmt.getGeneratedKeys();
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(2, rs.getInt(1));
  }

  @Test
  public void executeUpdate() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO StatementTest(t1, t2) values (1, 110), (2, 120)");
    Assertions.assertEquals(
        2, stmt.executeUpdate("UPDATE StatementTest SET t2 = 130 WHERE t2 > 100"));
    Assertions.assertEquals(2, stmt.getUpdateCount());
    Assertions.assertFalse(stmt.getMoreResults());
    Assertions.assertEquals(-1, stmt.getUpdateCount());

    try {
      stmt.executeUpdate("SELECT 1");
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
          sqle.getMessage()
              .contains("the given SQL statement produces an unexpected ResultSet object"));
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
    Assertions.assertFalse(stmt.isClosed());
    ResultSet rs = stmt.executeQuery("select * FROM mysql.user LIMIT 1");
    rs.next();
    Object[] objs = new Object[46];
    for (int i = 0; i < 46; i++) {
      objs[i] = rs.getObject(i + 1);
    }

    stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    stmt.close();
    Assertions.assertTrue(stmt.isClosed());
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



  @Test
  public void maxRows() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertEquals(0, stmt.getMaxRows());
    try {
      stmt.setMaxRows(-1);
      Assertions.fail();
    } catch (SQLException e) {
      Assertions.assertTrue(e.getMessage().contains("max rows cannot be negative"));
    }

    stmt.setMaxRows(10);
    Assertions.assertEquals(10, stmt.getMaxRows());

    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    int i = 0;
    while (rs.next()) {
      i++;
      Assertions.assertEquals(i, rs.getInt(1));
    }
    Assertions.assertEquals(10, i);
  }

  @Test
  public void checkFixedData() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertFalse(stmt.isPoolable());
    stmt.setPoolable(true);
    Assertions.assertFalse(stmt.isPoolable());
    Assertions.assertFalse(stmt.isWrapperFor(String.class));
    Assertions.assertTrue(stmt.isWrapperFor(Statement.class));
    java.sql.Statement s = stmt.unwrap(java.sql.Statement.class);
    try {
      stmt.unwrap(String.class);
      Assertions.fail();
    } catch (SQLException e) {
      Assertions.assertTrue(e.getMessage().contains("he receiver is not a wrapper and does not implement the interface"));
    }
    Assertions.assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
    Assertions.assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    Assertions.assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
    Assertions.assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
  }

  @Test
  public void closeOnCompletion() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.closeOnCompletion();
    ResultSet rs = stmt.executeQuery("SELECT 1");
    Assertions.assertFalse(stmt.isClosed());
    Assertions.assertFalse(rs.isClosed());
    rs.close();
    Assertions.assertTrue(rs.isClosed());
    Assertions.assertTrue(stmt.isClosed());
  }


  @Test
  public void getMoreResults() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    Assertions.assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    Assertions.assertFalse(rs.isClosed());

    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    Assertions.assertTrue(rs.isClosed());
    stmt.close();
  }

}

package org.mariadb.jdbc.integration;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
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

    Assertions.assertTrue(stmt.execute("SELECT 1", new int[] {1, 2}));
    rs = stmt.getGeneratedKeys();
    Assertions.assertFalse(rs.next());

    Assertions.assertTrue(stmt.execute("SELECT 1", new String[] {"test", "test2"}));
    rs = stmt.getGeneratedKeys();
    Assertions.assertFalse(rs.next());

    stmt.close();
  }

  @Test
  public void executeGenerated() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertFalse(stmt.execute("INSERT INTO StatementTest(t2) values (100)"));

    SQLException e = Assertions.assertThrows(SQLException.class, () -> stmt.getGeneratedKeys());
    Assertions.assertTrue(e.getMessage().contains("Cannot return generated keys"));

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

    Assertions.assertEquals(
            2, stmt.executeUpdate("UPDATE StatementTest SET t2 = 150 WHERE t2 > 100", new int[]{1, 2}));
    Assertions.assertEquals(2, stmt.getUpdateCount());

    Assertions.assertEquals(
            2, stmt.executeUpdate("UPDATE StatementTest SET t2 = 150 WHERE t2 > 100", new String[]{"test", "test2"}));
    Assertions.assertEquals(2, stmt.getUpdateCount());

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
  public void executeLargeUpdate() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO StatementTest(t1, t2) values (1, 110), (2, 120)");
    Assertions.assertEquals(
            2, stmt.executeLargeUpdate("UPDATE StatementTest SET t2 = 130 WHERE t2 > 100"));
    Assertions.assertEquals(2L, stmt.getLargeUpdateCount());
    Assertions.assertFalse(stmt.getMoreResults());
    Assertions.assertEquals(-1L, stmt.getLargeUpdateCount());

    Assertions.assertEquals(
            2, stmt.executeLargeUpdate("UPDATE StatementTest SET t2 = 150 WHERE t2 > 100", new int[]{1, 2}));
    Assertions.assertEquals(2L, stmt.getLargeUpdateCount());

    Assertions.assertEquals(
            2, stmt.executeLargeUpdate("UPDATE StatementTest SET t2 = 150 WHERE t2 > 100", new String[]{"test", "test2"}));
    Assertions.assertEquals(2L, stmt.getLargeUpdateCount());

    try {
      stmt.executeLargeUpdate("SELECT 1");
      Assertions.fail();
    } catch (SQLException sqle) {
      Assertions.assertTrue(
              sqle.getMessage()
                      .contains("the given SQL statement produces an unexpected ResultSet object"));
    }
    Assertions.assertEquals(0, stmt.executeLargeUpdate("DO 1"));
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
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    Assertions.assertFalse(stmt.isClosed());
    ResultSet rs = stmt.executeQuery("select * FROM mysql.user LIMIT 1");
    rs.next();
    Object[] objs = new Object[45];
    for (int i = 0; i < 45; i++) {
      objs[i] = rs.getObject(i + 1);
    }

    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    Assertions.assertFalse(rs.isClosed());
    stmt.close();
    Assertions.assertTrue(stmt.isClosed());
    Assertions.assertTrue(rs.isClosed());

    assertThrows(
            SQLException.class,
            () -> stmt.clearBatch(),
            "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.isPoolable(),
            "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.setPoolable(true),
            "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.closeOnCompletion(),
            "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.isCloseOnCompletion(),
            "Cannot do an operation on a closed statement");

  assertThrows(
            SQLException.class,
            () -> stmt.getResultSetConcurrency(),
            "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.getFetchSize(),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.getMoreResults(),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.execute("ANY"),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.executeUpdate("ANY"),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.executeQuery("ANY"),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.executeBatch(),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.getConnection(),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.getMoreResults(1),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class, () -> stmt.cancel(), "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.getMaxRows(),
        "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.getLargeMaxRows(),
            "Cannot do an operation on a closed statement");

    assertThrows(
        SQLException.class,
        () -> stmt.setMaxRows(1),
        "Cannot do an operation on a closed statement");
    assertThrows(
        SQLException.class,
        () -> stmt.setEscapeProcessing(true),
        "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.getQueryTimeout(),
            "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.getUpdateCount(),
            "Cannot do an operation on a closed statement");
    assertThrows(
            SQLException.class,
            () -> stmt.getLargeUpdateCount(),
            "Cannot do an operation on a closed statement");
  }

  @Test
  public void maxRows() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
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

    stmt.setQueryTimeout(2);
    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    i = 0;
    while (rs.next()) {
      i++;
      Assertions.assertEquals(i, rs.getInt(1));
    }
    Assertions.assertEquals(10, i);
  }

  @Test
  public void largeMaxRows() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    Assertions.assertEquals(0L, stmt.getLargeMaxRows());
    try {
      stmt.setLargeMaxRows(-1);
      Assertions.fail();
    } catch (SQLException e) {
      Assertions.assertTrue(e.getMessage().contains("max rows cannot be negative"));
    }

    stmt.setLargeMaxRows(10);
    Assertions.assertEquals(10L, stmt.getLargeMaxRows());

    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    int i = 0;
    while (rs.next()) {
      i++;
      Assertions.assertEquals(i, rs.getInt(1));
    }
    Assertions.assertEquals(10, i);

    stmt.setQueryTimeout(2);
    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    i = 0;
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
    stmt.unwrap(java.sql.Statement.class);

    assertThrows(
        SQLException.class,
        () -> stmt.unwrap(String.class),
        "he receiver is not a wrapper and does not implement the interface");
    assertThrows(SQLException.class, () -> stmt.setCursorName(""), "Cursors are not supported");

    Assertions.assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
    Assertions.assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    Assertions.assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
    Assertions.assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
    Assertions.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
    Assertions.assertEquals(0, stmt.getMaxFieldSize());
    stmt.setMaxFieldSize(100);
    Assertions.assertEquals(0, stmt.getMaxFieldSize());
  }

  @Test
  public void getMoreResults() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    Assertions.assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    Assertions.assertFalse(rs.isClosed());

    rs = stmt.executeQuery("SELECT * FROM seq_1_to_10000");
    stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    Assertions.assertTrue(rs.isClosed());
    stmt.close();
  }

  @Test
  @Timeout(20)
  public void queryTimeout() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();

    assertThrows(
        SQLException.class, () -> stmt.setQueryTimeout(-1), "Query timeout cannot be negative");

    assertThrows(
        SQLTimeoutException.class,
        () -> {
          stmt.setQueryTimeout(1);
          Assertions.assertEquals(1, stmt.getQueryTimeout());
          stmt.execute(
              "select * from information_schema.columns as c1,  information_schema.tables, information_schema"
                  + ".tables as t2");
        },
        "Query execution was interrupted (max_statement_time exceeded)");
  }

  @Test
  public void escaping() throws Exception {
    try (Connection con =
        (Connection) DriverManager.getConnection(mDefUrl + "&dumpQueriesOnException=true")) {
      Statement stmt = con.createStatement();
      assertThrows(
          SQLException.class,
          () ->
              stmt.executeQuery(
                  "select {fn timestampdiff(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df "),
          "select {fn timestampdiff" + "(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df ");
      stmt.setEscapeProcessing(true);
      assertThrows(
          SQLException.class,
          () ->
              stmt.executeQuery(
                  "select {fn timestampdiff(SQL_TSI_HOUR, '2003-02-01','2003-05-01')} df df "),
          "select timestampdiff(HOUR, '2003-02-01','2003-05-01') df df ");
    }
  }

  @Test
  public void testWarnings() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.executeQuery("select now() = 1");
    SQLWarning warning = stmt.getWarnings();
    Assertions.assertTrue(warning.getMessage().contains("ncorrect datetime value: '1'"));
    stmt.clearWarnings();
    Assertions.assertNull(stmt.getWarnings());
  }

  @Test
  public void cancel() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.cancel(); // will do nothing

    ExecutorService exec = Executors.newFixedThreadPool(1);

    assertThrows(
        SQLTimeoutException.class,
        () -> {
          exec.execute(new CancelThread(stmt));
          stmt.execute(
              "select * from information_schema.columns as c1,  information_schema.tables, information_schema"
                  + ".tables as t2");
          exec.shutdown();
        },
        "Query execution was interrupted");
  }

  private static class CancelThread implements Runnable {

    private final java.sql.Statement stmt;

    public CancelThread(java.sql.Statement stmt) {
      this.stmt = stmt;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(100);
        stmt.cancel();
      } catch (SQLException | InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void fetch() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    assertThrows(
            SQLException.class, () -> stmt.setFetchSize(-10), "invalid fetch size");

    stmt.setFetchSize(10);
    Assertions.assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_10000");

    for (int i = 1; i <= 10000; i++) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(i, rs.getInt(1));
    }

    Assertions.assertFalse(rs.next());
  }

  @Test
  public void fetchUnFinishedSameStatement() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(10);
    Assertions.assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_1000");

    for (int i = 1; i <= 500; i++) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(i, rs.getInt(1));
    }

    ResultSet rs2 = stmt.executeQuery("select * FROM seq_1_to_1000");

    for (int i = 501; i <= 1000; i++) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(i, rs.getInt(1));
    }
    Assertions.assertFalse(rs.next());

    for (int i = 1; i <= 1000; i++) {
      Assertions.assertTrue(rs2.next());
      Assertions.assertEquals(i, rs2.getInt(1));
    }
    Assertions.assertFalse(rs2.next());
  }

  @Test
  public void fetchUnFinishedOtherStatement() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(5);
    Assertions.assertEquals(5, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_20");

    for (int i = 1; i <= 10; i++) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(i, rs.getInt(1));
    }

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM seq_1_to_20");

    for (int i = 11; i <= 20; i++) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(i, rs.getInt(1));
    }
    Assertions.assertFalse(rs.next());

    for (int i = 1; i <= 20; i++) {
      Assertions.assertTrue(rs2.next());
      Assertions.assertEquals(i, rs2.getInt(1));
    }
    Assertions.assertFalse(rs2.next());
  }

  @Test
  public void fetchClose() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(10);
    Assertions.assertEquals(10, stmt.getFetchSize());
    ResultSet rs = stmt.executeQuery("select * FROM seq_1_to_1000");

    for (int i = 1; i <= 500; i++) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(i, rs.getInt(1));
    }
    stmt.close();
    Assertions.assertTrue(rs.isClosed());

    Statement stmt2 = sharedConn.createStatement();
    ResultSet rs2 = stmt2.executeQuery("select * FROM seq_1_to_1000");
    for (int i = 1; i <= 1000; i++) {
      Assertions.assertTrue(rs2.next());
      Assertions.assertEquals(i, rs2.getInt(1));
    }
    Assertions.assertFalse(rs2.next());
  }

  @Test
  public void executeBatchBasic() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS executeBatchBasic");
    stmt.execute("CREATE TABLE executeBatchBasic (t1 int not null primary key auto_increment, t2 int)");

    assertThrows(
            SQLException.class,
            () -> stmt.addBatch(null),
            "null cannot be set to addBatch(String sql)");


    stmt.addBatch("INSERT INTO executeBatchBasic(t2) VALUES (55)");
    stmt.addBatch("INSERT INTO executeBatchBasic(t2) VALUES (56)");
    int[] ret = stmt.executeBatch();
    Assertions.assertArrayEquals(new int[] {1,1},ret);

    ret = stmt.executeBatch();
    Assertions.assertArrayEquals(new int[0],ret);

    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (57)");
    stmt.clearBatch();
    ret = stmt.executeBatch();
    Assertions.assertArrayEquals(new int[0],ret);

    stmt.addBatch("WRONG QUERY");
    assertThrows(
            BatchUpdateException.class,
            () -> stmt.executeBatch(),
            "You have an error in your SQL syntax");
  }

  @Test
  public void executeLargeBatchBasic() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS executeLargeBatchBasic");
    stmt.execute("CREATE TABLE executeLargeBatchBasic (t1 int not null primary key auto_increment, t2 int)");
    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (55)");
    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (56)");
    long[] ret = stmt.executeLargeBatch();
    Assertions.assertArrayEquals(new long[] {1,1},ret);

    ret = stmt.executeLargeBatch();
    Assertions.assertArrayEquals(new long[0],ret);

    stmt.addBatch("INSERT INTO executeLargeBatchBasic(t2) VALUES (57)");
    stmt.clearBatch();
    ret = stmt.executeLargeBatch();
    Assertions.assertArrayEquals(new long[0],ret);

    stmt.addBatch("WRONG QUERY");
    assertThrows(
            BatchUpdateException.class,
            () -> stmt.executeLargeBatch(),
            "You have an error in your SQL syntax");
  }

  @Test
  public void moreResults() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multi");
    stmt.setFetchSize(3);
    stmt.execute("CREATE PROCEDURE multi() BEGIN SELECT * from seq_1_to_10; SELECT * FROM seq_1_to_1000;SELECT 2; END");
    stmt.execute("CALL multi()");
    Assertions.assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }

    stmt.setFetchSize(3);
    rs = stmt.executeQuery("CALL multi()");
    Assertions.assertFalse(rs.isClosed());
    stmt.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(stmt.getMoreResults());
    Assertions.assertTrue(rs.isClosed());
    rs = stmt.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }

    stmt.setFetchSize(3);
    rs = stmt.executeQuery("CALL multi()");
    Assertions.assertFalse(rs.isClosed());
    stmt.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(stmt.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
    Assertions.assertFalse(rs.isClosed());
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(11, i);
    rs = stmt.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(1001, i);
  }

  @Test
  public void closeOnCompletion() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Assertions.assertFalse(stmt.isCloseOnCompletion());
    stmt.closeOnCompletion();
    Assertions.assertTrue(stmt.isCloseOnCompletion());
    Assertions.assertFalse(stmt.isClosed());
    ResultSet rs = stmt.executeQuery("SELECT 1");
    Assertions.assertFalse(rs.isClosed());
    Assertions.assertFalse(stmt.isClosed());
    rs.close();
    Assertions.assertTrue(rs.isClosed());
    Assertions.assertTrue(stmt.isClosed());
  }

}

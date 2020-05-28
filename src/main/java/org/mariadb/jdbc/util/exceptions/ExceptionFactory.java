package org.mariadb.jdbc.util.exceptions;

import java.sql.*;
import javax.sql.ConnectionEvent;
import javax.sql.StatementEvent;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.util.options.Options;

public class ExceptionFactory {

  private final Connection connection;
  private final Options options;
  private final HostAddress hostAddress;

  private long threadId;
  private Statement statement;

  public ExceptionFactory(Connection connection, Options options, HostAddress hostAddress) {
    this.options = options;
    this.connection = connection;
    this.hostAddress = hostAddress;
  }

  public void setThreadId(long threadId) {
    this.threadId = threadId;
  }

  private ExceptionFactory(
      Connection connection,
      Options options,
      HostAddress hostAddress,
      long threadId,
      Statement statement) {
    this.connection = connection;
    this.options = options;
    this.hostAddress = hostAddress;
    this.threadId = threadId;
    this.statement = statement;
  }

  public ExceptionFactory of(Statement statement) {
    return new ExceptionFactory(
        this.connection, this.options, this.hostAddress, this.threadId, statement);
  }

  public ExceptionFactory withSql(String sql) {
    return new SqlExceptionFactory(
        this.connection, this.options, this.hostAddress, this.threadId, statement, sql);
  }

  private SQLException createException(
      String initialMessage, String sqlState, int errorCode, Exception cause) {

    String msg = buildMsgText(initialMessage, threadId, options, cause, getSql());

    if ("70100".equals(sqlState)) { // ER_QUERY_INTERRUPTED
      return new SQLTimeoutException(msg, sqlState, errorCode);
    }

    SQLException returnEx;
    String sqlClass = sqlState == null ? "42" : sqlState.substring(0, 2);
    switch (sqlClass) {
      case "0A":
        returnEx = new SQLFeatureNotSupportedException(msg, sqlState, errorCode, cause);
        break;
      case "22":
      case "26":
      case "2F":
      case "20":
      case "42":
      case "XA":
        returnEx = new SQLSyntaxErrorException(msg, sqlState, errorCode, cause);
        break;
      case "25":
      case "28":
        returnEx = new SQLInvalidAuthorizationSpecException(msg, sqlState, errorCode, cause);
        break;
      case "21":
      case "23":
        returnEx = new SQLIntegrityConstraintViolationException(msg, sqlState, errorCode, cause);
        break;
      case "08":
        returnEx = new SQLNonTransientConnectionException(msg, sqlState, errorCode, cause);
        break;
      case "40":
        returnEx = new SQLTransactionRollbackException(msg, sqlState, errorCode, cause);
        break;
      default:
        returnEx = new SQLTransientConnectionException(msg, sqlState, errorCode, cause);
        break;
    }

    if (statement != null && statement instanceof PreparedStatement) {
      StatementEvent event =
          new StatementEvent(connection, (PreparedStatement) statement, returnEx);
      connection.fireStatementErrorOccurred(event);
    }

    if (returnEx instanceof SQLNonTransientConnectionException
        || returnEx instanceof SQLTransientConnectionException) {
      ConnectionEvent event = new ConnectionEvent(connection, returnEx);
      connection.fireConnectionErrorOccurred(event);
    }

    return returnEx;
  }

  private static String buildMsgText(
      String initialMessage, long threadId, Options options, Exception cause, String sql) {

    StringBuilder msg = new StringBuilder();
    String deadLockException = null;
    String threadName = null;

    if (threadId != -1L) {
      msg.append("(conn=").append(threadId).append(") ").append(initialMessage);
    } else {
      msg.append(initialMessage);
    }

    if (options.dumpQueriesOnException && sql != null) {
      if (options != null
          && options.maxQuerySizeToLog != 0
          && sql.length() > options.maxQuerySizeToLog - 3) {
        msg.append("\nQuery is: ").append(sql, 0, options.maxQuerySizeToLog - 3).append("...");
      } else {
        msg.append("\nQuery is: ").append(sql);
      }
    }

    //    if (cause instanceof MariaDbSqlException) {
    //      MariaDbSqlException exception = ((MariaDbSqlException) cause);
    //      String sql = exception.getSql();
    //      if (options.dumpQueriesOnException && sql != null) {
    //        if (options != null
    //            && options.maxQuerySizeToLog != 0
    //            && sql.length() > options.maxQuerySizeToLog - 3) {
    //          msg.append("\nQuery is: ").append(sql, 0, options.maxQuerySizeToLog -
    // 3).append("...");
    //        } else {
    //          msg.append("\nQuery is: ").append(sql);
    //        }
    //      }
    //      deadLockException = exception.getDeadLockInfo();
    //      threadName = exception.getThreadName();
    //    }

    if (options != null
        && options.includeInnodbStatusInDeadlockExceptions
        && deadLockException != null) {
      msg.append("\ndeadlock information: ").append(deadLockException);
    }

    if (options != null && options.includeThreadDumpInDeadlockExceptions) {
      if (threadName != null) {
        msg.append("\nthread name: ").append(threadName);
      }
      msg.append("\ncurrent threads: ");
      Thread.getAllStackTraces()
          .forEach(
              (thread, traces) -> {
                msg.append("\n  name:\"")
                    .append(thread.getName())
                    .append("\" pid:")
                    .append(thread.getId())
                    .append(" status:")
                    .append(thread.getState());
                for (int i = 0; i < traces.length; i++) {
                  msg.append("\n    ").append(traces[i]);
                }
              });
    }

    return msg.toString();
  }

  public SQLException create(SQLException cause) {

    return createException(
        cause.getMessage().contains("\n")
            ? cause.getMessage().substring(0, cause.getMessage().indexOf("\n"))
            : cause.getMessage(),
        cause.getSQLState(),
        cause.getErrorCode(),
        cause);
  }

  public SQLException notSupported(String message) {
    return createException(message, "0A000", -1, null);
  }

  public SQLException create(String message) {
    return createException(message, "42000", -1, null);
  }

  public SQLException create(String message, Exception cause) {
    return createException(message, "42000", -1, cause);
  }

  public SQLException create(String message, String sqlState) {
    return createException(message, sqlState, -1, null);
  }

  public SQLException create(String message, String sqlState, Exception cause) {
    return createException(message, sqlState, -1, cause);
  }

  public SQLException create(String message, String sqlState, int errorCode) {
    return createException(message, sqlState, errorCode, null);
  }

  public String getSql() {
    return null;
  }

  @Override
  public String toString() {
    return "ExceptionFactory{threadId=" + threadId + '}';
  }

  public class SqlExceptionFactory extends ExceptionFactory {
    private String sql;

    public SqlExceptionFactory(
        Connection connection,
        Options options,
        HostAddress hostAddress,
        long threadId,
        Statement statement,
        String sql) {
      super(connection, options, hostAddress, threadId, statement);
      this.sql = sql;
    }

    public String getSql() {
      return sql;
    }
  }
}

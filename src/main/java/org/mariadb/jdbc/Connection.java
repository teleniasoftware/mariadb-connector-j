package org.mariadb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLSocket;
import javax.sql.*;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.client.result.Result;
import org.mariadb.jdbc.client.result.StreamingResult;
import org.mariadb.jdbc.message.client.*;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.message.server.OkPacket;
import org.mariadb.jdbc.plugin.credential.Credential;
import org.mariadb.jdbc.plugin.credential.CredentialPlugin;
import org.mariadb.jdbc.util.NativeSql;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.ConnectionState;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;
import org.mariadb.jdbc.util.options.Options;

public class Connection implements java.sql.Connection, PooledConnection {

  private final Socket socket;
  private final ConnectionContext context;
  private final AtomicInteger sequence = new AtomicInteger(0);
  private final AtomicBoolean close = new AtomicBoolean();
  private final ReentrantLock lock = new ReentrantLock();
  private final List<ConnectionEventListener> connectionEventListeners = new ArrayList<>();
  private final List<StatementEventListener> statementEventListeners = new ArrayList<>();
  private final Configuration conf;
  private final HostAddress hostAddress;
  private final ExceptionFactory exceptionFactory;

  private PacketWriter writer;
  private PacketReader reader;
  private int lowercaseTableNames = -1;
  private boolean readOnly;
  private boolean canUseServerTimeout;
  private int stateFlag = 0;
  private Statement stream = null;

  public Connection(Configuration conf, HostAddress hostAddress) throws SQLException {
    this.conf = conf;
    this.hostAddress = hostAddress;
    this.exceptionFactory = new ExceptionFactory(this, conf.getOptions(), hostAddress);

    List<String> galeraAllowedStates =
        conf.getOptions().galeraAllowedState == null
            ? Collections.emptyList()
            : Arrays.asList(conf.getOptions().galeraAllowedState.split(","));
    String host = hostAddress != null ? hostAddress.host : null;
    int port = hostAddress != null ? hostAddress.port : 3306;

    this.socket = ConnectionHelper.createSocket(host, port, conf.getOptions(), close);
    assignStream(this.socket, conf.getOptions());

    try {

      final InitialHandshakePacket handshake =
          InitialHandshakePacket.decode(reader.readPacket(true));
      this.exceptionFactory.setThreadId(handshake.getThreadId());
      this.context = new ConnectionContext(handshake, conf, this.exceptionFactory);
      this.reader.setServerThreadId(handshake.getThreadId(), hostAddress);
      this.writer.setServerThreadId(handshake.getThreadId(), hostAddress);
      this.canUseServerTimeout = this.context.getVersion().versionGreaterOrEqual(10, 1, 2);

      long clientCapabilities =
          ConnectionHelper.initializeClientCapabilities(conf, this.context.getServerCapabilities());
      byte exchangeCharset = ConnectionHelper.decideLanguage(handshake);

      SSLSocket sslSocket =
          ConnectionHelper.sslWrapper(
              host, socket, clientCapabilities, exchangeCharset, context, writer);

      if (sslSocket != null) assignStream(sslSocket, conf.getOptions());

      String authenticationPluginType = handshake.getAuthenticationPluginType();
      CredentialPlugin credentialPlugin = conf.getCredentialPlugin();
      if (credentialPlugin != null && credentialPlugin.defaultAuthenticationPluginType() != null) {
        authenticationPluginType = credentialPlugin.defaultAuthenticationPluginType();
      }
      Credential credential = ConnectionHelper.loadCredential(credentialPlugin, conf, hostAddress);

      new HandshakeResponse(
              authenticationPluginType,
              context.getSeed(),
              conf,
              host,
              clientCapabilities,
              exchangeCharset)
          .encode(writer, context);
      writer.flush();

      ConnectionHelper.authenticationHandler(credential, writer, reader, context);

      ConnectionHelper.compressionHandler(conf, context);

    } catch (IOException ioException) {
      destroySocket();
      if (host == null) {
        throw exceptionFactory.create(
            String.format("Could not connect to socket : %s", ioException.getMessage()),
            "08000",
            ioException);
      }

      throw exceptionFactory.create(
          String.format(
              "Could not connect to %s:%s : %s", host, socket.getPort(), ioException.getMessage()),
          "08000",
          ioException);
    } catch (SQLException sqlException) {
      destroySocket();
      throw sqlException;
    }

    postConnectionQueries();

    // validate galera state
    if (hostAddress != null
        && Boolean.TRUE.equals(hostAddress.master)
        && !galeraAllowedStates.isEmpty()) {
      galeraStateValidation();
    }
  }

  private void assignStream(Socket socket, Options options) throws SQLException {
    try {
      this.writer = new PacketWriter(socket.getOutputStream(), options.maxQuerySizeToLog, sequence);
      this.reader = new PacketReader(socket.getInputStream(), options, sequence);
    } catch (IOException ioe) {
      destroySocket();
      throw exceptionFactory.create("Socket error: " + ioe.getMessage(), "08000", ioe);
    }
  }

  /** Closing socket in case of Connection error after socket creation. */
  private void destroySocket() {
    close.set(true);
    try {
      this.reader.close();
    } catch (IOException ee) {
      // eat exception
    }
    try {
      this.writer.close();
    } catch (IOException ee) {
      // eat exception
    }
    try {
      this.socket.close();
    } catch (IOException ee) {
      // eat exception
    }
  }

  private void galeraStateValidation() throws SQLException {
    //    ResultSet rs;
    //    try {
    //      Results results = new Results();
    //      executeQuery(true, results, CHECK_GALERA_STATE_QUERY);
    //      results.commandEnd();
    //      rs = results.getResultSet();
    //
    //    } catch (SQLException sqle) {
    //      destroySocket();
    //      throw exceptionFactory.create("fail to validate Galera state");
    //    }
    //
    //    if (rs == null || !rs.next()) {
    //      destroySocket();
    //      throw exceptionFactory.create("fail to validate Galera state");
    //    }
    //
    //    if (!galeraAllowedStates.contains(rs.getString(2))) {
    //      destroySocket();
    //      throw exceptionFactory.create(
    //          String.format("fail to validate Galera state (State is %s)", rs.getString(2)));
    //    }
  }

  private void postConnectionQueries() throws SQLException {
    //    try {
    //
    //      if (options.usePipelineAuth
    //          && (options.socketTimeout == null
    //              || options.socketTimeout == 0
    //              || options.socketTimeout > 500)) {
    //        // set a timeout to avoid hang in case server doesn't support pipelining
    //        socket.setSoTimeout(500);
    //      }
    //
    //      boolean mustLoadAdditionalInfo = true;
    //      if (globalInfo != null) {
    //        if (globalInfo.isAutocommit() == options.autocommit) {
    //          mustLoadAdditionalInfo = false;
    //        }
    //      }
    //
    //      if (mustLoadAdditionalInfo) {
    //        Map<String, String> serverData = new TreeMap<>();
    //        if (options.usePipelineAuth && !options.createDatabaseIfNotExist) {
    //          try {
    //            sendPipelineAdditionalData();
    //            readPipelineAdditionalData(serverData);
    //          } catch (SQLException sqle) {
    //            if ("08".equals(sqle.getSQLState())) {
    //              throw sqle;
    //            }
    //            // in case pipeline is not supported
    //            // (proxy flush socket after reading first packet)
    //            additionalData(serverData);
    //          }
    //        } else {
    //          additionalData(serverData);
    //        }
    //
    //        writer.setMaxAllowedPacket(Integer.parseInt(serverData.get("max_allowed_packet")));
    //        autoIncrementIncrement = Integer.parseInt(serverData.get("auto_increment_increment"));
    //        loadCalendar(serverData.get("time_zone"), serverData.get("system_time_zone"));
    //
    //      } else {
    //
    //        writer.setMaxAllowedPacket((int) globalInfo.getMaxAllowedPacket());
    //        autoIncrementIncrement = globalInfo.getAutoIncrementIncrement();
    //        loadCalendar(globalInfo.getTimeZone(), globalInfo.getSystemTimeZone());
    //      }
    //
    //      reader.setServerThreadId(this.serverThreadId, isMasterConnection());
    //      writer.setServerThreadId(this.serverThreadId, isMasterConnection());
    //
    //      activeStreamingResult = null;
    //      hostFailed = false;
    //
    //      if (options.usePipelineAuth) {
    //        // reset timeout to configured value
    //        if (options.socketTimeout != null) {
    //          socket.setSoTimeout(options.socketTimeout);
    //        } else {
    //          socket.setSoTimeout(0);
    //        }
    //      }
    //
    //    } catch (SocketTimeoutException timeoutException) {
    //      destroySocket();
    //      String msg = "Socket error during post connection queries: " +
    // timeoutException.getMessage();
    //      if (options.usePipelineAuth) {
    //        msg +=
    //            "\nServer might not support pipelining, try disabling with option
    // `usePipelineAuth` and `useBatchMultiSend`";
    //      }
    //      throw exceptionFactory.create(msg, "08000", timeoutException);
    //    } catch (IOException ioException) {
    //      destroySocket();
    //      throw exceptionFactory.create(
    //          "Socket error during post connection queries: " + ioException.getMessage(),
    //          "08000",
    //          ioException);
    //    } catch (SQLException sqlException) {
    //      destroySocket();
    //      throw sqlException;
    //    }
  }

  /**
   * Cancels the current query - clones the current protocol and executes a query using the new
   * connection.
   *
   * @throws SQLException never thrown
   */
  public void cancelCurrentQuery() throws SQLException {
    try (Connection con = new Connection(conf, hostAddress)) {
      con.sendQuery(new QueryPacket("KILL QUERY " + context.getThreadId()));
    }
  }

  private void sendQuery(ClientMessage message) throws SQLException {
    sendQuery(
        null, null, message, 0, 0, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  protected List<Completion> sendQuery(
      Statement stmt,
      String sql,
      ClientMessage message,
      int maxRows,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetScrollType,
      boolean closeOnCompletion)
      throws SQLException {
    checkNotClosed();
    if (stream != null) stream.fetchRemaining();
    try {
      writer.initPacket();
      message.encode(writer, context);
      writer.flush();
      return readResults(
          stmt,
          sql,
          maxRows,
          fetchSize,
          resultSetConcurrency,
          resultSetScrollType,
          closeOnCompletion);
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory
          .withSql(sql)
          .create(
              "Socket error during post connection queries: " + ioException.getMessage(),
              "08000",
              ioException);
    }
  }

  private List<Completion> readResults(
      Statement stmt,
      String sql,
      int maxRows,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetScrollType,
      boolean closeOnCompletion)
      throws SQLException {
    List<Completion> completions = new ArrayList<>();
    completions.add(
        readPacket(
            stmt,
            sql,
            maxRows,
            fetchSize,
            resultSetConcurrency,
            resultSetScrollType,
            closeOnCompletion));

    while ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
      completions.add(
          readPacket(
              stmt,
              sql,
              maxRows,
              fetchSize,
              resultSetConcurrency,
              resultSetScrollType,
              closeOnCompletion));
    }
    return completions;
  }

  /**
   * Read server response packet.
   *
   * @throws SQLException if sub-result connection fail
   * @see <a href="https://mariadb.com/kb/en/mariadb/4-server-response-packets/">server response
   *     packets</a>
   */
  public Completion readPacket(
      Statement stmt,
      String sql,
      int maxRows,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetScrollType,
      boolean closeOnCompletion)
      throws SQLException {
    ReadableByteBuf buf;
    try {
      buf = reader.readPacket(true);

      switch (buf.getUnsignedByte()) {

          // *********************************************************************************************************
          // * OK response
          // *********************************************************************************************************
        case 0x00:
          stream = null;
          return OkPacket.decode(buf, context);

          // *********************************************************************************************************
          // * ERROR response
          // *********************************************************************************************************
        case 0xff:
          // force current status to in transaction to ensure rollback/commit, since command may
          // have
          // issue a transaction
          ErrorPacket errorPacket = ErrorPacket.decode(buf, context);
          stream = null;
          throw exceptionFactory
              .withSql(sql)
              .create(
                  errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

          // *********************************************************************************************************
          // * ResultSet
          // *********************************************************************************************************
        default:
          int fieldCount = buf.readLengthNotNull();

          // read columns information's
          ColumnDefinitionPacket[] ci = new ColumnDefinitionPacket[fieldCount];
          for (int i = 0; i < fieldCount; i++) {
            ci[i] = ColumnDefinitionPacket.decode(reader.readPacket(false));
          }

          if (!context.isEofDeprecated()) {
            // skip intermediate EOF
            reader.readPacket(true);
          }

          // read resultSet
          if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
            // TODO implement UPDATABLE result set
            throw new SQLFeatureNotSupportedException("TODO");
          }

          Result result;
          if (fetchSize != 0) {
            if ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
              context.setServerStatus(context.getServerStatus() - ServerStatus.MORE_RESULTS_EXISTS);
            }

            result =
                new StreamingResult(
                    stmt,
                    true,
                    ci,
                    reader,
                    context,
                    maxRows,
                    fetchSize,
                    lock,
                    resultSetScrollType,
                    closeOnCompletion);
            if (!result.loaded()) {
              stream = stmt;
            }
          } else {
            result =
                new CompleteResult(
                    stmt,
                    true,
                    ci,
                    reader,
                    context,
                    maxRows,
                    resultSetScrollType,
                    closeOnCompletion);
            stream = null;
          }
          return result;
      }
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.create("Socket error", "08000", ioException);
    }
  }

  private void closeSocket() {
    try {
      try {
        long maxCurrentMillis = System.currentTimeMillis() + 10;
        socket.shutdownOutput();
        socket.setSoTimeout(3);
        InputStream is = socket.getInputStream();
        //noinspection StatementWithEmptyBody
        while (is.read() != -1 && System.currentTimeMillis() < maxCurrentMillis) {
          // read byte
        }
      } catch (Throwable t) {
        // eat exception
      }
      writer.close();
      reader.close();
    } catch (IOException e) {
      // eat
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        // socket closed, if any error, so not throwing error
      }
    }
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new Statement(this, lock, canUseServerTimeout);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return null;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return NativeSql.parse(sql, context);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return (context.getServerStatus() & ServerStatus.AUTOCOMMIT) > 0;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit == getAutoCommit()) {
      return;
    }
    lock.lock();
    try {
      stateFlag |= ConnectionState.STATE_AUTOCOMMIT;
      sendQuery(new QueryPacket("set autocommit=" + ((autoCommit) ? "1" : "0")));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void commit() throws SQLException {
    lock.lock();
    try {
      if ((context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        sendQuery(new QueryPacket("COMMIT"));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void rollback() throws SQLException {
    lock.lock();
    try {
      if ((context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        sendQuery(new QueryPacket("ROLLBACK"));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws SQLException {
    fireConnectionClosed(new ConnectionEvent(this));
    boolean locked = false;
    if (lock != null) {
      locked = lock.tryLock();
    }

    if (this.close.compareAndSet(false, true)) {}

    try {
      writer.initPacket();
      QuitPacket.INSTANCE.encode(writer, context);
      writer.flush();
    } catch (SQLException | IOException e) {
      // eat
    }

    closeSocket();
    if (locked) {
      lock.unlock();
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return close.get();
  }

  public ConnectionContext getContext() {
    return context;
  }

  /**
   * Are table case sensitive or not . Default Value: 0 (Unix), 1 (Windows), 2 (Mac OS X). If set to
   * 0 (the default on Unix-based systems), table names and aliases and database names are compared
   * in a case-sensitive manner. If set to 1 (the default on Windows), names are stored in lowercase
   * and not compared in a case-sensitive manner. If set to 2 (the default on Mac OS X), names are
   * stored as declared, but compared in lowercase.
   *
   * @return int value.
   * @throws SQLException if a connection error occur
   */
  public int getLowercaseTableNames() throws SQLException {
    if (lowercaseTableNames == -1) {
      try (java.sql.Statement st = createStatement()) {
        try (ResultSet rs = st.executeQuery("select @@lower_case_table_names")) {
          rs.next();
          lowercaseTableNames = rs.getInt(1);
        }
      }
    }
    return lowercaseTableNames;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new DatabaseMetaData(this, this.conf);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return this.readOnly;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    lock.lock();
    try {
      if (conf.getOptions().assureReadOnly
          && this.readOnly != readOnly
          && context.getVersion().versionGreaterOrEqual(5, 6, 5)) {
        sendQuery(
            new QueryPacket("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE")));
      }
      this.readOnly = readOnly;
      stateFlag |= ConnectionState.STATE_READ_ONLY;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String getCatalog() throws SQLException {

    if ((context.getServerCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0) {
      // client session track return empty value, not null value. Java require sending null if empty
      String db = context.getDatabase();
      return (db != null && db.isEmpty()) ? null : db;
    }

    Statement stmt = createStatement();
    ResultSet rs = stmt.executeQuery("select database()");
    if (rs.next()) {
      context.setDatabase(rs.getString(1));
      return context.getDatabase();
    }
    return null;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    if ((context.getServerCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0
        && catalog == context.getDatabase()) {
      return;
    }
    lock.lock();
    try {
      stateFlag |= ConnectionState.STATE_DATABASE;
      sendQuery(new ChangeDbPacket(catalog));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {

    String sql = "SELECT @@tx_isolation";

    if (!context.getVersion().isMariaDBServer()) {
      if ((context.getVersion().getMajorVersion() >= 8
              && context.getVersion().versionGreaterOrEqual(8, 0, 3))
          || (context.getVersion().getMajorVersion() < 8
              && context.getVersion().versionGreaterOrEqual(5, 7, 20))) {
        sql = "SELECT @@transaction_isolation";
      }
    }

    ResultSet rs = createStatement().executeQuery(sql);
    if (rs.next()) {
      final String response = rs.getString(1);
      switch (response) {
        case "REPEATABLE-READ":
          return java.sql.Connection.TRANSACTION_REPEATABLE_READ;

        case "READ-UNCOMMITTED":
          return java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;

        case "READ-COMMITTED":
          return java.sql.Connection.TRANSACTION_READ_COMMITTED;

        case "SERIALIZABLE":
          return java.sql.Connection.TRANSACTION_SERIALIZABLE;

        default:
          throw exceptionFactory.create(
              String.format(
                  "Could not get transaction isolation level: Invalid value \"%s\"", response));
      }
    }
    throw exceptionFactory.create("Failed to retrieve transaction isolation");
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
    switch (level) {
      case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
        query += " READ UNCOMMITTED";
        break;
      case java.sql.Connection.TRANSACTION_READ_COMMITTED:
        query += " READ COMMITTED";
        break;
      case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
        query += " REPEATABLE READ";
        break;
      case java.sql.Connection.TRANSACTION_SERIALIZABLE:
        query += " SERIALIZABLE";
        break;
      default:
        throw new SQLException("Unsupported transaction isolation level");
    }
    lock.lock();
    try {
      stateFlag |= ConnectionState.STATE_TRANSACTION_ISOLATION;
      sendQuery(new QueryPacket(query));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkNotClosed();
    if (context.getWarning() == 0) {
      return null;
    }

    SQLWarning last = null;
    SQLWarning first = null;

    try (java.sql.Statement st = this.createStatement()) {
      try (ResultSet rs = st.executeQuery("show warnings")) {
        // returned result set has 'level', 'code' and 'message' columns, in this order.
        while (rs.next()) {
          int code = rs.getInt(2);
          String message = rs.getString(3);
          SQLWarning warning = new SQLWarning(message, null, code);
          if (first == null) {
            first = warning;
            last = warning;
          } else {
            last.setNextWarning(warning);
            last = warning;
          }
        }
      }
    }
    return first;
  }

  @Override
  public void clearWarnings() throws SQLException {
    context.setWarning(0);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {}

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {}

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {}

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return null;
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  private void checkNotClosed() throws SQLException {
    if (close.get()) {
      throw exceptionFactory.create("Connection is closed", "08000", 1220);
    }
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw exceptionFactory.create("the value supplied for timeout is negative");
    }
    lock.lock();
    try {
      sendQuery(PingPacket.INSTANCE);
      return true;
    } catch (SQLException sqle) {
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {}

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {}

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public String getSchema() throws SQLException {
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {}

  @Override
  public void abort(Executor executor) throws SQLException {}

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {}

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public java.sql.Connection getConnection() throws SQLException {
    return this;
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    connectionEventListeners.add(listener);
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener listener) {
    connectionEventListeners.remove(listener);
  }

  @Override
  public void addStatementEventListener(StatementEventListener listener) {
    statementEventListeners.add(listener);
  }

  @Override
  public void removeStatementEventListener(StatementEventListener listener) {
    statementEventListeners.remove(listener);
  }

  public void fireStatementClosed(StatementEvent event) {
    for (StatementEventListener listener : statementEventListeners) {
      listener.statementClosed(event);
    }
  }

  public void fireStatementErrorOccurred(StatementEvent event) {
    for (StatementEventListener listener : statementEventListeners) {
      listener.statementErrorOccurred(event);
    }
  }

  public void fireConnectionClosed(ConnectionEvent event) {
    for (ConnectionEventListener listener : connectionEventListeners) {
      listener.connectionClosed(event);
    }
  }

  public void fireConnectionErrorOccurred(ConnectionEvent event) {
    for (ConnectionEventListener listener : connectionEventListeners) {
      listener.connectionErrorOccurred(event);
    }
  }

  public boolean isMariaDbServer() {
    return context.getVersion().isMariaDBServer();
  }
}

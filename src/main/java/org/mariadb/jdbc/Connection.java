package org.mariadb.jdbc;

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
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;
import org.mariadb.jdbc.util.options.Options;

import javax.net.ssl.SSLSocket;
import javax.sql.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Connection implements java.sql.Connection, PooledConnection {

  private static final Logger logger = Loggers.getLogger(Connection.class);

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
  private int socketTimeout;

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

      postConnectionQueries();
      galeraStateValidation(galeraAllowedStates);

    } catch (IOException ioException) {
      destroySocket();

      String errorMsg =
          String.format(
              "Could not connect to %s:%s : %s", host, socket.getPort(), ioException.getMessage());
      if (host == null) {
        errorMsg = String.format("Could not connect to socket : %s", ioException.getMessage());
      }

      throw exceptionFactory.create(errorMsg, "08000", ioException);
    } catch (SQLException sqlException) {
      destroySocket();
      throw sqlException;
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

  private void galeraStateValidation(List<String> galeraAllowedStates) throws SQLException {
    if (hostAddress != null
        && Boolean.TRUE.equals(hostAddress.master)
        && !galeraAllowedStates.isEmpty()) {

      Statement stmt = createStatement();
      ResultSet rs = stmt.executeQuery("show status like 'wsrep_local_state'");
      if (rs == null || !rs.next()) {
        throw exceptionFactory.create("fail to validate Galera state");
      }
      if (!galeraAllowedStates.contains(rs.getString(2))) {
        throw exceptionFactory.create(
            String.format("fail to validate Galera state (State is %s)", rs.getString(2)));
      }
    }
  }

  private void postConnectionQueries() throws SQLException {
    try {
      if (conf.getOptions().socketTimeout == null
              || conf.getOptions().socketTimeout == 0
              || conf.getOptions().socketTimeout > 500) {
        // set a timeout to avoid hang in case server doesn't support pipelining
        socket.setSoTimeout(500);
      }

      ConnectionHelper.sendSessionInfos(context, conf.getOptions(), writer);
      ConnectionHelper.sendRequestSessionVariables(context, writer);

      Map<String, String> serverData = new TreeMap<>();
      readPipelineAdditionalData(serverData);

      writer.setMaxAllowedPacket(Integer.parseInt(serverData.get("max_allowed_packet")));
      //autoIncrementIncrement = Integer.parseInt(serverData.get("auto_increment_increment"));
      //loadCalendar(serverData.get("time_zone"), serverData.get("system_time_zone"));

      this.socketTimeout = conf.getOptions().socketTimeout != null ? conf.getOptions().socketTimeout : 0;
      socket.setSoTimeout(this.socketTimeout);

    } catch (IOException e) {
      throw exceptionFactory.create("Socket error during post connection queries", "08000", e);
    }
  }

  private void readPipelineAdditionalData(Map<String, String> serverData) throws SQLException {

    readResultsNoResultSet(new ArrayList<>(), "-SessionInfos-");
    try {
      readRequestSessionVariables(serverData);
    } catch (SQLException sqlException) {
      // fallback in case of galera non primary nodes that permit only show / set command,
      // not SELECT when not part of quorum
      Statement stmt = createStatement();
      ResultSet rs = stmt.executeQuery("SHOW VARIABLES WHERE Variable_name in ("
                      + "'max_allowed_packet',"
                      + "'system_time_zone',"
                      + "'time_zone',"
                      + "'auto_increment_increment')");
      while (rs.next()) {
        if (logger.isDebugEnabled()) {
          logger.debug("server data {} = {}", rs.getString(1), rs.getString(2));
        }
        serverData.put(rs.getString(1), rs.getString(2));
      }
    }
  }

  private void readRequestSessionVariables(Map<String, String> serverData) throws SQLException {
    List<Completion> results = new ArrayList<>();
    readResults("-Session Variables-", results);

    Result result = (Result) results.get(0);
    if (result != null) {
      result.next();

      serverData.put("max_allowed_packet", result.getString(1));
      serverData.put("system_time_zone", result.getString(2));
      serverData.put("time_zone", result.getString(3));
      serverData.put("auto_increment_increment", result.getString(4));

    } else {
      throw exceptionFactory.create(
          "Error reading SessionVariables results. Socket is connected ? " + socket.isConnected(),
          "08000");
    }
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

  private List<Completion> sendQuery(ClientMessage message) throws SQLException {
    return sendQuery(
        null, null, message, 0, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  protected List<Completion> sendQuery(
      Statement stmt,
      String sql,
      ClientMessage message,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    checkNotClosed();
    if (stream != null) stream.fetchRemaining();
    try {
      writer.initPacket();
      message.encode(writer, context);
      writer.flush();

      List<Completion> completions = new ArrayList<>();
      readResults(
          stmt,
          sql,
          completions,
          fetchSize,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
      return completions;
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

  protected List<OkPacket> sendBatch(List<String> sqls) throws SQLException {
    checkNotClosed();
    if (stream != null) stream.fetchRemaining();
    List<OkPacket> completions = new ArrayList<>();
    String sql = null;
    try {
      for (int i = 0; i < sqls.size(); i++) {
        sql = sqls.get(i);
        writer.initPacket();
        new QueryPacket(sql).encode(writer, context);
        writer.flush();

        readResultsNoResultSet(completions, sql);
      }
      return completions;
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.createBatchUpdate(
          completions,
          sqls.size(),
          exceptionFactory.create(
              "Socket error during post connection queries: " + ioException.getMessage(),
              "08000",
              ioException));
    } catch (SQLException sqle) {
      throw exceptionFactory.createBatchUpdate(completions, sqls.size(), sqle);
    }
  }

  private void readResultsNoResultSet(List<OkPacket> completions, String sql) throws SQLException {
    completions.add(readPacketNoResultSet(sql));
    while ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
      completions.add(readPacketNoResultSet(sql));
    }
  }

  private void readResults(String sql, List<? extends Completion> results) throws SQLException {
    readResults(
            null,
            sql,
            results,
            0,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.TYPE_FORWARD_ONLY,
            false);
  }

  private void readResults(
      Statement stmt,
      String sql,
      List<? extends Completion> completions,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    readPacket(
        stmt, sql, completions, fetchSize, resultSetConcurrency, resultSetType, closeOnCompletion);

    while ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
      readPacket(
          stmt,
          sql,
          completions,
          fetchSize,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
    }
  }

  /**
   * Read server response packet.
   *
   * @throws SQLException if sub-result connection fail
   * @see <a href="https://mariadb.com/kb/en/mariadb/4-server-response-packets/">server response
   *     packets</a>
   */
  public void readPacket(
      Statement stmt,
      String sql,
      List completions,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetType,
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
          completions.add(OkPacket.decode(buf, context));
          return;

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

          if (fetchSize != 0) {
            if ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
              context.setServerStatus(context.getServerStatus() - ServerStatus.MORE_RESULTS_EXISTS);
            }

            Result result =
                new StreamingResult(
                    stmt,
                    true,
                    ci,
                    reader,
                    context,
                    fetchSize,
                    lock,
                    resultSetType,
                    closeOnCompletion);
            if (!result.loaded()) {
              stream = stmt;
            }
            completions.add(result);
          } else {
            completions.add(
                new CompleteResult(
                    stmt, true, ci, reader, context, resultSetType, closeOnCompletion));
            stream = null;
          }
      }
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.withSql(sql).create("Socket error", "08000", ioException);
    }
  }

  /**
   * Read server response packet.
   *
   * @throws SQLException if sub-result connection fail
   * @see <a href="https://mariadb.com/kb/en/mariadb/4-server-response-packets/">server response
   *     packets</a>
   */
  public OkPacket readPacketNoResultSet(String sql) throws SQLException {
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
          for (int i = 0; i < fieldCount; i++) {
            reader.readPacket(true);
          }

          if (!context.isEofDeprecated()) {
            // skip intermediate EOF
            reader.readPacket(true);
          }

          while (true) {
            buf = reader.readPacket(false);
            switch (buf.getUnsignedByte()) {
              case 0xFF:
                ErrorPacket err = ErrorPacket.decode(buf, context);
                throw exceptionFactory.create(
                    err.getMessage(), err.getSqlState(), err.getErrorCode());

              case 0xFE:
                if ((context.isEofDeprecated() && buf.readableBytes() < 0xffffff)
                    || (!context.isEofDeprecated() && buf.readableBytes() < 8)) {
                  buf.skip(); // skip header
                  int serverStatus;
                  int warnings;

                  if (!context.isEofDeprecated()) {
                    // EOF_Packet
                    warnings = buf.readUnsignedShort();
                    serverStatus = buf.readUnsignedShort();
                  } else {
                    // OK_Packet with a 0xFE header
                    buf.skip(buf.readLengthNotNull()); // skip update count
                    buf.skip(buf.readLengthNotNull()); // skip insert id
                    serverStatus = buf.readUnsignedShort();
                    warnings = buf.readUnsignedShort();
                  }
                  context.setServerStatus(serverStatus);
                  context.setWarning(warnings);
                  while ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
                    try {
                      readPacketNoResultSet(sql);
                    } catch (SQLException e) {
                      // eat
                    }
                  }
                  throw exceptionFactory
                      .withSql(sql)
                      .create("No result-set is permit for this command");
                }
            }
          }
      }
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.withSql(sql).create("Socket error", "08000", ioException);
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
  public Statement createStatement() {
    return new Statement(
        this, lock, canUseServerTimeout, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    // TODO
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
    boolean locked = lock.tryLock();

    if (this.close.compareAndSet(false, true)) {
      try {
        writer.initPacket();
        QuitPacket.INSTANCE.encode(writer, context);
        writer.flush();
      } catch (SQLException | IOException e) {
        // eat
      }
      closeSocket();
    }

    if (locked) {
      lock.unlock();
    }
  }

  @Override
  public boolean isClosed() {
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
  public DatabaseMetaData getMetaData() {
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
  public Statement createStatement(int resultSetType, int resultSetConcurrency) {
    return new Statement(this, lock, canUseServerTimeout, resultSetType, resultSetConcurrency);
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
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    return new Statement(this, lock, canUseServerTimeout, resultSetType, resultSetConcurrency);
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
  public Clob createClob() {
    return new MariaDbClob();
  }

  @Override
  public Blob createBlob() {
    return new MariaDbBlob();
  }

  @Override
  public NClob createNClob() {
    return new MariaDbClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw exceptionFactory.notSupported("SQLXML type is not supported");
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
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    checkClientClose(name);
    checkClientValidProperty(name);

    try {
      java.sql.Statement statement = createStatement();
      statement.execute(buildClientQuery(name, value));
    } catch (SQLException sqle) {
      Map<String, ClientInfoStatus> failures = new HashMap<>();
      failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
      throw new SQLClientInfoException("unexpected error during setClientInfo", failures, sqle);
    }
  }

  private void checkClientClose(final String name) throws SQLClientInfoException {
    if (close.get()) {
      Map<String, ClientInfoStatus> failures = new HashMap<>();
      failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
      throw new SQLClientInfoException("setClientInfo() is called on closed connection", failures);
    }
  }

  private void checkClientValidProperty(final String name) throws SQLClientInfoException {
    if (name == null
        || (!"ApplicationName".equals(name)
            && !"ClientUser".equals(name)
            && !"ClientHostname".equals(name))) {
      Map<String, ClientInfoStatus> failures = new HashMap<>();
      failures.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
      throw new SQLClientInfoException(
          "setClientInfo() parameters can only be \"ApplicationName\",\"ClientUser\" or \"ClientHostname\", "
              + "but was : "
              + name,
          failures);
    }
  }

  private String buildClientQuery(final String name, final String value) {
    StringBuilder escapeQuery = new StringBuilder("SET @").append(name).append("=");
    if (value == null) {
      escapeQuery.append("null");
    } else {
      escapeQuery.append("'");
      int charsOffset = 0;
      int charsLength = value.length();
      char charValue;
      if ((context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0) {
        while (charsOffset < charsLength) {
          charValue = value.charAt(charsOffset);
          if (charValue == '\'') {
            escapeQuery.append('\''); // add a single escape quote
          }
          escapeQuery.append(charValue);
          charsOffset++;
        }
      } else {
        while (charsOffset < charsLength) {
          charValue = value.charAt(charsOffset);
          if (charValue == '\'' || charValue == '\\' || charValue == '"' || charValue == 0) {
            escapeQuery.append('\\'); // add escape slash
          }
          escapeQuery.append(charValue);
          charsOffset++;
        }
      }
      escapeQuery.append("'");
    }
    return escapeQuery.toString();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    checkNotClosed();
    if (!"ApplicationName".equals(name)
        && !"ClientUser".equals(name)
        && !"ClientHostname".equals(name)) {
      throw new SQLException(
          "name must be \"ApplicationName\", \"ClientUser\" or \"ClientHostname\", but was \""
              + name
              + "\"");
    }
    try (java.sql.Statement statement = createStatement()) {
      try (ResultSet rs = statement.executeQuery("SELECT @" + name)) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    }
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkNotClosed();
    Properties properties = new Properties();
    try (java.sql.Statement statement = createStatement()) {
      try (ResultSet rs =
          statement.executeQuery("SELECT @ApplicationName, @ClientUser, @ClientHostname")) {
        if (rs.next()) {
          if (rs.getString(1) != null) {
            properties.setProperty("ApplicationName", rs.getString(1));
          }
          if (rs.getString(2) != null) {
            properties.setProperty("ClientUser", rs.getString(2));
          }
          if (rs.getString(3) != null) {
            properties.setProperty("ClientHostname", rs.getString(3));
          }
          return properties;
        }
      }
    }
    properties.setProperty("ApplicationName", null);
    properties.setProperty("ClientUser", null);
    properties.setProperty("ClientHostname", null);
    return properties;
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    Map<String, ClientInfoStatus> propertiesExceptions = new HashMap<>();
    for (String name : new String[] {"ApplicationName", "ClientUser", "ClientHostname"}) {
      try {
        setClientInfo(name, properties.getProperty(name));
      } catch (SQLClientInfoException e) {
        propertiesExceptions.putAll(e.getFailedProperties());
      }
    }

    if (!propertiesExceptions.isEmpty()) {
      String errorMsg =
          "setClientInfo errors : the following properties where not set : "
              + propertiesExceptions.keySet();
      throw new SQLClientInfoException(errorMsg, propertiesExceptions);
    }
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw exceptionFactory.notSupported("Array type is not supported");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw exceptionFactory.notSupported("Struct type is not supported");
  }

  @Override
  public String getSchema() throws SQLException {
    // We support only catalog
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    // We support only catalog, and JDBC indicate "If the driver does not support schemas, it will
    // silently ignore this request."
  }

  @Override
  public void abort(Executor executor) throws SQLException {

    SQLPermission sqlPermission = new SQLPermission("callAbort");
    SecurityManager securityManager = System.getSecurityManager();
    if (securityManager != null) {
      securityManager.checkPermission(sqlPermission);
    }
    if (executor == null) {
      throw exceptionFactory.create("Cannot abort the connection: null executor passed");
    }

    fireConnectionClosed(new ConnectionEvent(this));
    boolean lockStatus = lock.tryLock();

    if (this.close.compareAndSet(false, true)) {
      if (!lockStatus) {
        // lock not available : query is running
        // force end by executing an KILL connection
        try (Connection con = new Connection(conf, hostAddress)) {
          con.sendQuery(new QueryPacket("KILL " + context.getThreadId()));
        } catch (SQLException e) {
          // eat
        }
      } else {
        try {
          writer.initPacket();
          QuitPacket.INSTANCE.encode(writer, context);
          writer.flush();
        } catch (SQLException | IOException e) {
          // eat
        }
      }
      closeSocket();
    }

    if (lockStatus) {
      lock.unlock();
    }
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    if (this.isClosed()) {
      throw exceptionFactory.create(
          "Connection.setNetworkTimeout cannot be called on a closed connection");
    }
    if (milliseconds < 0) {
      throw exceptionFactory.create(
          "Connection.setNetworkTimeout cannot be called with a negative timeout");
    }
    SQLPermission sqlPermission = new SQLPermission("setNetworkTimeout");
    SecurityManager securityManager = System.getSecurityManager();
    if (securityManager != null) {
      securityManager.checkPermission(sqlPermission);
    }
    try {
      stateFlag |= ConnectionState.STATE_NETWORK_TIMEOUT;
      lock.lock();
      try {
        this.socketTimeout = milliseconds;
        socket.setSoTimeout(milliseconds);
      } finally {
        lock.unlock();
      }
    } catch (SocketException se) {
      throw exceptionFactory.create("Cannot set the network timeout", se);
    }
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    checkNotClosed();
    return this.socketTimeout;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException("The receiver is not a wrapper for " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
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

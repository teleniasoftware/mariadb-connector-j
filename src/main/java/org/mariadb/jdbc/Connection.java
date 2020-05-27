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
  private final ExceptionFactory exceptionFactory;

  private PacketWriter writer;
  private PacketReader reader;
  private int lowercaseTableNames = -1;
  private boolean readOnly;
  private int stateFlag = 0;
  private Statement stream = null;

  public Connection(Configuration conf, HostAddress hostAddress) throws SQLException {
    this.conf = conf;
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

  private static String nativeSql(String sql, ConnectionContext context) throws SQLException {
    if (!sql.contains("{")) {
      return sql;
    }

    StringBuilder escapeSequenceBuf = new StringBuilder();
    StringBuilder sqlBuffer = new StringBuilder();

    char[] charArray = sql.toCharArray();
    char lastChar = 0;
    boolean inQuote = false;
    char quoteChar = 0;
    boolean inComment = false;
    boolean isSlashSlashComment = false;
    int inEscapeSeq = 0;

    for (char car : charArray) {
      if (lastChar == '\\'
          && (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) == 0) {
        sqlBuffer.append(car);
        lastChar = car;
        continue;
      }

      switch (car) {
        case '\'':
        case '"':
        case '`':
          if (!inComment) {
            if (inQuote) {
              if (quoteChar == car) {
                inQuote = false;
              }
            } else {
              inQuote = true;
              quoteChar = car;
            }
          }
          break;

        case '*':
          if (!inQuote && !inComment && lastChar == '/') {
            inComment = true;
            isSlashSlashComment = false;
          }
          break;

        case '-':
        case '/':
          if (!inQuote) {
            if (inComment) {
              if (lastChar == '*' && !isSlashSlashComment) {
                inComment = false;
              }
            } else {
              if (lastChar == car) {
                inComment = true;
                isSlashSlashComment = true;
              }
            }
          }
          break;
        case '\n':
          if (inComment && isSlashSlashComment) {
            // slash-slash and dash-dash comments ends with the end of line
            inComment = false;
          }
          break;
        case '{':
          if (!inQuote && !inComment) {
            inEscapeSeq++;
          }
          break;

        case '}':
          if (!inQuote && !inComment) {
            inEscapeSeq--;
            if (inEscapeSeq == 0) {
              escapeSequenceBuf.append(car);
              sqlBuffer.append(resolveEscapes(escapeSequenceBuf.toString(), context));
              escapeSequenceBuf.setLength(0);
              lastChar = car;
              continue;
            }
          }
          break;

        default:
          break;
      }
      lastChar = car;
      if (inEscapeSeq > 0) {
        escapeSequenceBuf.append(car);
      } else {
        sqlBuffer.append(car);
      }
    }
    if (inEscapeSeq > 0) {
      throw new SQLException(
          "Invalid escape sequence , missing closing '}' character in '" + sqlBuffer);
    }
    return sqlBuffer.toString();
  }

  private static String resolveEscapes(String escaped, ConnectionContext context)
      throws SQLException {
    int endIndex = escaped.length() - 1;
    String escapedLower = escaped.toLowerCase(Locale.ROOT);
    if (escaped.startsWith("{fn ")) {
      String resolvedParams = replaceFunctionParameter(escaped.substring(4, endIndex), context);
      return nativeSql(resolvedParams, context);
    } else if (escapedLower.startsWith("{oj ")) {
      // Outer join
      // the server supports "oj" in any case, even "oJ"
      return nativeSql(escaped.substring(4, endIndex), context);
    } else if (escaped.startsWith("{d ")) {
      // date literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{t ")) {
      // time literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{ts ")) {
      // timestamp literal
      return escaped.substring(4, endIndex);
    } else if (escaped.startsWith("{d'")) {
      // date literal, no space
      return escaped.substring(2, endIndex);
    } else if (escaped.startsWith("{t'")) {
      // time literal
      return escaped.substring(2, endIndex);
    } else if (escaped.startsWith("{ts'")) {
      // timestamp literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{call ") || escaped.startsWith("{CALL ")) {
      // We support uppercase "{CALL" only because Connector/J supports it. It is not in the JDBC
      // spec.

      return nativeSql(escaped.substring(1, endIndex), context);
    } else if (escaped.startsWith("{escape ")) {
      return escaped.substring(1, endIndex);
    } else if (escaped.startsWith("{?")) {
      // likely ?=call(...)
      return nativeSql(escaped.substring(1, endIndex), context);
    } else if (escaped.startsWith("{ ") || escaped.startsWith("{\n")) {
      // Spaces and newlines before keyword, this is not JDBC compliant, however some it works in
      // some drivers,
      // so we support it, too
      for (int i = 2; i < escaped.length(); i++) {
        if (!Character.isWhitespace(escaped.charAt(i))) {
          return resolveEscapes("{" + escaped.substring(i), context);
        }
      }
    } else if (escaped.startsWith("{\r\n")) {
      // Spaces and newlines before keyword, this is not JDBC compliant, however some it works in
      // some drivers,
      // so we support it, too
      for (int i = 3; i < escaped.length(); i++) {
        if (!Character.isWhitespace(escaped.charAt(i))) {
          return resolveEscapes("{" + escaped.substring(i), context);
        }
      }
    }
    throw new SQLException("unknown escape sequence " + escaped);
  }

  /**
   * Helper function to replace function parameters in escaped string. 3 functions are handles :
   *
   * <ul>
   *   <li>CONVERT(value, type): replacing SQL_XXX types to convertible type, i.e SQL_BIGINT to
   *       INTEGER
   *   <li>TIMESTAMPDIFF(type, ...): replacing type SQL_TSI_XXX in type with XXX, i.e SQL_TSI_HOUR
   *       with HOUR
   *   <li>TIMESTAMPADD(type, ...): replacing type SQL_TSI_XXX in type with XXX, i.e SQL_TSI_HOUR
   *       with HOUR
   * </ul>
   *
   * <p>caution: this use MariaDB server conversion: 'SELECT CONVERT('2147483648', INTEGER)' will
   * return a BIGINT. MySQL will throw a syntax error.
   *
   * @param functionString input string
   * @return unescaped string
   */
  private static String replaceFunctionParameter(String functionString, ConnectionContext context) {

    char[] input = functionString.toCharArray();
    StringBuilder sb = new StringBuilder();
    int index;
    for (index = 0; index < input.length; index++) {
      if (input[index] != ' ') {
        break;
      }
    }

    for (;
        index < input.length
            && ((input[index] >= 'a' && input[index] <= 'z')
                || (input[index] >= 'A' && input[index] <= 'Z'));
        index++) {
      sb.append(input[index]);
    }
    String func = sb.toString().toLowerCase(Locale.ROOT);
    switch (func) {
      case "convert":
        // Handle "convert(value, type)" case
        // extract last parameter, after the last ','
        int lastCommaIndex = functionString.lastIndexOf(',');
        int firstParentheses = functionString.indexOf('(');
        String value = functionString.substring(firstParentheses + 1, lastCommaIndex);
        for (index = lastCommaIndex + 1; index < input.length; index++) {
          if (!Character.isWhitespace(input[index])) {
            break;
          }
        }

        int endParam = index + 1;
        for (; endParam < input.length; endParam++) {
          if ((input[endParam] < 'a' || input[endParam] > 'z')
              && (input[endParam] < 'A' || input[endParam] > 'Z')
              && input[endParam] != '_') {
            break;
          }
        }
        String typeParam = new String(input, index, endParam - index).toUpperCase(Locale.ROOT);
        if (typeParam.startsWith("SQL_")) {
          typeParam = typeParam.substring(4);
        }

        switch (typeParam) {
          case "BOOLEAN":
            return "1=" + value;

          case "BIGINT":
          case "SMALLINT":
          case "TINYINT":
            typeParam = "SIGNED INTEGER";
            break;

          case "BIT":
            typeParam = "UNSIGNED INTEGER";
            break;

          case "BLOB":
          case "VARBINARY":
          case "LONGVARBINARY":
          case "ROWID":
            typeParam = "BINARY";
            break;

          case "NCHAR":
          case "CLOB":
          case "NCLOB":
          case "DATALINK":
          case "VARCHAR":
          case "NVARCHAR":
          case "LONGVARCHAR":
          case "LONGNVARCHAR":
          case "SQLXML":
          case "LONGNCHAR":
            typeParam = "CHAR";
            break;

          case "DOUBLE":
          case "FLOAT":
            if (context.getVersion().isMariaDBServer()
                || context.getVersion().versionGreaterOrEqual(8, 0, 17)) {
              typeParam = "DOUBLE";
              break;
            }
            return "0.0+" + value;

          case "REAL":
          case "NUMERIC":
            typeParam = "DECIMAL";
            break;

          case "TIMESTAMP":
            typeParam = "DATETIME";
            break;

          default:
            break;
        }
        return new String(input, 0, index)
            + typeParam
            + new String(input, endParam, input.length - endParam);

      case "timestampdiff":
      case "timestampadd":
        // Skip to first parameter
        for (; index < input.length; index++) {
          if (!Character.isWhitespace(input[index]) && input[index] != '(') {
            break;
          }
        }
        if (index < input.length - 8) {
          String paramPrefix = new String(input, index, 8);
          if ("SQL_TSI_".equals(paramPrefix)) {
            return new String(input, 0, index)
                + new String(input, index + 8, input.length - (index + 8));
          }
        }
        return functionString;

      default:
        return functionString;
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

  private void sendQuery(ClientMessage message) throws SQLException {
    sendQuery(null, message, 0, 0, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  protected List<Completion> sendQuery(
      Statement stmt,
      ClientMessage message,
      int maxRows,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetScrollType, boolean closeOnCompletion)
      throws SQLException {
    checkNotClosed();
    if (stream != null) stream.fetchRemaining();
    try {
      writer.initPacket();
      message.encode(writer, context);
      writer.flush();
      return readResults(stmt, maxRows, fetchSize, resultSetConcurrency, resultSetScrollType, closeOnCompletion);
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.create(
          "Socket error during post connection queries: " + ioException.getMessage(),
          "08000",
          ioException);
    }
  }

  private List<Completion> readResults(
      Statement stmt, int maxRows, int fetchSize, int resultSetConcurrency, int resultSetScrollType, boolean closeOnCompletion)
      throws SQLException {
    List<Completion> completions = new ArrayList<>();
    readPacket(stmt, completions, maxRows, fetchSize, resultSetConcurrency, resultSetScrollType, closeOnCompletion);
    while ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
      readPacket(stmt, completions, maxRows, fetchSize, resultSetConcurrency, resultSetScrollType, closeOnCompletion);
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
  public void readPacket(
      Statement stmt,
      List<Completion> completions,
      int maxRows,
      int fetchSize,
      int resultSetConcurrency,
      int resultSetScrollType, boolean closeOnCompletion)
      throws SQLException {
    ReadableByteBuf buf;
    try {
      buf = reader.readPacket(true);

      switch (buf.getUnsignedByte()) {

          // *********************************************************************************************************
          // * OK response
          // *********************************************************************************************************
        case 0x00:
          completions.add(OkPacket.decode(buf, context));
          stream = null;
          break;

          // *********************************************************************************************************
          // * ERROR response
          // *********************************************************************************************************
        case 0xff:
          // force current status to in transaction to ensure rollback/commit, since command may
          // have
          // issue a transaction
          ErrorPacket errorPacket = ErrorPacket.decode(buf, context);
          stream = null;
          throw exceptionFactory.create(
              errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

          // *********************************************************************************************************
          // * ResultSet
          // *********************************************************************************************************
        default:
          int fieldCount = buf.readLength();

          // read columns information's
          ColumnDefinitionPacket[] ci = new ColumnDefinitionPacket[fieldCount];
          for (int i = 0; i < fieldCount; i++) {
            ci[i] = ColumnDefinitionPacket.decode(reader.readPacket(true), context);
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
            result =
                new StreamingResult(
                        stmt, true, ci, reader, context, maxRows, fetchSize, lock, resultSetScrollType,
                        closeOnCompletion);
            if (!result.loaded()) {
              stream = stmt;
            }
          } else {
            result = new CompleteResult(stmt, true, ci, reader, context, maxRows, resultSetScrollType,
                    closeOnCompletion);
            stream = null;
          }
          completions.add(result);
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
    return new Statement(this, lock);
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
    return nativeSql(sql, context);
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
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

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

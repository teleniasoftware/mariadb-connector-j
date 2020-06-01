package org.mariadb.jdbc.client;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.SocketFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.socket.SocketHandlerFunction;
import org.mariadb.jdbc.client.socket.SocketUtility;
import org.mariadb.jdbc.message.client.QueryPacket;
import org.mariadb.jdbc.message.client.SslRequestPacket;
import org.mariadb.jdbc.message.server.AuthSwitchPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.mariadb.jdbc.plugin.credential.Credential;
import org.mariadb.jdbc.plugin.credential.CredentialPlugin;
import org.mariadb.jdbc.plugin.tls.TlsSocketPlugin;
import org.mariadb.jdbc.plugin.tls.TlsSocketPluginLoader;
import org.mariadb.jdbc.util.ConfigurableSocketFactory;
import org.mariadb.jdbc.util.Security;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.options.Options;

public class ConnectionHelper {

  private static final SocketHandlerFunction socketHandler;

  static {
    SocketHandlerFunction init;
    try {
      init = SocketUtility.getSocketHandler();
    } catch (Throwable t) {
      SocketHandlerFunction defaultSocketHandler = (options, host) -> standardSocket(options, host);
      init = defaultSocketHandler;
    }
    socketHandler = init;
  }

  /**
   * Create socket accordingly to options.
   *
   * @param options Url options
   * @param host hostName ( mandatory only for named pipe)
   * @return a nex socket
   * @throws IOException if connection error occur
   */
  public static Socket createSocket(Options options, String host) throws IOException {
    return socketHandler.apply(options, host);
  }

  /**
   * Use standard socket implementation.
   *
   * @param options url options
   * @param host host to connect
   * @return socket
   * @throws IOException in case of error establishing socket.
   */
  public static Socket standardSocket(Options options, String host) throws IOException {
    SocketFactory socketFactory;
    String socketFactoryName = options.socketFactory;
    if (socketFactoryName != null) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends SocketFactory> socketFactoryClass =
            (Class<? extends SocketFactory>) Class.forName(socketFactoryName);
        if (socketFactoryClass != null) {
          Constructor<? extends SocketFactory> constructor = socketFactoryClass.getConstructor();
          socketFactory = constructor.newInstance();
          if (socketFactoryClass.isInstance(ConfigurableSocketFactory.class)) {
            ((ConfigurableSocketFactory) socketFactory).setConfiguration(options, host);
          }
          return socketFactory.createSocket();
        }
      } catch (Exception exp) {
        throw new IOException(
            "Socket factory failed to initialized with option \"socketFactory\" set to \""
                + options.socketFactory
                + "\"",
            exp);
      }
    }
    socketFactory = SocketFactory.getDefault();
    return socketFactory.createSocket();
  }

  public static Socket createSocket(
      final String host, final int port, final Options options, AtomicBoolean close)
      throws SQLException {
    Socket socket;
    try {
      socket = createSocket(options, host);
      socket.setTcpNoDelay(true);

      if (options.socketTimeout != null) {
        socket.setSoTimeout(options.socketTimeout);
      }
      if (options.tcpKeepAlive) {
        socket.setKeepAlive(true);
      }
      if (options.tcpAbortiveClose) {
        socket.setSoLinger(true, 0);
      }

      // Bind the socket to a particular interface if the connection property
      // localSocketAddress has been defined.
      if (options.localSocketAddress != null) {
        InetSocketAddress localAddress = new InetSocketAddress(options.localSocketAddress, 0);
        socket.bind(localAddress);
      }

      if (!socket.isConnected()) {
        InetSocketAddress sockAddr =
            options.pipe == null ? new InetSocketAddress(host, port) : null;
        socket.connect(sockAddr, options.connectTimeout);
      }
      return socket;

    } catch (IOException ioe) {
      close.set(true);
      throw new SQLNonTransientConnectionException(
          String.format(
              "Socket fail to connect to host:%s, port:%s. %s", host, port, ioe.getMessage()),
          "08000",
          ioe);
    }
  }

  public static long initializeClientCapabilities(
      final Configuration configuration, final long serverCapabilities) {
    long capabilities =
        Capabilities.IGNORE_SPACE
            | Capabilities.CLIENT_PROTOCOL_41
            | Capabilities.TRANSACTIONS
            | Capabilities.SECURE_CONNECTION
            | Capabilities.MULTI_RESULTS
            | Capabilities.PS_MULTI_RESULTS
            | Capabilities.PLUGIN_AUTH
            | Capabilities.CONNECT_ATTRS
            | Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA
            | Capabilities.CLIENT_SESSION_TRACK;

    if (!configuration.getOptions().useAffectedRows) {
      capabilities |= Capabilities.FOUND_ROWS;
    }

    if (configuration.getOptions().allowMultiQueries
        || (configuration.getOptions().rewriteBatchedStatements)) {
      capabilities |= Capabilities.MULTI_STATEMENTS;
    }

    if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) != 0) {
      capabilities |= Capabilities.CLIENT_DEPRECATE_EOF;
    }

    if (configuration.getOptions().useCompression
        && ((serverCapabilities & Capabilities.COMPRESS) != 0)) {
      capabilities |= Capabilities.COMPRESS;
    }

    if (configuration.getOptions().interactiveClient) {
      capabilities |= Capabilities.CLIENT_INTERACTIVE;
    }

    if (!configuration.getDatabase().isEmpty()) {
      capabilities |= Capabilities.CONNECT_WITH_DB;
    }
    return capabilities;
  }

  /**
   * Default collation used for string exchanges with server. Always return 4 bytes utf8 collation
   * for server that permit it.
   *
   * @param handshake initial handshake packet
   * @return collation byte
   */
  public static byte decideLanguage(InitialHandshakePacket handshake) {
    short serverLanguage = handshake.getDefaultCollation();
    // return current server utf8mb4 collation
    if (serverLanguage == 45 // utf8mb4_general_ci
        || serverLanguage == 46 // utf8mb4_bin
        || (serverLanguage >= 224 && serverLanguage <= 247)) {
      return (byte) serverLanguage;
    }
    if (handshake.getMajorServerVersion() == 5 && handshake.getMinorServerVersion() <= 1) {
      // 5.1 version doesn't know 4 bytes utf8
      return (byte) 33; // utf8_general_ci
    }
    return (byte) 224; // UTF8MB4_UNICODE_CI;
  }

  public static void authenticationHandler(
      Credential credential, PacketWriter writer, PacketReader reader, ConnectionContext context)
      throws SQLException, IOException {

    writer.permitTrace(true);
    Options options = context.getConf().getOptions();

    ReadableByteBuf buf = reader.readPacket(false);

    authentication_loop:
    while (true) {
      switch (buf.getByte() & 0xFF) {
        case 0xFE:
          // *************************************************************************************
          // Authentication Switch Request see
          // https://mariadb.com/kb/en/library/connection/#authentication-switch-request
          // *************************************************************************************
          AuthSwitchPacket authSwitchPacket = AuthSwitchPacket.decode(buf, context);
          AuthenticationPlugin authenticationPlugin =
              AuthenticationPluginLoader.get(authSwitchPacket.getPlugin());

          if (authenticationPlugin.mustUseSsl() && options.useSsl == null) {
            throw context
                .getExceptionFactory()
                .create(
                    "Connector use a plugin that require SSL without enabling ssl. "
                        + "For compatibility, this can still be disabled explicitly forcing "
                        + "'useSsl=false' in connection string."
                        + "plugin is = "
                        + authenticationPlugin.type(),
                    "08004",
                    1251);
          }

          authenticationPlugin.initialize(credential.getPassword(), context.getSeed(), options);
          buf = authenticationPlugin.process(writer, reader, context);
          break;

        case 0xFF:
          // *************************************************************************************
          // ERR_Packet
          // see https://mariadb.com/kb/en/library/err_packet/
          // *************************************************************************************
          ErrorPacket errorPacket = ErrorPacket.decode(buf, context);
          if (credential.getPassword() != null
              && !credential.getPassword().isEmpty()
              && options.passwordCharacterEncoding == null
              && errorPacket.getErrorCode() == 1045
              && "28000".equals(errorPacket.getSqlState())) {
            // Access denied
            throw context
                .getExceptionFactory()
                .create(
                    String.format(
                        "%s\nCurrent charset is %s"
                            + ". If password has been set using other charset, consider "
                            + "using option 'passwordCharacterEncoding'",
                        errorPacket.getMessage(), Charset.defaultCharset().displayName()),
                    errorPacket.getSqlState(),
                    errorPacket.getErrorCode());
          }
          throw context
              .getExceptionFactory()
              .create(
                  errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

        case 0x00:
          // *************************************************************************************
          // OK_Packet -> Authenticated !
          // see https://mariadb.com/kb/en/library/ok_packet/
          // *************************************************************************************
          buf.skip(); // 0x00 OkPacket Header
          buf.skip(buf.readLengthNotNull()); // affectedRows
          buf.skip(buf.readLengthNotNull());
          ; // insertId
          context.setServerStatus(buf.readShort());
          break authentication_loop;

        default:
          throw context
              .getExceptionFactory()
              .create(
                  "unexpected data during authentication (header=" + (buf.getUnsignedByte()),
                  "08000");
      }
    }
    writer.permitTrace(true);
  }

  public static Credential loadCredential(
      CredentialPlugin credentialPlugin, Configuration configuration, HostAddress hostAddress)
      throws SQLException {
    if (credentialPlugin != null) {
      return credentialPlugin
          .initialize(configuration.getOptions(), configuration.getUsername(), hostAddress)
          .get();
    }
    return new Credential(configuration.getUsername(), configuration.getPassword());
  }

  public static SSLSocket sslWrapper(
      final String host,
      final Socket socket,
      long clientCapabilities,
      final byte exchangeCharset,
      ConnectionContext context,
      PacketWriter writer)
      throws SQLException, IOException {

    Options options = context.getConf().getOptions();
    if (Boolean.TRUE.equals(context.getConf().getOptions().useSsl)) {

      if ((context.getServerCapabilities() & Capabilities.SSL) == 0) {
        context
            .getExceptionFactory()
            .create("Trying to connect with ssl, but ssl not enabled in the server", "08000");
      }

      clientCapabilities |= Capabilities.SSL;
      new SslRequestPacket(clientCapabilities, exchangeCharset).encode(writer, context);
      writer.flush();

      TlsSocketPlugin socketPlugin = TlsSocketPluginLoader.get(options.tlsSocketType);
      SSLSocketFactory sslSocketFactory =
          socketPlugin.getSocketFactory(options, context.getExceptionFactory());
      SSLSocket sslSocket = socketPlugin.createSocket(socket, sslSocketFactory);

      enabledSslProtocolSuites(sslSocket, options);
      enabledSslCipherSuites(sslSocket, options);

      sslSocket.setUseClientMode(true);
      sslSocket.startHandshake();

      // perform hostname verification
      // (rfc2818 indicate that if "client has external information as to the expected identity of
      // the server, the hostname check MAY be omitted")
      if (!options.disableSslHostnameVerification && !options.trustServerCertificate) {
        SSLSession session = sslSocket.getSession();
        try {
          socketPlugin.verify(host, session, options, context.getThreadId());
        } catch (SSLException ex) {
          throw context
              .getExceptionFactory()
              .create(
                  "SSL hostname verification failed : "
                      + ex.getMessage()
                      + "\nThis verification can be disabled using the option \"disableSslHostnameVerification\" "
                      + "but won't prevent man-in-the-middle attacks anymore",
                  "08006");
        }
      }
      return sslSocket;
    }
    return null;
  }

  /**
   * Return possible protocols : values of option enabledSslProtocolSuites is set, or default to
   * "TLSv1,TLSv1.1". MariaDB versions &ge; 10.0.15 and &ge; 5.5.41 supports TLSv1.2 if compiled
   * with openSSL (default). MySQL community versions &ge; 5.7.10 is compile with yaSSL, so max TLS
   * is TLSv1.1.
   *
   * @param sslSocket current sslSocket
   * @throws SQLException if protocol isn't a supported protocol
   */
  static void enabledSslProtocolSuites(SSLSocket sslSocket, Options options) throws SQLException {
    if (options.enabledSslProtocolSuites != null) {
      List<String> possibleProtocols = Arrays.asList(sslSocket.getSupportedProtocols());
      String[] protocols = options.enabledSslProtocolSuites.split("[,;\\s]+");
      for (String protocol : protocols) {
        if (!possibleProtocols.contains(protocol)) {
          throw new SQLException(
              "Unsupported SSL protocol '"
                  + protocol
                  + "'. Supported protocols : "
                  + possibleProtocols.toString().replace("[", "").replace("]", ""));
        }
      }
      sslSocket.setEnabledProtocols(protocols);
    }
  }

  /**
   * Set ssl socket cipher according to options.
   *
   * @param sslSocket current ssl socket
   * @throws SQLException if a cipher isn't known
   */
  static void enabledSslCipherSuites(SSLSocket sslSocket, Options options) throws SQLException {
    if (options.enabledSslCipherSuites != null) {
      List<String> possibleCiphers = Arrays.asList(sslSocket.getSupportedCipherSuites());
      String[] ciphers = options.enabledSslCipherSuites.split("[,;\\s]+");
      for (String cipher : ciphers) {
        if (!possibleCiphers.contains(cipher)) {
          throw new SQLException(
              "Unsupported SSL cipher '"
                  + cipher
                  + "'. Supported ciphers : "
                  + possibleCiphers.toString().replace("[", "").replace("]", ""));
        }
      }
      sslSocket.setEnabledCipherSuites(ciphers);
    }
  }

  public static void compressionHandler(Configuration configuration, ConnectionContext context)
      throws SQLException {
    if (configuration.getOptions().useCompression
        && ((context.getServerCapabilities() & Capabilities.COMPRESS) != 0)) {
      // TODO write compression handler
      throw new SQLFeatureNotSupportedException("Compression not implemented yet !");
    }
  }


  public static void sendSessionInfos(ConnectionContext context, Options options, PacketWriter writer) throws IOException,
          SQLException {
    // In JDBC, connection must start in autocommit mode
    // [CONJ-269] we cannot rely on serverStatus & ServerStatus.AUTOCOMMIT before this command to
    // avoid this command.
    // if autocommit=0 is set on server configuration, DB always send Autocommit on serverStatus
    // flag
    // after setting autocommit, we can rely on serverStatus value
    StringBuilder sessionOption =
            new StringBuilder("autocommit=").append(options.autocommit ? "1" : "0");
    if ((context.getServerCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0) {
      sessionOption.append(", session_track_schema=1");
      if (options.rewriteBatchedStatements) {
        sessionOption.append(", session_track_system_variables='auto_increment_increment' ");
      }
    }

    if (options.jdbcCompliantTruncation) {
      sessionOption.append(", sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')");
    }

    if (options.sessionVariables != null && !options.sessionVariables.isEmpty()) {
      sessionOption.append(",").append(Security.parseSessionVariables(options.sessionVariables));
    }
    writer.initPacket();
    new QueryPacket("set " + sessionOption.toString()).encode(writer, context);
    writer.flush();
  }

  public static void sendRequestSessionVariables(ConnectionContext context,PacketWriter writer) throws SQLException, IOException {
    writer.initPacket();
    new QueryPacket("SELECT @@max_allowed_packet,"
            + "@@system_time_zone,"
            + "@@time_zone,"
            + "@@auto_increment_increment").encode(writer, context);
    writer.flush();
  }
}

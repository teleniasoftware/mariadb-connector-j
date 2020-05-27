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

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.util.Version;
import org.mariadb.jdbc.util.constants.Capabilities;

public final class HandshakeResponse implements ClientMessage {

  private static final String _CLIENT_NAME = "_client_name";
  private static final String _CLIENT_VERSION = "_client_version";
  private static final String _SERVER_HOST = "_server_host";
  private static final String _OS = "_os";
  private static final String _PID = "_pid";
  private static final String _THREAD = "_thread";
  private static final String _JAVA_VENDOR = "_java_vendor";
  private static final String _JAVA_VERSION = "_java_version";

  private String username;
  private CharSequence password;
  private String database;
  private String connectionAttributes;
  private String host;
  private long clientCapabilities;
  private byte exchangeCharset;
  private String authenticationPluginType;
  private byte[] seed;

  public HandshakeResponse(
      String authenticationPluginType,
      byte[] seed,
      Configuration conf,
      String host,
      long clientCapabilities,
      byte exchangeCharset) {

    this.authenticationPluginType = authenticationPluginType;
    this.seed = seed;
    this.username = conf.getUsername();
    this.password = conf.getPassword();
    this.database = conf.getDatabase();
    this.connectionAttributes = conf.getOptions().connectionAttributes;
    this.host = host;
    this.clientCapabilities = clientCapabilities;
    this.exchangeCharset = exchangeCharset;
  }

  @Override
  public void encode(PacketWriter encoder, ConnectionContext context)
      throws IOException, SQLException {

    final byte[] authData;
    switch (authenticationPluginType) {
      case "mysql_clear_password":
        // TODO check that SSL is enable
        if (password == null) {
          authData = new byte[0];
        } else {
          authData = password.toString().getBytes(StandardCharsets.UTF_8);
        }
        break;

      default:
        authenticationPluginType = "mysql_native_password";
        authData = NativePasswordPacket.encrypt(password, seed);
        break;
    }

    encoder.writeInt((int) clientCapabilities);
    encoder.writeInt(1024 * 1024 * 1024);
    encoder.writeByte(exchangeCharset); // 1

    encoder.writeBytes(0x00, 19); // 19
    encoder.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag

    if (username != null && !username.isEmpty()) {
      encoder.writeString(username);
    } else {
      // to permit SSO
      encoder.writeString(System.getProperty("user.name"));
    }
    encoder.writeByte(0x00);

    if ((context.getServerCapabilities() & Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
      encoder.writeLength(authData.length);
      encoder.writeBytes(authData);
    } else if ((context.getServerCapabilities() & Capabilities.SECURE_CONNECTION) != 0) {
      encoder.writeByte((byte) authData.length);
      encoder.writeBytes(authData);
    } else {
      encoder.writeBytes(authData);
      encoder.writeByte(0x00);
    }

    if ((clientCapabilities & Capabilities.CONNECT_WITH_DB) != 0) {
      encoder.writeString(database);
      encoder.writeByte(0x00);
    }

    if ((context.getServerCapabilities() & Capabilities.PLUGIN_AUTH) != 0) {
      encoder.writeString(authenticationPluginType);
      encoder.writeByte(0x00);
    }

    if ((context.getServerCapabilities() & Capabilities.CONNECT_ATTRS) != 0) {
      writeConnectAttributes(encoder, connectionAttributes, host);
    }
  }

  private static void writeStringLengthAscii(PacketWriter encoder, String value)
      throws IOException {
    byte[] valBytes = value.getBytes(StandardCharsets.US_ASCII);
    encoder.writeLength(valBytes.length);
    encoder.writeBytes(valBytes);
  }

  private static void writeStringLength(PacketWriter encoder, String value) throws IOException {
    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
    encoder.writeLength(valBytes.length);
    encoder.writeBytes(valBytes);
  }

  private static void writeConnectAttributes(
      PacketWriter encoder, String connectionAttributes, String host)
      throws IOException, SQLException {

    encoder.mark();
    encoder.writeInt(0);

    writeStringLengthAscii(encoder, _CLIENT_NAME);
    writeStringLength(encoder, "MariaDB Connector/J");

    writeStringLengthAscii(encoder, _CLIENT_VERSION);
    writeStringLength(encoder, Version.version);

    writeStringLengthAscii(encoder, _SERVER_HOST);
    writeStringLength(encoder, (host != null) ? host : "");

    writeStringLengthAscii(encoder, _OS);
    writeStringLength(encoder, System.getProperty("os.name"));

    writeStringLengthAscii(encoder, _THREAD);
    writeStringLength(encoder, Long.toString(Thread.currentThread().getId()));

    writeStringLengthAscii(encoder, _JAVA_VENDOR);
    writeStringLength(encoder, System.getProperty("java.vendor"));

    writeStringLengthAscii(encoder, _JAVA_VERSION);
    writeStringLength(encoder, System.getProperty("java.version"));

    if (connectionAttributes != null && !connectionAttributes.isEmpty()) {
      StringTokenizer tokenizer = new StringTokenizer(connectionAttributes, ",");
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        int separator = token.indexOf(":");
        if (separator != -1) {
          writeStringLength(encoder, token.substring(0, separator));
          writeStringLength(encoder, token.substring(separator + 1));
        } else {
          writeStringLength(encoder, token);
          writeStringLength(encoder, "");
        }
      }
    }

    // write real length
    int ending = encoder.pos();
    encoder.resetMark();
    int length = ending - (encoder.pos() + 4);
    byte[] arr = new byte[4];
    arr[0] = (byte) 0xfd;
    arr[1] = (byte) length;
    arr[2] = (byte) (length >>> 8);
    arr[3] = (byte) (length >>> 16);
    encoder.writeBytes(arr);
    encoder.pos(ending);
  }

  @Override
  public String toString() {
    return "HandshakeResponse{"
        + "username='"
        + username
        + '\''
        + ", password="
        + password
        + ", database='"
        + database
        + '\''
        + ", connectionAttributes='"
        + connectionAttributes
        + '\''
        + ", host='"
        + host
        + '\''
        + ", clientCapabilities="
        + clientCapabilities
        + ", exchangeCharset="
        + exchangeCharset
        + ", authenticationPluginType='"
        + authenticationPluginType
        + '\''
        + ", seed="
        + Arrays.toString(seed)
        + '}';
  }
}

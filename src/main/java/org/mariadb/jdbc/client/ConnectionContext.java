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

package org.mariadb.jdbc.client;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public class ConnectionContext {

  private final long threadId;
  private final long serverCapabilities;
  private final byte[] seed;
  private final ServerVersion version;
  private final boolean eofDeprecated;
  private final Configuration conf;

  private String database;
  private int serverStatus;
  private int warning;
  private ExceptionFactory exceptionFactory;

  public ConnectionContext(
      InitialHandshakePacket handshake, Configuration conf, ExceptionFactory exceptionFactory) {
    this.threadId = handshake.getThreadId();
    this.seed = handshake.getSeed();
    this.serverCapabilities = handshake.getCapabilities();
    this.serverStatus = handshake.getServerStatus();
    this.version = new ServerVersion(handshake.getServerVersion(), handshake.isMariaDBServer());
    this.eofDeprecated = (serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0;
    this.conf = conf;
    this.database = conf.getDatabase();
    this.exceptionFactory = exceptionFactory;
  }

  public long getThreadId() {
    return threadId;
  }

  public byte[] getSeed() {
    return seed;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  public int getServerStatus() {
    return serverStatus;
  }

  public void setServerStatus(int serverStatus) {
    this.serverStatus = serverStatus;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public ServerVersion getVersion() {
    return version;
  }

  public boolean isEofDeprecated() {
    return eofDeprecated;
  }

  public int getWarning() {
    return warning;
  }

  public void setWarning(int warning) {
    this.warning = warning;
  }

  public ExceptionFactory getExceptionFactory() {
    return exceptionFactory;
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public String toString() {
    return "ConnectionContext{" + "threadId=" + threadId + ", version=" + version + '}';
  }
}

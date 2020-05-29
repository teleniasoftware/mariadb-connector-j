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

package org.mariadb.jdbc.message.server;

import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.constants.StateChange;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public class OkPacket implements ServerMessage, Completion {
  public static final byte TYPE = (byte) 0x00;
  private static final Logger logger = Loggers.getLogger(OkPacket.class);
  private final long affectedRows;
  private final long lastInsertId;
  private final int serverStatus;
  private final int warningCount;
  private final boolean ending;

  public OkPacket(
      long affectedRows,
      long lastInsertId,
      int serverStatus,
      int warningCount,
      final boolean ending) {
    this.affectedRows = affectedRows;
    this.lastInsertId = lastInsertId;
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.ending = ending;
  }

  public static OkPacket decode(ReadableByteBuf buf, ConnectionContext context) {
    buf.skip();
    long affectedRows = buf.readLengthNotNull();
    long lastInsertId = buf.readLengthNotNull();
    int serverStatus = buf.readUnsignedShort();
    int warningCount = buf.readUnsignedShort();

    if ((context.getServerCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0
        && buf.readableBytes() > 0) {
      buf.skip(buf.readLengthNotNull()); // skip info
      while (buf.readableBytes() > 0) {
        ReadableByteBuf stateInfo = buf.readLengthBuffer();
        if (stateInfo.readableBytes() > 0) {
          switch (stateInfo.readByte()) {
            case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
              ReadableByteBuf sessionVariableBuf = stateInfo.readLengthBuffer();
              String variable =
                  sessionVariableBuf.readString(sessionVariableBuf.readLengthNotNull());
              Integer len = sessionVariableBuf.readLength();
              String value = len == null ? null : sessionVariableBuf.readString(len);
              logger.debug("System variable change :  {} = {}", variable, value);
              break;

            case StateChange.SESSION_TRACK_SCHEMA:
              ReadableByteBuf sessionSchemaBuf = stateInfo.readLengthBuffer();
              Integer dbLen = sessionSchemaBuf.readLength();
              String database = dbLen == null ? null : sessionSchemaBuf.readString(dbLen);
              context.setDatabase(database);
              logger.debug("Database change : now is '{}'", database);
              break;

            default:
              // eat;
          }
        }
      }
    }
    context.setServerStatus(serverStatus);
    context.setWarning(warningCount);
    return new OkPacket(
        affectedRows,
        lastInsertId,
        serverStatus,
        warningCount,
        (serverStatus & ServerStatus.MORE_RESULTS_EXISTS) == 0);
  }

  public long getAffectedRows() {
    return affectedRows;
  }

  public long getLastInsertId() {
    return lastInsertId;
  }

  public int getServerStatus() {
    return serverStatus;
  }

  public int getWarningCount() {
    return warningCount;
  }

  @Override
  public boolean ending() {
    return this.ending;
  }

  @Override
  public boolean resultSetEnd() {
    return true;
  }

  @Override
  public String toString() {
    return "OkPacket{"
        + ", affectedRows="
        + affectedRows
        + ", lastInsertId="
        + lastInsertId
        + ", serverStatus="
        + serverStatus
        + ", warningCount="
        + warningCount
        + ", ending="
        + ending
        + '}';
  }
}

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

import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public final class ErrorPacket implements ServerMessage {
  private static final Logger logger = Loggers.getLogger(ErrorPacket.class);
  private final short errorCode;
  private final String message;
  private final String sqlState;

  private ErrorPacket(short errorCode, String sqlState, String message) {
    this.errorCode = errorCode;
    this.message = message;
    this.sqlState = sqlState;
  }

  public static ErrorPacket decode(ReadableByteBuf buf, ConnectionContext context) {
    buf.skip();
    short errorCode = buf.readShort();
    byte next = buf.getByte(buf.pos());
    String sqlState;
    String msg;
    if (next == (byte) '#') {
      buf.skip(); // skip '#'
      sqlState = buf.readAscii(5);
      msg = buf.readStringEof();
    } else {
      msg = buf.readStringEof();
      sqlState = "HY000";
    }
    ErrorPacket err = new ErrorPacket(errorCode, sqlState, msg);
    logger.warn("Error: {}", err.toString());

    // force current status to in transaction to ensure rollback/commit, since command may have
    // issue a transaction
    int serverStatus = context.getServerStatus();
    serverStatus |= ServerStatus.IN_TRANSACTION;
    context.setServerStatus(serverStatus);

    return err;
  }

  public short getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }

  public String getSqlState() {
    return sqlState;
  }

  @Override
  public boolean ending() {
    return true;
  }

  @Override
  public String toString() {
    return "ErrorPacket{"
        + "errorCode="
        + errorCode
        + ", message='"
        + message
        + '\''
        + ", sqlState='"
        + sqlState
        + "\'}";
  }
}

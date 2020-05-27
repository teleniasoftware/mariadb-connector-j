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

public class EofPacket implements ServerMessage {

  private final short serverStatus;
  private final short warningCount;
  private final boolean ending;
  private final boolean resultSetEnd;

  public EofPacket(
      final short serverStatus,
      final short warningCount,
      final boolean resultSetEnd,
      final boolean ending) {
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.resultSetEnd = resultSetEnd;
    this.ending = ending;
  }

  public static EofPacket decode(
      ReadableByteBuf buf, ConnectionContext context, boolean resultSetEnd) {
    buf.skip();
    short warningCount = buf.readShort();
    short serverStatus = buf.readShort();
    context.setServerStatus(serverStatus);
    return new EofPacket(
        serverStatus,
        warningCount,
        resultSetEnd,
        resultSetEnd && (serverStatus & ServerStatus.MORE_RESULTS_EXISTS) == 0);
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public short getWarningCount() {
    return warningCount;
  }

  @Override
  public boolean ending() {
    return this.ending;
  }

  @Override
  public boolean resultSetEnd() {
    return resultSetEnd;
  }

  @Override
  public String toString() {
    return "EofPacket{"
        + ", serverStatus="
        + serverStatus
        + ", warningCount="
        + warningCount
        + ", ending="
        + ending
        + '}';
  }
}

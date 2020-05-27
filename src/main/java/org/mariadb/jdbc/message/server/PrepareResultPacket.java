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
import org.mariadb.jdbc.util.constants.Capabilities;

public final class PrepareResultPacket implements ServerMessage {

  private final int statementId;
  private final int numColumns;
  private final int numParams;
  private final boolean eofDeprecated;

  private PrepareResultPacket(
      final int statementId,
      final int numColumns,
      final int numParams,
      final boolean eofDeprecated) {
    this.statementId = statementId;
    this.numColumns = numColumns;
    this.numParams = numParams;
    this.eofDeprecated = eofDeprecated;
  }

  @Override
  public boolean ending() {
    return numParams == 0 && numColumns == 0 && eofDeprecated;
  }

  public static PrepareResultPacket decode(ReadableByteBuf buffer, ConnectionContext context) {
    /* Prepared Statement OK */
    buffer.readByte(); /* skip field count */
    final int statementId = buffer.readInt();
    final int numColumns = buffer.readUnsignedShort();
    final int numParams = buffer.readUnsignedShort();
    return new PrepareResultPacket(
        statementId,
        numColumns,
        numParams,
        ((context.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0));
  }

  public int getStatementId() {
    return statementId;
  }

  public int getNumColumns() {
    return numColumns;
  }

  public int getNumParams() {
    return numParams;
  }

  public boolean isEofDeprecated() {
    return eofDeprecated;
  }

  @Override
  public String toString() {
    return "PrepareResultPacket{"
        + "statementId="
        + statementId
        + ", numColumns="
        + numColumns
        + ", numParams="
        + numParams
        + '}';
  }
}

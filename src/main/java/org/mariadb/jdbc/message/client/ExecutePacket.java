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
import java.sql.SQLException;
import java.util.Map;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.codec.Parameter;

public final class ExecutePacket implements ClientMessage {
  private final Map<Integer, Parameter<?>> parameters;
  private final int statementId;

  public ExecutePacket(int statementId, Map<Integer, Parameter<?>> parameters) {
    this.parameters = parameters;
    this.statementId = statementId;
  }

  @Override
  public void encode(PacketWriter encoder, ConnectionContext context)
      throws IOException, SQLException {

    encoder.writeByte(0x17);
    encoder.writeInt(statementId);
    encoder.writeByte(0x00); // NO CURSOR
    encoder.writeInt(1); // Iteration pos

    int parameterCount = parameters.size();
    // create null bitmap
    if (parameterCount > 0) {
      int nullCount = (parameterCount + 7) / 8;

      byte[] nullBitsBuffer = new byte[nullCount];
      for (int i = 0; i < parameterCount; i++) {
        if (parameters.get(i).isNull()) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
      }
      encoder.writeBytes(nullBitsBuffer);

      encoder.writeByte(0x01); // Send Parameter type flag
      // Store types of parameters in first in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        if (!parameters.get(i).canEncodeLongData()) {
          encoder.writeShort(parameters.get(i).getBinaryEncodeType().get());
        }
      }
    }

    // TODO avoid to send long data here.
    for (int i = 0; i < parameterCount; i++) {
      Parameter<?> parameter = parameters.get(i);
      if (!parameter.canEncodeLongData() && !parameter.isNull()) {
        parameter.encodeBinary(encoder, context);
      }
    }
  }

  @Override
  public String toString() {
    return "ExecutePacket{" + "statementId=" + statementId + ", parameters=" + parameters + '}';
  }
}

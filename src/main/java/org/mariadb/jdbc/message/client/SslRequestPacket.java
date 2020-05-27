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
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;

public final class SslRequestPacket implements ClientMessage {

  private long clientCapabilities;
  private byte exchangeCharset;

  public SslRequestPacket(long clientCapabilities, byte exchangeCharset) {
    this.clientCapabilities = clientCapabilities;
    this.exchangeCharset = exchangeCharset;
  }

  @Override
  public void encode(PacketWriter encoder, ConnectionContext context)
      throws IOException, SQLException {
    encoder.writeInt((int) clientCapabilities);
    encoder.writeInt(1024 * 1024 * 1024);
    encoder.writeByte(exchangeCharset); // 1 byte

    encoder.writeBytes(0x00, 19); // 19  bytes
    encoder.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag
  }

  @Override
  public String toString() {
    return "SslRequestPacket{"
        + "clientCapabilities="
        + clientCapabilities
        + ", exchangeCharset="
        + exchangeCharset
        + '}';
  }
}

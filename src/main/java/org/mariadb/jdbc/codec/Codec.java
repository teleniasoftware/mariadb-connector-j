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

package org.mariadb.jdbc.codec;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public interface Codec<T> {

  boolean canDecode(ColumnDefinitionPacket column, Class<?> type);

  boolean canEncode(Object value);

  T decodeText(ReadableByteBuf buffer, int length, ColumnDefinitionPacket column);

  T decodeBinary(ReadableByteBuf buffer, int length, ColumnDefinitionPacket column);

  void encodeText(PacketWriter encoder, ConnectionContext context, T value)
      throws IOException, SQLException;

  void encodeBinary(PacketWriter encoder, ConnectionContext context, T value)
      throws IOException, SQLException;

  default boolean canEncodeLongData() {
    return false;
  }

  default void encodeLongData(PacketWriter encoder, ConnectionContext context, T value)
      throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_LONG_DATA");
  }

  DataType getBinaryEncodeType();
}

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

package org.mariadb.jdbc.codec.list;

import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class TinyIntCodec implements Codec<Byte> {

  public static final TinyIntCodec INSTANCE = new TinyIntCodec();

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.TINYINT
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return false;
  }

  @Override
  public Byte decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    return (byte) LongCodec.parse(buf, length);
  }

  @Override
  public Byte decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    return buf.readByte();
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Byte value) {}

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Byte value) {}

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }

  @Override
  public String toString() {
    return "TinyIntCodec{}";
  }
}

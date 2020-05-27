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

import java.io.IOException;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class ShortCodec implements Codec<Short> {

  public static final ShortCodec INSTANCE = new ShortCodec();

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return (column.getType() == DataType.TINYINT
            || (column.getType() == DataType.SMALLINT && column.isSigned())
            || column.getType() == DataType.YEAR
            || column.getType() == DataType.FLOAT
            || column.getType() == DataType.DOUBLE)
        && ((type.isPrimitive() && type == Short.TYPE) || type.isAssignableFrom(Short.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Short;
  }

  @Override
  public Short decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    switch (column.getType()) {
      case DOUBLE:
        String str = buf.readAscii(length);
        return Double.valueOf(str).shortValue();

      case FLOAT:
        String str2 = buf.readAscii(length);
        return Float.valueOf(str2).shortValue();

      default:
        return (short) LongCodec.parse(buf, length);
    }
  }

  @Override
  public Short decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return buf.readUnsignedByte();
        }
        return (short) buf.readByte();

      case DOUBLE:
        return (short) buf.readDouble();

      case FLOAT:
        return (short) buf.readFloat();

      default: // YEAR and SMALLINT
        return buf.readShort();
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Short value)
      throws IOException {
    encoder.writeAscii(String.valueOf(value));
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Short value)
      throws IOException {
    encoder.writeShort(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.SMALLINT;
  }

  @Override
  public String toString() {
    return "ShortCodec{}";
  }
}

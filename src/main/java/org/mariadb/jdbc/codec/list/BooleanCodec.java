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
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class BooleanCodec implements Codec<Boolean> {

  public static final BooleanCodec INSTANCE = new BooleanCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.BIGINT,
          DataType.INTEGER,
          DataType.MEDIUMINT,
          DataType.SMALLINT,
          DataType.TINYINT,
          DataType.BIT);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Boolean.TYPE) || type.isAssignableFrom(Boolean.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Boolean;
  }

  @Override
  public Boolean decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    switch (column.getType()) {
      case BIT:
        return ByteCodec.parseBit(buf, length) != 0;
      case VARCHAR:
      case VARSTRING:
        String rawValue = buf.readAscii(length);
        return !"0".equals(rawValue);
      default:
        return LongCodec.parse(buf, length) != 0L;
    }
  }

  @Override
  public Boolean decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    switch (column.getType()) {
      case BIT:
        return ByteCodec.parseBit(buf, length) != 0;

      case TINYINT:
        return buf.readByte() != 0;

      case YEAR:
      case SMALLINT:
        return buf.readShort() != 0;

      case MEDIUMINT:
        return buf.readMedium() != 0;

      case INTEGER:
        return buf.readInt() != 0;

      case BIGINT:
        return buf.readLong() != 0;

      default:
        String rawValue = buf.readString(length);
        return !"0".equals(rawValue);
    }
  }

  @Override
  public String toString() {
    return "BooleanCodec{}";
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Boolean value)
      throws IOException {
    encoder.writeAscii(value ? "1" : "0");
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Boolean value)
      throws IOException {
    encoder.writeByte(value ? 1 : 0);
  }

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }
}

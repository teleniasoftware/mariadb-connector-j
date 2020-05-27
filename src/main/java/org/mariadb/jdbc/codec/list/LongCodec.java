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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class LongCodec implements Codec<Long> {

  public static final LongCodec INSTANCE = new LongCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.YEAR,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.DECIMAL);

  public static long parse(ReadableByteBuf buf, int length) {
    long result = 0;
    boolean negate = false;
    int idx = 0;
    if (length > 0 && buf.getByte(buf.pos()) == 45) { // minus sign
      negate = true;
      idx++;
      buf.skip(1);
    }

    while (idx++ < length) {
      result = result * 10 + buf.readByte() - 48;
    }

    if (negate) result = -1 * result;
    return result;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return (COMPATIBLE_TYPES.contains(column.getType())
            || (column.getType() == DataType.BIGINT && column.isSigned()))
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Long.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Long;
  }

  @Override
  public Long decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case YEAR:
      case BIGINT:
        return parse(buf, length);

      case DECIMAL:
      case DOUBLE:
      case FLOAT:
        String str1 = buf.readAscii(length);
        return new BigDecimal(str1).longValue();

      default:
        String str = buf.readString(length);
        return new BigInteger(str).longValueExact();
    }
  }

  @Override
  public Long decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedByte();
        }
        return (long) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedShort();
        }
        return (long) buf.readShort();

      case MEDIUMINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedMedium();
        }
        return (long) buf.readMedium();

      case INTEGER:
        if (!column.isSigned()) {
          return buf.readUnsignedInt();
        }
        return (long) buf.readInt();

      case FLOAT:
        return (long) buf.readFloat();

      case DOUBLE:
        return (long) buf.readDouble();

      case VARSTRING:
      case VARCHAR:
      case STRING:
      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readString(length)).longValue();

      default:
        return buf.readLong();
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Long value)
      throws IOException {
    encoder.writeAscii(String.valueOf(value));
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Long value)
      throws IOException {
    encoder.writeLong(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.BIGINT;
  }

  @Override
  public String toString() {
    return "LongCodec{}";
  }
}

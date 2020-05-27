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
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class IntCodec implements Codec<Integer> {

  public static final IntCodec INSTANCE = new IntCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.YEAR,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.DECIMAL);

  public static void rangeCheck(
      String className, long minValue, long maxValue, long value, ColumnDefinitionPacket col) {
    if (value < minValue || value > maxValue) {
      throw new IllegalArgumentException(
          String.format(
              "Out of range value for column '%s' : value %d  is not in %s range",
              col.getColumn(), value, className));
    }
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return (COMPATIBLE_TYPES.contains(column.getType())
            || (column.getType() == DataType.INTEGER && column.isSigned()))
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Integer.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Integer;
  }

  @Override
  public Integer decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        result = LongCodec.parse(buf, length);
        break;

      default:
        String str = buf.readAscii(length);
        result = new BigDecimal(str).longValue();
    }
    return (int) result;
  }

  @Override
  public Integer decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return (int) buf.readUnsignedByte();
        }
        return (int) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return buf.readUnsignedShort();
        }
        return (int) buf.readShort();

      case MEDIUMINT:
        if (!column.isSigned()) {
          return buf.readUnsignedMedium();
        }
        return buf.readMedium();

      case OLDDECIMAL:
      case DECIMAL:
      case VARCHAR:
      case VARSTRING:
        return new BigDecimal(buf.readAscii(length)).intValue();

      case DOUBLE:
        return (int) buf.readDouble();

      case FLOAT:
        return (int) buf.readFloat();

      default:
        return buf.readInt();
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Integer value)
      throws IOException {
    encoder.writeAscii(String.valueOf(value));
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Integer value)
      throws IOException {
    encoder.writeInt(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.INTEGER;
  }

  @Override
  public String toString() {
    return "IntCodec{}";
  }
}

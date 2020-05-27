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

public class BigIntegerCodec implements Codec<BigInteger> {

  public static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.FLOAT);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(BigInteger.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigInteger;
  }

  @Override
  public BigInteger decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    String val = buf.readAscii(length);
    switch (column.getType()) {
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
        return new BigDecimal(val).toBigInteger();

      default:
        return new BigInteger(val);
    }
  }

  @Override
  public BigInteger decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedByte());
        }
        return BigInteger.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedShort());
        }
        return BigInteger.valueOf((int) buf.readShort());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf((buf.readUnsignedMedium()));
        }
        return BigInteger.valueOf(buf.readMedium());

      case INTEGER:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedInt());
        }
        return BigInteger.valueOf(buf.readInt());

      case FLOAT:
        return BigDecimal.valueOf(buf.readFloat()).toBigInteger();

      case DOUBLE:
        return BigDecimal.valueOf(buf.readDouble()).toBigInteger();

      case DECIMAL:
        return new BigDecimal(buf.readAscii(length)).toBigInteger();

      default:
        if (column.isSigned()) return BigInteger.valueOf(buf.readLong());

        // need BIG ENDIAN, so reverse order
        byte[] bb = new byte[8];
        for (int i = 7; i >= 0; i--) {
          bb[i] = buf.readByte();
        }
        return new BigInteger(1, bb);
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, BigInteger value)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, BigInteger value)
      throws IOException {
    String asciiFormat = value.toString();
    encoder.writeLength(asciiFormat.length());
    encoder.writeAscii(asciiFormat);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DECIMAL;
  }

  @Override
  public String toString() {
    return "BigIntegerCodec{}";
  }
}

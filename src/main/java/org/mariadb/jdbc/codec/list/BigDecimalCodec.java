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

public class BigDecimalCodec implements Codec<BigDecimal> {

  public static final BigDecimalCodec INSTANCE = new BigDecimalCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.DECIMAL);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(BigDecimal.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigDecimal;
  }

  @Override
  public BigDecimal decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    String value = buf.readString(length);
    return new BigDecimal(value);
  }

  @Override
  public BigDecimal decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedByte());
        }
        return BigDecimal.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedShort());
        }
        return BigDecimal.valueOf((int) buf.readShort());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf((buf.readUnsignedMedium()));
        }
        return BigDecimal.valueOf(buf.readMedium());

      case INTEGER:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedInt());
        }
        return BigDecimal.valueOf(buf.readInt());

      case BIGINT:
        BigInteger val;
        if (column.isSigned()) {
          val = BigInteger.valueOf(buf.readLong());
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          val = new BigInteger(1, bb);
        }

        return new BigDecimal(String.valueOf(val)).setScale(column.getDecimals());

      case FLOAT:
        return BigDecimal.valueOf(buf.readFloat());

      case DOUBLE:
        return BigDecimal.valueOf(buf.readDouble());

      default:
        return new BigDecimal(buf.readString(length));
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, BigDecimal value)
      throws IOException {
    encoder.writeAscii(value.toPlainString());
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, BigDecimal value)
      throws IOException {
    String asciiFormat = value.toPlainString();
    encoder.writeLength(asciiFormat.length());
    encoder.writeAscii(asciiFormat);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DECIMAL;
  }

  @Override
  public String toString() {
    return "BigDecimalCodec{}";
  }
}

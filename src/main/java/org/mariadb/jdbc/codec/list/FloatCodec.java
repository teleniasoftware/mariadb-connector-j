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

public class FloatCodec implements Codec<Float> {

  public static final FloatCodec INSTANCE = new FloatCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.BIGINT,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Float.TYPE) || type.isAssignableFrom(Float.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Float;
  }

  @Override
  public Float decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    return Float.valueOf(buf.readAscii(length));
  }

  @Override
  public Float decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return Float.valueOf(buf.readUnsignedByte());
        }
        return Float.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return Float.valueOf(buf.readUnsignedShort());
        }
        return Float.valueOf((int) buf.readShort());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return Float.valueOf((buf.readUnsignedMedium()));
        }
        return Float.valueOf(buf.readMedium());

      case INTEGER:
        if (!column.isSigned()) {
          return Float.valueOf(buf.readUnsignedInt());
        }
        return Float.valueOf(buf.readInt());

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
        return val.floatValue();

      case DOUBLE:
        return BigDecimal.valueOf(buf.readDouble()).floatValue();

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readString(length)).floatValue();
      default:
        return buf.readFloat();
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Float value)
      throws IOException {
    encoder.writeAscii(String.valueOf(value));
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Float value)
      throws IOException {
    encoder.writeFloat(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.FLOAT;
  }

  @Override
  public String toString() {
    return "FloatCodec{}";
  }
}

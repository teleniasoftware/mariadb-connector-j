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
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class StringCodec implements Codec<String> {

  public static final StringCodec INSTANCE = new StringCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.OLDDECIMAL,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.TIMESTAMP,
          DataType.BIGINT,
          DataType.MEDIUMINT,
          DataType.DATE,
          DataType.TIME,
          DataType.DATETIME,
          DataType.YEAR,
          DataType.NEWDATE,
          DataType.JSON,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.SET,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public static String zeroFillingIfNeeded(String value, ColumnDefinitionPacket col) {
    if (col.isZeroFill()) {
      StringBuilder zeroAppendStr = new StringBuilder();
      long zeroToAdd = col.getDisplaySize() - value.length();
      while (zeroToAdd-- > 0) {
        zeroAppendStr.append("0");
      }
      return zeroAppendStr.append(value).toString();
    }
    return value;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(String.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof String;
  }

  @Override
  public String decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    if (column.getType() == DataType.BIT) {

      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
      sb.append("b'");
      boolean firstByteNonZero = false;
      for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
        boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
        if (b) {
          sb.append('1');
          firstByteNonZero = true;
        } else if (firstByteNonZero) {
          sb.append('0');
        }
      }
      sb.append("'");
      return sb.toString();
    }

    String rawValue = buf.readString(length);
    if (column.isZeroFill()) {
      return zeroFillingIfNeeded(rawValue, column);
    }
    return rawValue;
  }

  @Override
  public String decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    switch (column.getType()) {
      case BIT:
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
        sb.append("b'");
        boolean firstByteNonZero = false;
        for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
          boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
          if (b) {
            sb.append('1');
            firstByteNonZero = true;
          } else if (firstByteNonZero) {
            sb.append('0');
          }
        }
        sb.append("'");
        return sb.toString();

      case TINYINT:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedByte());
        }
        return String.valueOf((int) buf.readByte());

      case YEAR:
        String s = String.valueOf(buf.readUnsignedShort());
        while (s.length() < column.getLength()) s = "0" + s;
        return s;

      case SMALLINT:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedShort());
        }
        return String.valueOf(buf.readShort());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return String.valueOf((buf.readUnsignedMedium()));
        }
        return String.valueOf(buf.readMedium());

      case INTEGER:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedInt());
        }
        return String.valueOf(buf.readInt());

      case BIGINT:
        BigInteger val;
        if (column.isSigned()) {
          val = BigInteger.valueOf(buf.readLong());
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int ii = 7; ii >= 0; ii--) {
            bb[ii] = buf.readByte();
          }
          val = new BigInteger(1, bb);
        }

        return new BigDecimal(String.valueOf(val)).setScale(column.getDecimals()).toPlainString();

      case FLOAT:
        return String.valueOf(buf.readFloat());

      case DOUBLE:
        return String.valueOf(buf.readDouble());

      case TIME:
        long tDays = 0;
        int tHours = 0;
        int tMinutes = 0;
        int tSeconds = 0;
        long tMicroseconds = 0;
        boolean negate = false;

        if (length > 0) {
          negate = buf.readByte() == 0x01;
          if (length > 4) {
            tDays = buf.readUnsignedInt();
            if (length > 7) {
              tHours = buf.readByte();
              tMinutes = buf.readByte();
              tSeconds = buf.readByte();
              if (length > 8) {
                tMicroseconds = buf.readInt();
              }
            }
          }
        }

        Duration duration =
            Duration.ZERO
                .plusDays(tDays)
                .plusHours(tHours)
                .plusMinutes(tMinutes)
                .plusSeconds(tSeconds)
                .plusNanos(tMicroseconds * 1000);
        if (negate) return duration.negated().toString();
        return duration.toString();

      case DATE:
        int dateYear = buf.readUnsignedShort();
        int dateMonth = buf.readByte();
        int dateDay = buf.readByte();
        if (length > 4) {
          buf.skip(length - 4);
        }
        return LocalDate.of(dateYear, dateMonth, dateDay).toString();

      case DATETIME:
      case TIMESTAMP:
        int year = buf.readUnsignedShort();
        int month = buf.readByte();
        int day = buf.readByte();
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        long microseconds = 0;

        if (length > 4) {
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length > 7) {
            microseconds = buf.readUnsignedInt();
          }
        }
        return LocalDateTime.of(year, month, day, hour, minutes, seconds)
            .plusNanos(microseconds * 1000)
            .toString();

      default:
        return buf.readString(length);
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, String value)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeStringEscaped(
        value, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, String value)
      throws IOException {
    encoder.writeString(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.VARCHAR;
  }

  @Override
  public String toString() {
    return "StringCodec{}";
  }
}

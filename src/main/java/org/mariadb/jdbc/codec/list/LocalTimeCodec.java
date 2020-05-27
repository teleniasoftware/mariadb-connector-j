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
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class LocalTimeCodec implements Codec<LocalTime> {

  public static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.TIME, DataType.DATETIME, DataType.TIMESTAMP);

  public static int[] parseTime(ReadableByteBuf buf, int length) {
    String raw = buf.readString(length);
    boolean negate = raw.startsWith("-");
    if (negate) {
      raw = raw.substring(1);
    }
    String[] rawPart = raw.split(":");
    if (rawPart.length == 3) {
      int hour = Integer.parseInt(rawPart[0]);
      int minutes = Integer.parseInt(rawPart[1]);
      int seconds = Integer.parseInt(rawPart[2].substring(0, 2));
      int nanoseconds = extractNanos(raw);

      return new int[] {hour, minutes, seconds, nanoseconds};

    } else {
      throw new IllegalArgumentException(
          String.format(
              "%s cannot be parse as time. time must have" + " \"99:99:99\" format", raw));
    }
  }

  protected static int extractNanos(String timestring) {
    int index = timestring.indexOf('.');
    if (index == -1) {
      return 0;
    }
    int nanos = 0;
    for (int i = index + 1; i < index + 10; i++) {
      int digit;
      if (i >= timestring.length()) {
        digit = 0;
      } else {
        digit = timestring.charAt(i) - '0';
      }
      nanos = nanos * 10 + digit;
    }
    return nanos;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(LocalTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalTime;
  }

  @Override
  public LocalTime decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    int[] parts;
    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        parts = LocalDateTimeCodec.parseTimestamp(buf, length);
        if (parts == null) return null;
        return LocalTime.of(parts[3], parts[4], parts[5], parts[6]);

      default:
        parts = parseTime(buf, length);
        return LocalTime.of(parts[0] % 24, parts[1], parts[2], parts[3]);
    }
  }

  @Override
  public LocalTime decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;
    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        buf.skip(4); // skip year, month and day
        if (length > 4) {
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length > 7) {
            microseconds = buf.readInt();
          }
        }
        return LocalTime.of(hour, minutes, seconds).plusNanos(microseconds * 1000);

      default: // TIME
        buf.skip(1); // skip negate
        if (length > 4) {
          buf.skip(4); // skip days
          if (length > 7) {
            hour = buf.readByte();
            minutes = buf.readByte();
            seconds = buf.readByte();
            if (length > 8) {
              microseconds = buf.readInt();
            }
          }
        }
        return LocalTime.of(hour, minutes, seconds).plusNanos(microseconds * 1000);
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, LocalTime val)
      throws IOException {
    StringBuilder dateString = new StringBuilder(15);
    dateString
        .append(val.getHour() < 10 ? "0" : "")
        .append(val.getHour())
        .append(val.getMinute() < 10 ? ":0" : ":")
        .append(val.getMinute())
        .append(val.getSecond() < 10 ? ":0" : ":")
        .append(val.getSecond());

    int microseconds = val.getNano() / 1000;
    if (microseconds > 0) {
      dateString.append(".");
      if (microseconds % 1000 == 0) {
        dateString.append(Integer.toString(microseconds / 1000 + 1000).substring(1));
      } else {
        dateString.append(Integer.toString(microseconds + 1000000).substring(1));
      }
    }

    encoder.writeByte('\'');
    encoder.writeAscii(dateString.toString());
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, LocalTime value)
      throws IOException {
    int nano = value.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 12);
      encoder.writeByte((byte) 0);
      encoder.writeInt(0);
      encoder.writeByte((byte) value.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte((byte) value.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte((byte) value.get(ChronoField.SECOND_OF_MINUTE));
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 8);
      encoder.writeByte((byte) 0);
      encoder.writeInt(0);
      encoder.writeByte((byte) value.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte((byte) value.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte((byte) value.get(ChronoField.SECOND_OF_MINUTE));
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.TIME;
  }

  @Override
  public String toString() {
    return "LocalTimeCodec{}";
  }
}

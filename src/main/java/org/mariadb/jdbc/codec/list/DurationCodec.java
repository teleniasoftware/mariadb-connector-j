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
import java.time.Duration;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class DurationCodec implements Codec<Duration> {

  public static final DurationCodec INSTANCE = new DurationCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.TIME, DataType.DATETIME, DataType.TIMESTAMP);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Duration.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Duration;
  }

  @Override
  public Duration decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    int[] parts;
    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        parts = LocalDateTimeCodec.parseTimestamp(buf, length);
        if (parts == null) return null;
        return Duration.ZERO
            .plusDays(parts[2] - 1)
            .plusHours(parts[3])
            .plusMinutes(parts[4])
            .plusSeconds(parts[5])
            .plusNanos(parts[6]);

      default:
        parts = LocalTimeCodec.parseTime(buf, length);
        return Duration.ZERO
            .plusHours(parts[0])
            .plusMinutes(parts[1])
            .plusSeconds(parts[2])
            .plusNanos(parts[3]);
    }
  }

  @Override
  public Duration decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    long days = 0;
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    switch (column.getType()) {
      case TIME:
        boolean negate = false;
        if (length > 0) {
          negate = buf.readUnsignedByte() == 0x01;
          if (length > 4) {
            days = buf.readUnsignedInt();
            if (length > 7) {
              hours = buf.readByte();
              minutes = buf.readByte();
              seconds = buf.readByte();
              if (length > 8) {
                microseconds = buf.readInt();
              }
            }
          }
        }

        Duration duration =
            Duration.ZERO
                .plusDays(days)
                .plusHours(hours)
                .plusMinutes(minutes)
                .plusSeconds(seconds)
                .plusNanos(microseconds * 1000);
        if (negate) return duration.negated();
        return duration;

      default:
        buf.readUnsignedShort(); // skip year
        buf.readByte(); // skip month
        days = buf.readByte();
        if (length > 4) {
          hours = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length > 7) {
            microseconds = buf.readUnsignedInt();
          }
        }
        return Duration.ZERO
            .plusDays(days - 1)
            .plusHours(hours)
            .plusMinutes(minutes)
            .plusSeconds(seconds)
            .plusNanos(microseconds * 1000);
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Duration val)
      throws IOException {
    long s = val.getSeconds();
    long microSecond = val.getNano() / 1000;
    encoder.writeByte('\'');
    if (microSecond != 0) {
      encoder.writeAscii(
          String.format("%d:%02d:%02d.%06d", s / 3600, (s % 3600) / 60, (s % 60), microSecond));
    } else {
      encoder.writeAscii(String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60)));
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Duration value)
      throws IOException {
    int nano = value.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 12);
      encoder.writeByte((byte) (value.isNegative() ? 1 : 0));
      encoder.writeInt((int) value.toDays());
      encoder.writeByte((byte) (value.toHours() - 24 * value.toDays()));
      encoder.writeByte((byte) (value.toMinutes() - 60 * value.toHours()));
      encoder.writeByte((byte) (value.getSeconds() - 60 * value.toMinutes()));
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 8);
      encoder.writeByte((byte) (value.isNegative() ? 1 : 0));
      encoder.writeInt((int) value.toDays());
      encoder.writeByte((byte) (value.toHours() - 24 * value.toDays()));
      encoder.writeByte((byte) (value.toMinutes() - 60 * value.toHours()));
      encoder.writeByte((byte) (value.getSeconds() - 60 * value.toMinutes()));
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.TIME;
  }

  @Override
  public String toString() {
    return "DurationCodec{}";
  }
}

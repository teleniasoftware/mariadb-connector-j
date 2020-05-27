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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class LocalDateTimeCodec implements Codec<LocalDateTime> {

  public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.DATETIME, DataType.TIMESTAMP);

  public static int[] parseTimestamp(ReadableByteBuf buf, int length) {
    int nanoLen = -1;
    int[] timestampsPart = new int[] {0, 0, 0, 0, 0, 0, 0};
    int partIdx = 0;
    int idx = 0;
    while (idx++ < length) {
      byte b = buf.readByte();
      if (b == '-' || b == ' ' || b == ':') {
        partIdx++;
        continue;
      }
      if (b == '.') {
        partIdx++;
        nanoLen = 0;
        continue;
      }
      if (nanoLen >= 0) nanoLen++;
      timestampsPart[partIdx] = timestampsPart[partIdx] * 10 + b - 48;
    }
    if (timestampsPart[0] == 0 && timestampsPart[1] == 0 && timestampsPart[2] == 0) {
      if (timestampsPart[3] == 0
          && timestampsPart[4] == 0
          && timestampsPart[5] == 0
          && timestampsPart[6] == 0) return null;
      timestampsPart[1] = 1;
      timestampsPart[2] = 1;
    }

    // fix non leading tray for nanoseconds
    if (nanoLen >= 0) {
      for (int begin = 0; begin < 6 - nanoLen; begin++) {
        timestampsPart[6] = timestampsPart[6] * 10;
      }
      timestampsPart[6] = timestampsPart[6] * 1000;
    }
    return timestampsPart;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(LocalDateTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalDateTime;
  }

  @Override
  public LocalDateTime decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    if (column.getType() == DataType.TIMESTAMP || column.getType() == DataType.DATETIME) {
      int[] parts = parseTimestamp(buf, length);
      if (parts == null) return null;
      return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
          .plusNanos(parts[6]);
    }
    buf.skip(length);
    throw new IllegalArgumentException("date type not supported");
  }

  @Override
  public LocalDateTime decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

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
        .plusNanos(microseconds * 1000);
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, LocalDateTime val)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeAscii(
        val.format(val.getNano() != 0 ? TIMESTAMP_FORMAT : TIMESTAMP_FORMAT_NO_FRACTIONAL));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, LocalDateTime value)
      throws IOException {

    int nano = value.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 11);
      encoder.writeShort((short) value.get(ChronoField.YEAR));
      encoder.writeByte(value.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(value.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(value.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(value.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(value.get(ChronoField.SECOND_OF_MINUTE));
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 7);
      encoder.writeShort((short) value.get(ChronoField.YEAR));
      encoder.writeByte(value.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(value.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(value.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(value.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(value.get(ChronoField.SECOND_OF_MINUTE));
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.DATETIME;
  }

  @Override
  public String toString() {
    return "LocalDateTimeCodec{}";
  }
}

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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class LocalDateCodec implements Codec<LocalDate> {

  public static final LocalDateCodec INSTANCE = new LocalDateCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE, DataType.NEWDATE, DataType.DATETIME, DataType.TIMESTAMP, DataType.YEAR);

  public static int[] parseDate(ReadableByteBuf buf, int length) {
    int[] datePart = new int[] {0, 0, 0};
    int partIdx = 0;
    int idx = 0;
    while (idx++ < length) {
      byte b = buf.readByte();
      if (b == '-') {
        partIdx++;
        continue;
      }
      datePart[partIdx] = datePart[partIdx] * 10 + b - 48;
    }

    if (datePart[0] == 0 && datePart[1] == 0 && datePart[2] == 0) {
      return null;
    }
    return datePart;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(LocalDate.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalDate;
  }

  @Override
  public LocalDate decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    int[] parts;
    switch (column.getType()) {
      case YEAR:
        short year = (short) LongCodec.parse(buf, length);

        if (length == 2 && column.getLength() == 2) {
          // YEAR(2) - deprecated
          if (year <= 69) {
            year += 2000;
          } else {
            year += 1900;
          }
        }

        return LocalDate.of(year, 1, 1);
      case NEWDATE:
      case DATE:
        parts = parseDate(buf, length);
        break;

      default:
        parts = LocalDateTimeCodec.parseTimestamp(buf, length);
        break;
    }
    if (parts == null) return null;
    return LocalDate.of(parts[0], parts[1], parts[2]);
  }

  @Override
  public LocalDate decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {

    int year;
    int month = 1;
    int day = 1;

    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        year = buf.readUnsignedShort();
        month = buf.readByte();
        day = buf.readByte();

        if (length > 4) {
          buf.skip(length - 4);
        }
        return LocalDate.of(year, month, day);

      default:
        year = buf.readUnsignedShort();

        if (length == 2 && column.getLength() == 2) {
          // YEAR(2) - deprecated
          if (year <= 69) {
            year += 2000;
          } else {
            year += 1900;
          }
        }

        if (length >= 4) {
          month = buf.readByte();
          day = buf.readByte();
        }
        return LocalDate.of(year, month, day);
    }
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, LocalDate val)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeAscii(val.format(DateTimeFormatter.ISO_LOCAL_DATE));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, LocalDate value)
      throws IOException {
    encoder.writeByte(7); // length
    encoder.writeShort((short) value.get(ChronoField.YEAR));
    encoder.writeByte(value.get(ChronoField.MONTH_OF_YEAR));
    encoder.writeByte(value.get(ChronoField.DAY_OF_MONTH));
    encoder.writeBytes(new byte[] {0, 0, 0});
  }

  public DataType getBinaryEncodeType() {
    return DataType.DATE;
  }

  @Override
  public String toString() {
    return "LocalDateCodec{}";
  }
}

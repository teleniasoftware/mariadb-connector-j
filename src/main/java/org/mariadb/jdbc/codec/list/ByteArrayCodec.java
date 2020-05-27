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
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class ByteArrayCodec implements Codec<byte[]> {

  public static final byte[] BINARY_PREFIX = {'_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\''};

  public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.BIT,
          DataType.GEOMETRY,
          DataType.VARSTRING,
          DataType.VARCHAR);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  @Override
  public byte[] decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    return arr;
  }

  @Override
  public byte[] decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    return arr;
  }

  public boolean canEncode(Object value) {
    return value instanceof byte[];
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, byte[] value)
      throws IOException {
    encoder.writeBytes(BINARY_PREFIX);
    encoder.writeBytesEscaped(
        value, value.length, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, byte[] value)
      throws IOException {
    encoder.writeLength(value.length);
    encoder.writeBytes(value);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, ConnectionContext context, byte[] value)
      throws IOException {
    encoder.writeBytes(value);
  }

  public boolean canEncodeLongData() {
    return true;
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }

  @Override
  public String toString() {
    return "ByteArrayCodec{}";
  }
}

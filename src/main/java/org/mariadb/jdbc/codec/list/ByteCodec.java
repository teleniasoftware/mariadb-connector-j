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
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class ByteCodec implements Codec<Byte> {

  public static final ByteCodec INSTANCE = new ByteCodec();

  public static long parseBit(ReadableByteBuf buf, int length) {
    if (length == 1) {
      return buf.readUnsignedByte();
    }
    long val = 0;
    int idx = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * length);
      idx++;
    } while (idx < length);
    return val;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.BIT
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public Byte decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    Byte val = buf.readByte();
    if (length > 1) buf.skip(length - 1);
    return val;
  }

  @Override
  public Byte decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    Byte val = buf.readByte();
    if (length > 1) buf.skip(length - 1);
    return val;
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Byte value)
      throws IOException {
    encoder.writeAscii(Integer.toString((int) value));
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Byte value)
      throws IOException {
    encoder.writeByte(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }

  @Override
  public String toString() {
    return "ByteCodec{}";
  }
}

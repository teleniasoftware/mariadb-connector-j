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
import java.util.BitSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class BitSetCodec implements Codec<BitSet> {

  public static final BitSetCodec INSTANCE = new BitSetCodec();

  public static BitSet parseBit(ReadableByteBuf buf, int length) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    revertOrder(arr);
    return BitSet.valueOf(arr);
  }

  public static void revertOrder(byte[] array) {
    int i = 0;
    int j = array.length - 1;
    byte tmp;
    while (j > i) {
      tmp = array[j];
      array[j] = array[i];
      array[i] = tmp;
      j--;
      i++;
    }
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.BIT && type.isAssignableFrom(BitSet.class);
  }

  @Override
  public BitSet decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    return parseBit(buf, length);
  }

  @Override
  public BitSet decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    return parseBit(buf, length);
  }

  public boolean canEncode(Object value) {
    return value instanceof BitSet;
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, BitSet value)
      throws IOException {
    byte[] bytes = value.toByteArray();
    revertOrder(bytes);

    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    for (int i = 0; i < Byte.SIZE * bytes.length; i++)
      sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
    sb.append("'");
    encoder.writeAscii(sb.toString());
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, BitSet value)
      throws IOException {
    byte[] bytes = value.toByteArray();
    revertOrder(bytes);
    encoder.writeLength(bytes.length);
    encoder.writeBytes(bytes);
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }

  @Override
  public String toString() {
    return "BitSetCodec{}";
  }
}

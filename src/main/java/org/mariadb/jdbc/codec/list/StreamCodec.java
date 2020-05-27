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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class StreamCodec implements Codec<InputStream> {

  public static final StreamCodec INSTANCE = new StreamCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.BLOB, DataType.TINYBLOB, DataType.MEDIUMBLOB, DataType.LONGBLOB);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return false;
  }

  @Override
  public InputStream decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    ByteArrayInputStream is = new ByteArrayInputStream(buf.buf(), buf.pos(), length);
    buf.skip(length);
    return is;
  }

  @Override
  public InputStream decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    ByteArrayInputStream is = new ByteArrayInputStream(buf.buf(), buf.pos(), length);
    buf.skip(length);
    return is;
  }

  public boolean canEncode(Object value) {
    return value instanceof InputStream;
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, InputStream value)
      throws IOException {
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    byte[] array = new byte[4096];

    int len;
    while ((len = value.read(array)) > 0) {
      encoder.writeBytesEscaped(
          array, len, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, InputStream value)
      throws IOException {
    // length is not known
    byte[] blobBytes = new byte[4096];
    int pos = 0;
    byte[] array = new byte[4096];

    int len;
    while ((len = value.read(array)) > 0) {
      if (blobBytes.length - (pos + 1) < len) {
        byte[] newBlobBytes = new byte[blobBytes.length + 65536];
        System.arraycopy(blobBytes, 0, newBlobBytes, 0, blobBytes.length);
        pos = blobBytes.length;
        blobBytes = newBlobBytes;
      }
      System.arraycopy(array, 0, blobBytes, pos, len);
      pos += len;
    }
    encoder.writeLength(pos);
    encoder.writeBytes(blobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, ConnectionContext context, InputStream value)
      throws IOException {
    byte[] array = new byte[4096];

    int len;
    while ((len = value.read(array)) > 0) {
      encoder.writeBytes(array, 0, len);
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }

  public boolean canEncodeLongData() {
    return true;
  }

  @Override
  public String toString() {
    return "StreamCodec{}";
  }
}

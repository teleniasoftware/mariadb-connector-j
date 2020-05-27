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
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.EnumSet;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class BlobCodec implements Codec<Blob> {

  public static final BlobCodec INSTANCE = new BlobCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.BLOB, DataType.TINYBLOB, DataType.MEDIUMBLOB, DataType.LONGBLOB);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Blob.class);
  }

  @Override
  public Blob decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    Blob blob = new MariaDbBlob(buf.buf(), buf.pos(), length);
    buf.skip(length);
    return blob;
  }

  @Override
  public Blob decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    Blob blob = new MariaDbBlob(buf.buf(), buf.pos(), length);
    buf.skip(length);
    return blob;
  }

  public boolean canEncode(Object value) {
    return value instanceof Blob;
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Blob value)
      throws IOException, SQLException {
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    byte[] array = new byte[4096];
    InputStream is = value.getBinaryStream();
    int len;
    while ((len = is.read(array)) > 0) {
      encoder.writeBytesEscaped(
          array, len, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Blob value)
      throws IOException, SQLException {
    long length;
    InputStream is = value.getBinaryStream();
    try {
      length = value.length();

      // if not have thrown an error
      encoder.writeLength(length);
      byte[] array = new byte[4096];
      int len;
      while ((len = is.read(array)) > 0) {
        encoder.writeBytes(array, 0, len);
      }

    } catch (SQLException sqle) {

      // length is not known
      byte[] blobBytes = new byte[4096];
      int pos = 0;
      byte[] array = new byte[4096];

      int len;
      while ((len = is.read(array)) > 0) {
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
  }

  @Override
  public void encodeLongData(PacketWriter encoder, ConnectionContext context, Blob value)
      throws IOException, SQLException {
    byte[] array = new byte[4096];
    InputStream is = value.getBinaryStream();
    int len;
    while ((len = is.read(array)) > 0) {
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
    return "BlobCodec{}";
  }
}

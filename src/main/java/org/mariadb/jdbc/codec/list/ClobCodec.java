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
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.EnumSet;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class ClobCodec implements Codec<Clob> {

  public static final ClobCodec INSTANCE = new ClobCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.VARCHAR, DataType.VARSTRING, DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Clob.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Clob;
  }

  @Override
  public Clob decodeText(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    Clob clob = new MariaDbClob(buf.buf(), buf.pos(), length);
    buf.skip(length);
    return clob;
  }

  @Override
  public Clob decodeBinary(ReadableByteBuf buf, int length, ColumnDefinitionPacket column) {
    Clob clob = new MariaDbClob(buf.buf(), buf.pos(), length);
    buf.skip(length);
    return clob;
  }

  @Override
  public void encodeText(PacketWriter encoder, ConnectionContext context, Clob value)
      throws IOException, SQLException {
    Reader reader = value.getCharacterStream();
    char[] buf = new char[4096];
    int len;
    encoder.writeByte('\'');
    while ((len = reader.read(buf)) >= 0) {
      byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
      encoder.writeBytesEscaped(
          data, data.length, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, ConnectionContext context, Clob value)
      throws IOException, SQLException {
    // prefer use of encodeLongData, because length is unknown
    Reader reader = value.getCharacterStream();
    byte[] clobBytes = new byte[4096];
    int pos = 0;
    char[] buf = new char[4096];

    int len;
    while ((len = reader.read(buf)) > 0) {
      byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
      if (clobBytes.length - (pos + 1) < data.length) {
        byte[] newBlobBytes = new byte[clobBytes.length + 65536];
        System.arraycopy(clobBytes, 0, newBlobBytes, 0, clobBytes.length);
        pos = clobBytes.length;
        clobBytes = newBlobBytes;
      }
      System.arraycopy(data, 0, clobBytes, pos, data.length);
      pos += len;
    }
    encoder.writeLength(pos);
    encoder.writeBytes(clobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, ConnectionContext context, Clob value)
      throws IOException, SQLException {
    Reader reader = value.getCharacterStream();
    char[] buf = new char[4096];
    int len;
    while ((len = reader.read(buf)) >= 0) {
      byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
      encoder.writeBytes(data, 0, data.length);
    }
  }

  public boolean canEncodeLongData() {
    return true;
  }

  public DataType getBinaryEncodeType() {
    return DataType.VARCHAR;
  }

  @Override
  public String toString() {
    return "ClobCodec{}";
  }
}

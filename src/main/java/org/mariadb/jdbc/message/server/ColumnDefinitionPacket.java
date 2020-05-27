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

package org.mariadb.jdbc.message.server;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.codec.list.*;
import org.mariadb.jdbc.util.constants.ColumnFlags;

public final class ColumnDefinitionPacket implements ServerMessage {

  // This array stored character length for every collation id up to collation id 256
  // It is generated from the information schema using
  // "select  id, maxlen from information_schema.character_sets, information_schema.collations
  // where character_sets.character_set_name = collations.character_set_name order by id"
  private static final int[] maxCharlen = {
    0, 2, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 3, 2, 1, 1,
    1, 0, 1, 2, 1, 1, 1, 1,
    2, 1, 1, 1, 2, 1, 1, 1,
    1, 3, 1, 2, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 4, 4, 1,
    1, 1, 1, 1, 1, 1, 4, 4,
    0, 1, 1, 1, 4, 4, 0, 1,
    1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 0, 1, 1, 1,
    1, 1, 1, 3, 2, 2, 2, 2,
    2, 1, 2, 3, 1, 1, 1, 2,
    2, 3, 3, 1, 0, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 0, 0, 0, 0, 0, 0, 0,
    2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 0, 2, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 2,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    3, 3, 3, 3, 3, 3, 3, 3,
    3, 3, 3, 3, 3, 3, 3, 3,
    3, 3, 3, 3, 0, 3, 4, 4,
    0, 0, 0, 0, 0, 0, 0, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 0, 4, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0
  };

  private final byte[] meta;
  private int[] stringPointer;
  private int[] stringLength;
  private final int charset;
  private final long length;
  private final DataType dataType;
  private final byte decimals;
  private final int flags;
  private boolean useAliasAsName;

  private ColumnDefinitionPacket(
      byte[] meta,
      int[] stringPointer,
      int[] stringLength,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags) {
    this.meta = meta;
    this.stringPointer = stringPointer;
    this.stringLength = stringLength;
    this.charset = charset;
    this.length = length;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
  }

  public static ColumnDefinitionPacket create(String name, DataType type) {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    byte[] arr = new byte[6 + 2 * nameBytes.length];
    int[] stringPointer = new int[6];
    int[] stringLength = new int[6];

    // lenenc_str     catalog
    // lenenc_str     schema
    // lenenc_str     table
    // lenenc_str     org_table
    for (int i = 0; i < 4; i++) {
      stringLength[i] = 0;
      stringPointer[i] = i;
      arr[i] = 0;
    }

    int pos = 4;

    // lenenc_str     name
    // lenenc_str     org_name
    for (int i = 0; i < 2; i++) {
      arr[pos++] = (byte) nameBytes.length;
      System.arraycopy(nameBytes, 0, arr, pos, nameBytes.length);
      stringLength[4 + i] = (byte) nameBytes.length;
      stringPointer[4 + i] = pos;
      pos += nameBytes.length;
    }

    int len;

    /* Sensible predefined length - since we're dealing with I_S here, most char fields are 64 char long */
    switch (type) {
      case VARCHAR:
      case VARSTRING:
        len = 64 * 3; /* 3 bytes per UTF8 char */
        break;
      case SMALLINT:
        len = 5;
        break;
      case NULL:
        len = 0;
        break;
      default:
        len = 1;
        break;
    }

    return new ColumnDefinitionPacket(
        arr, stringPointer, stringLength, 33, len, type, (byte) 0, ColumnFlags.PRIMARY_KEY);
  }

  public static ColumnDefinitionPacket decode(ReadableByteBuf buf, ConnectionContext context) {
    int[] stringPointer = new int[6];
    int[] stringLength = new int[6];
    for (int i = 0; i < 6; i++) {
      stringLength[i] = buf.readLength();
      stringPointer[i] = buf.pos();
      buf.skip(stringLength[i]);
    }
    byte[] meta = new byte[buf.readableBytes() - 12];
    buf.readBytes(meta);
    int charset = buf.readUnsignedShort();
    long length = buf.readUnsignedInt();
    DataType dataType = DataType.fromServer(buf.readUnsignedByte(), charset);
    int flags = buf.readUnsignedShort();
    byte decimals = buf.readByte();
    return new ColumnDefinitionPacket(
        meta, stringPointer, stringLength, charset, length, dataType, decimals, flags);
  }

  public String getSchema() {
    return new String(meta, stringPointer[1], stringLength[1], StandardCharsets.UTF_8);
  }

  public String getTableAlias() {
    return new String(meta, stringPointer[2], stringLength[2], StandardCharsets.UTF_8);
  }

  public String getTable() {
    int index = useAliasAsName ? 2 : 3;
    return new String(meta, stringPointer[index], stringLength[index], StandardCharsets.UTF_8);
  }

  public String getColumnAlias() {
    return new String(meta, stringPointer[4], stringLength[4], StandardCharsets.UTF_8);
  }

  public String getColumn() {
    int index = useAliasAsName ? 4 : 5;
    return new String(meta, stringPointer[index], stringLength[index], StandardCharsets.UTF_8);
  }

  public int getCharset() {
    return charset;
  }

  public long getLength() {
    return length;
  }

  public DataType getType() {
    return dataType;
  }

  public byte getDecimals() {
    return decimals;
  }

  public boolean isSigned() {
    return ((flags & ColumnFlags.UNSIGNED) == 0);
  }

  public int getDisplaySize() {
    if (dataType == DataType.VARCHAR
        || dataType == DataType.JSON
        || dataType == DataType.ENUM
        || dataType == DataType.SET
        || dataType == DataType.VARSTRING
        || dataType == DataType.STRING) {
      int maxWidth = maxCharlen[charset];
      if (maxWidth == 0) {
        maxWidth = 1;
      }
      return (int) length / maxWidth;
    }
    return (int) length;
  }

  public boolean getNullability() {
    return (flags & ColumnFlags.NOT_NULL) == 0;
  }

  public boolean isPrimaryKey() {
    return ((this.flags & ColumnFlags.PRIMARY_KEY) > 0);
  }

  public boolean isUniqueKey() {
    return ((this.flags & ColumnFlags.UNIQUE_KEY) > 0);
  }

  public boolean isMultipleKey() {
    return ((this.flags & ColumnFlags.MULTIPLE_KEY) > 0);
  }

  public boolean isBlob() {
    return ((this.flags & ColumnFlags.BLOB) > 0);
  }

  public boolean isZeroFill() {
    return ((this.flags & ColumnFlags.ZEROFILL) > 0);
  }

  // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
  // like string), but have the binary flag
  public boolean isBinary() {
    return (charset == 63);
  }

  public Class<?> getJavaClass() {
    switch (dataType) {
      case TINYINT:
        return isSigned() ? Byte.class : Short.class;
      case SMALLINT:
        return isSigned() ? Short.class : Integer.class;
      case INTEGER:
        return isSigned() ? Integer.class : Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case TIMESTAMP:
      case DATETIME:
        return LocalDateTime.class;
      case BIGINT:
        return isSigned() ? Long.class : BigInteger.class;
      case MEDIUMINT:
        return Integer.class;
      case DATE:
      case NEWDATE:
        return LocalDate.class;
      case TIME:
        return Duration.class;
      case YEAR:
        return Short.class;
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
        return isBinary() ? ByteBuffer.class : String.class;
      case OLDDECIMAL:
      case DECIMAL:
        return BigDecimal.class;
      case BIT:
        return BitSetCodec.class;
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        return ByteBuffer.class;

      default:
        return null;
    }
  }

  public Codec<?> getDefaultCodec() {
    switch (dataType) {
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
        return isBinary() ? ByteArrayCodec.INSTANCE : StringCodec.INSTANCE;
      case TINYINT:
        return isSigned() ? TinyIntCodec.INSTANCE : ShortCodec.INSTANCE;
      case SMALLINT:
        return isSigned() ? ShortCodec.INSTANCE : IntCodec.INSTANCE;
      case INTEGER:
        return isSigned() ? IntCodec.INSTANCE : LongCodec.INSTANCE;
      case FLOAT:
        return FloatCodec.INSTANCE;
      case DOUBLE:
        return DoubleCodec.INSTANCE;
      case TIMESTAMP:
      case DATETIME:
        return LocalDateTimeCodec.INSTANCE;
      case BIGINT:
        return isSigned() ? LongCodec.INSTANCE : BigIntegerCodec.INSTANCE;
      case MEDIUMINT:
        return IntCodec.INSTANCE;
      case DATE:
      case NEWDATE:
        return LocalDateCodec.INSTANCE;
      case TIME:
        return DurationCodec.INSTANCE;
      case YEAR:
        return ShortCodec.INSTANCE;
      case OLDDECIMAL:
      case DECIMAL:
        return BigDecimalCodec.INSTANCE;
      case BIT:
        return BitSetCodec.INSTANCE;
      case GEOMETRY:
        return ByteArrayCodec.INSTANCE;
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
        return BlobCodec.INSTANCE;
      default:
        return null;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnDefinitionPacket that = (ColumnDefinitionPacket) o;
    return charset == that.charset
        && length == that.length
        && dataType == that.dataType
        && decimals == that.decimals
        && flags == that.flags;
  }

  @Override
  public int hashCode() {
    return Objects.hash(charset, length, dataType, decimals, flags);
  }

  @Override
  public String toString() {
    return "ColumnDefinitionPacket{"
        + "charset="
        + charset
        + ", length="
        + length
        + ", dataType="
        + dataType
        + ", decimals="
        + decimals
        + ", flags="
        + flags
        + '}';
  }

  public void useAliasAsName() {
    useAliasAsName = true;
  }
}

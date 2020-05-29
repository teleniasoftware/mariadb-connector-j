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

package org.mariadb.jdbc.codec;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public abstract class RowDecoder {
  protected static final int NULL_LENGTH = -1;

  protected ReadableByteBuf buf;
  protected ColumnDefinitionPacket[] columns;

  protected int length;
  protected int index;
  protected int columnCount;
  private Map<String, Integer> mapper = null;

  public RowDecoder(int columnCount, ColumnDefinitionPacket[] columns) {
    this.columnCount = columnCount;
    this.columns = columns;
  }

  public void setRow(ReadableByteBuf buf) {
    if (buf != null) {
      this.buf = buf;
      this.buf.mark();
    } else {
      this.buf = null;
    }
    index = -1;
  }

  protected IllegalArgumentException noDecoderException(
      ColumnDefinitionPacket column, Class<?> type) {

    if (type.isArray()) {
      if (EnumSet.of(
              DataType.TINYINT,
              DataType.SMALLINT,
              DataType.MEDIUMINT,
              DataType.INTEGER,
              DataType.BIGINT)
          .contains(column.getType())) {
        throw new IllegalArgumentException(
            String.format(
                "No decoder for type %s[] and column type %s(%s)",
                type.getComponentType().getName(),
                column.getType().toString(),
                column.isSigned() ? "signed" : "unsigned"));
      }
      throw new IllegalArgumentException(
          String.format(
              "No decoder for type %s[] and column type %s",
              type.getComponentType().getName(), column.getType().toString()));
    }
    if (EnumSet.of(
            DataType.TINYINT,
            DataType.SMALLINT,
            DataType.MEDIUMINT,
            DataType.INTEGER,
            DataType.BIGINT)
        .contains(column.getType())) {
      throw new IllegalArgumentException(
          String.format(
              "No decoder for type %s and column type %s(%s)",
              type.getName(),
              column.getType().toString(),
              column.isSigned() ? "signed" : "unsigned"));
    }
    throw new IllegalArgumentException(
        String.format(
            "No decoder for type %s and column type %s",
            type.getName(), column.getType().toString()));
  }

  public abstract void setPosition(int position);

  @SuppressWarnings("unchecked")
  public <T> T get(int index, Class<T> type) throws SQLException {
    if (buf == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    if (index < 1 || index > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, columnCount));
    }

    if (wasNull()) {
      if (type.isPrimitive()) {
        throw new SQLException(
            String.format("Cannot return null for primitive %s", type.getName()));
      }
      return null;
    }

    ColumnDefinitionPacket column = columns[index - 1];
    // type generic, return "natural" java type
    if (Object.class == type || type == null) {
      Codec<T> defaultCodec = ((Codec<T>) column.getDefaultCodec());
      return getNoCheck(index - 1, defaultCodec);
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canDecode(column, type)) {
        return getNoCheck(index - 1, (Codec<T>) codec);
      }
    }

    throw new SQLException(String.format("Not supported type '%s%'", type));
  }

  public <T> T get(int index, Codec<T> codec) throws SQLException {
    if (index < 1 || index > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, columnCount));
    }
    if (buf == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    return getNoCheck(index - 1, codec);
  }

  public abstract boolean wasNull();

  public abstract <T> T getNoCheck(int index, Codec<T> codec) throws SQLException;

  public <T> T get(String label, Codec<T> codec) throws SQLException {
    if (buf == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    return getNoCheck(getIndex(label), codec);
  }

  public int getIndex(String label) throws SQLException {
    if (mapper == null) {
      for (int i = 0; i < columnCount; i++) {
        ColumnDefinitionPacket ci = columns[i];
        String columnAlias = ci.getColumnAlias();
        if (columnAlias != null) {
          columnAlias = columnAlias.toLowerCase(Locale.ROOT);
          mapper.putIfAbsent(columnAlias, i);

          String tableName = ci.getTable();
          if (tableName != null) {
            mapper.putIfAbsent(tableName.toLowerCase(Locale.ROOT) + "." + columnAlias, i);
          }
        }
      }
    }
    Integer ind = mapper.get(label);
    if (ind == null) {
      String keys = Arrays.toString(mapper.keySet().toArray(new String[0]));
      throw new SQLException(String.format("Unknown label '%s'. Possible value %s", label, keys));
    }
    return ind;
  }
}

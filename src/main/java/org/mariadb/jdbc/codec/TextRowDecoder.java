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

import java.sql.SQLException;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class TextRowDecoder extends RowDecoder {

  public TextRowDecoder(int columnCount, ColumnDefinitionPacket[] columns) {
    super(columnCount, columns);
  }

  /**
   * Get value.
   *
   * @param index REAL index (0 = first)
   * @param codec codec
   * @return value
   * @throws SQLException if cannot decode value
   */
  public <T> T getNoCheck(int index, Codec<T> codec) throws SQLException {
    setPosition(index);
    if (length == NULL_LENGTH) {
      return null;
    }
    return codec.decodeText(buf, length, columns[index]);
  }

  public boolean wasNull() {
    return length == NULL_LENGTH;
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  public void setPosition(int newIndex) {
    if (index >= newIndex) {
      index = 0;
      buf.reset();
    } else {
      index++;
    }

    for (; index < newIndex; index++) {
      int type = this.buf.readUnsignedByte();
      switch (type) {
        case 252:
          buf.skip(buf.readUnsignedShort());
          break;
        case 253:
          buf.skip(buf.readUnsignedMedium());
          break;
        case 254:
          buf.skip((int) (4 + buf.readUnsignedInt()));
          break;
        case 251:
          break;
        default:
          buf.skip(type);
          break;
      }
    }
    short type = this.buf.readUnsignedByte();
    switch (type) {
      case 251:
        length = NULL_LENGTH;
        break;
      case 252:
        length = buf.readUnsignedShort();
        break;
      case 253:
        length = buf.readUnsignedMedium();
        break;
      case 254:
        length = (int) buf.readUnsignedInt();
        buf.skip(4);
        break;
      default:
        length = type;
        break;
    }
  }
}

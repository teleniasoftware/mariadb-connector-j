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
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class BinaryRowDecoder extends RowDecoder {

  private byte[] nullBitmap;

  public BinaryRowDecoder(int columnCount, ColumnDefinitionPacket[] columns) {
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
  @SuppressWarnings("unchecked")
  public <T> T getNoCheck(int index, Codec<T> codec) throws SQLException {
    setPosition(index);
    // check NULL-Bitmap that indicate if field is null
    if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) != 0) {
      return null;
    }
    setPosition(index);
    return codec.decodeBinary(buf, length, columns[index]);
  }

  @Override
  public void setRow(ReadableByteBuf buf) {
    if (buf != null) {
      this.buf = buf;
      this.buf.mark();
      buf.skip(1); // skip 0x00 header
      nullBitmap = new byte[(columnCount + 9) / 8];
      buf.readBytes(nullBitmap);
    } else {
      this.buf = null;
    }
    index = -1;
  }

  public boolean wasNull() {
    return (nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) == 1;
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

    for (; index <= newIndex; index++) {
      if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) == 0) {
        if (index != newIndex) {
          // skip bytes
          switch (columns[index].getType()) {
            case BIGINT:
            case DOUBLE:
              buf.skip(8);
              break;

            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
              buf.skip(4);
              break;

            case SMALLINT:
            case YEAR:
              buf.skip(2);
              break;

            case TINYINT:
              buf.skip(1);
              break;

            default:
              int type = this.buf.readUnsignedByte();
              switch (type) {
                case 251:
                  break;

                case 252:
                  this.buf.skip(this.buf.readUnsignedShort());
                  break;

                case 253:
                  this.buf.skip(this.buf.readUnsignedMedium());
                  break;

                case 254:
                  this.buf.skip((int) this.buf.readLong());
                  break;

                default:
                  this.buf.skip(type);
                  break;
              }
              break;
          }
        } else {
          // read asked field position and length
          switch (columns[index].getType()) {
            case BIGINT:
            case DOUBLE:
              length = 8;
              return;

            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
              length = 4;
              return;

            case SMALLINT:
            case YEAR:
              length = 2;
              return;

            case TINYINT:
              length = 1;
              return;

            default:
              // field with variable length
              int len = this.buf.readUnsignedByte();
              switch (len) {
                case 251:
                  // null length field
                  // must never occur
                  // null value are set in NULL-Bitmap, not send with a null length indicator.
                  throw new IllegalStateException(
                      "null data is encoded in binary protocol but NULL-Bitmap is not set");

                case 252:
                  // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
                  length = this.buf.readUnsignedShort();
                  return;

                case 253:
                  // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
                  length = this.buf.readUnsignedMedium();
                  return;

                case 254:
                  // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
                  length = (int) this.buf.readLong();
                  return;

                default:
                  // length is encoded on 1 bytes (is then less than 251)
                  length = len;
                  return;
              }
          }
        }
      }
    }
  }
}

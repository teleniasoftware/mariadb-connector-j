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

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;

public final class QueryPacket implements ClientMessage {

  private final String sql;

  public QueryPacket(String sql) {
    this.sql = sql;
  }

  @Override
  public void encode(PacketWriter encoder, ConnectionContext context)
      throws IOException, SQLException {
    encoder.writeByte(0x03);
    encoder.writeString(this.sql);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryPacket that = (QueryPacket) o;
    return Objects.equals(this.sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.sql);
  }

  @Override
  public String toString() {
    return "QueryPacket{" + "sql='" + this.sql + '\'' + '}';
  }
}

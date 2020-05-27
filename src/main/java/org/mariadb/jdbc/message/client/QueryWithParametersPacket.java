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
import java.util.Arrays;
import java.util.Objects;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.codec.Parameter;
import org.mariadb.jdbc.util.ClientPrepareResult;

public final class QueryWithParametersPacket implements ClientMessage {

  private final ClientPrepareResult prepareResult;
  private final Parameter<?>[] parameters;
  private final String[] generatedColumns;

  public QueryWithParametersPacket(
      ClientPrepareResult prepareResult, Parameter<?>[] parameters, String[] generatedColumns) {
    this.prepareResult = prepareResult;
    this.parameters = parameters;
    this.generatedColumns = generatedColumns;
  }

  @Override
  public void encode(PacketWriter encoder, ConnectionContext context)
      throws IOException, SQLException {
    String additionalReturningPart = null;
    if (generatedColumns != null) {
      additionalReturningPart =
          generatedColumns.length == 0
              ? " RETURNING *"
              : " RETURNING " + String.join(", ", generatedColumns);
    }

    encoder.writeByte(0x03);

    if (prepareResult.getParamCount() == 0) {
      encoder.writeBytes(prepareResult.getQueryParts().get(0));
      if (additionalReturningPart != null) encoder.writeString(additionalReturningPart);
    } else {
      encoder.writeBytes(prepareResult.getQueryParts().get(0));
      for (int i = 0; i < prepareResult.getParamCount(); i++) {
        parameters[i].encodeText(encoder, context);
        encoder.writeBytes(prepareResult.getQueryParts().get(i + 1));
      }
      if (additionalReturningPart != null) encoder.writeString(additionalReturningPart);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryWithParametersPacket that = (QueryWithParametersPacket) o;
    return Objects.equals(prepareResult, that.prepareResult)
        && Arrays.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(prepareResult);
    result = 31 * result + Arrays.hashCode(parameters);
    return result;
  }

  @Override
  public String toString() {
    return "QueryWithParametersPacket{"
        + "prepareResult="
        + prepareResult
        + ", parameters="
        + Arrays.toString(parameters)
        + '}';
  }
}

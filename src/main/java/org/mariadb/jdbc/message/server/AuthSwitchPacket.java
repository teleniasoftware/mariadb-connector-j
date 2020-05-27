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

import java.util.Arrays;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.ReadableByteBuf;

public class AuthSwitchPacket implements ServerMessage {

  private String plugin;
  private byte[] seed;

  public AuthSwitchPacket(String plugin, byte[] seed) {
    this.plugin = plugin;
    this.seed = seed;
  }

  public static AuthSwitchPacket decode(ReadableByteBuf buf, ConnectionContext context) {
    buf.skip(1);
    String plugin = buf.readStringNullEnd();

    byte[] seed = new byte[buf.readableBytes()];
    buf.readBytes(seed);
    return new AuthSwitchPacket(plugin, seed);
  }

  public String getPlugin() {
    return plugin;
  }

  public byte[] getSeed() {
    return seed;
  }

  @Override
  public boolean ending() {
    return true;
  }

  @Override
  public String toString() {
    return "AuthSwitchPacket{"
        + ", plugin='"
        + plugin
        + '\''
        + ", seed="
        + Arrays.toString(seed)
        + '}';
  }
}

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
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import javax.crypto.Cipher;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;

public final class Sha256PasswordPacket implements ClientMessage {

  private CharSequence password;
  private byte[] seed;
  private PublicKey publicKey;

  public Sha256PasswordPacket(CharSequence password, byte[] seed, PublicKey publicKey) {
    this.password = password;
    byte[] truncatedSeed = new byte[seed.length - 1];
    System.arraycopy(seed, 0, truncatedSeed, 0, seed.length - 1);
    this.seed = truncatedSeed;
    this.publicKey = publicKey;
  }

  /**
   * Encode password with seed and public key.
   *
   * @param publicKey public key
   * @param password password
   * @param seed seed
   * @return encoded password
   * @throws SQLException if cannot encode password
   */
  public static byte[] encrypt(PublicKey publicKey, CharSequence password, byte[] seed)
      throws SQLException {

    byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

    byte[] nullFinishedPwd = Arrays.copyOf(bytePwd, bytePwd.length + 1);
    byte[] xorBytes = new byte[nullFinishedPwd.length];
    int seedLength = seed.length;

    for (int i = 0; i < xorBytes.length; i++) {
      xorBytes[i] = (byte) (nullFinishedPwd[i] ^ seed[i % seedLength]);
    }

    try {
      Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
      cipher.init(Cipher.ENCRYPT_MODE, publicKey);
      return cipher.doFinal(xorBytes);
    } catch (Exception ex) {
      throw new SQLFeatureNotSupportedException(
          "Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
    }
  }

  @Override
  public void encode(PacketWriter encoder, ConnectionContext context)
      throws IOException, SQLException {
    if (password == null) return;
    encoder.writeBytes(encrypt(publicKey, password, seed));
  }

  @Override
  public String toString() {
    return "Sha256PasswordPacket{" + ", password=*******" + ", seed=" + Arrays.toString(seed) + '}';
  }
}

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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Arrays;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.GroupElement;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.ed25519.ScalarOps;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.spec.EdDSANamedCurveTable;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.spec.EdDSAParameterSpec;

public final class Ed25519PasswordPacket implements ClientMessage {

  private CharSequence password;
  private byte[] seed;

  public Ed25519PasswordPacket(CharSequence password, byte[] seed) {
    this.password = password;
    this.seed = seed;
  }

  private static byte[] ed25519SignWithPassword(final CharSequence password, final byte[] seed)
      throws SQLNonTransientException {

    try {
      byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

      MessageDigest hash = MessageDigest.getInstance("SHA-512");

      int mlen = seed.length;
      final byte[] sm = new byte[64 + mlen];

      byte[] az = hash.digest(bytePwd);
      az[0] &= 248;
      az[31] &= 63;
      az[31] |= 64;

      System.arraycopy(seed, 0, sm, 64, mlen);
      System.arraycopy(az, 32, sm, 32, 32);

      byte[] buff = Arrays.copyOfRange(sm, 32, 96);
      hash.reset();
      byte[] nonce = hash.digest(buff);

      org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.ed25519.ScalarOps scalar =
          new ScalarOps();

      EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName("Ed25519");
      GroupElement elementAvalue = spec.getB().scalarMultiply(az);
      byte[] elementAarray = elementAvalue.toByteArray();
      System.arraycopy(elementAarray, 0, sm, 32, elementAarray.length);

      nonce = scalar.reduce(nonce);
      GroupElement elementRvalue = spec.getB().scalarMultiply(nonce);
      byte[] elementRarray = elementRvalue.toByteArray();
      System.arraycopy(elementRarray, 0, sm, 0, elementRarray.length);

      hash.reset();
      byte[] hram = hash.digest(sm);
      hram = scalar.reduce(hram);
      byte[] tt = scalar.multiplyAndAdd(hram, az, nonce);
      System.arraycopy(tt, 0, sm, 32, tt.length);

      return Arrays.copyOfRange(sm, 0, 64);

    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-512, failing", e);
    }
  }

  @Override
  public void encode(PacketWriter encoder, ConnectionContext context)
      throws IOException, SQLException {
    if (password == null || password.toString().isEmpty()) return;
    encoder.writeBytes(ed25519SignWithPassword(password, seed));
  }

  @Override
  public String toString() {
    return "Ed25519PasswordPacket{" + "password=*******" + ", seed=" + Arrays.toString(seed) + '}';
  }
}

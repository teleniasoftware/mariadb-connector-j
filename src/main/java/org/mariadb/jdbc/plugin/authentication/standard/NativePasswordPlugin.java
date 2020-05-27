/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.plugin.authentication.standard;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketReader;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;
import org.mariadb.jdbc.util.options.Options;

public class NativePasswordPlugin implements AuthenticationPlugin {

  public static final String TYPE = "mysql_native_password";

  private String authenticationData;
  private String passwordCharacterEncoding;
  private byte[] seed;

  @Override
  public String name() {
    return "mysql native password";
  }

  @Override
  public String type() {
    return TYPE;
  }

  /**
   * Initialized data.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param options Connection string options
   */
  public void initialize(String authenticationData, byte[] seed, Options options) {
    this.seed = seed;
    this.authenticationData = authenticationData;
    this.passwordCharacterEncoding = options.passwordCharacterEncoding;
  }

  /**
   * Process native password plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-mysql_native_password/
   *
   * @param out out stream
   * @param in in stream
   * @param context connection context
   * @return response packet
   * @throws IOException if socket error
   */
  public ReadableByteBuf process(PacketWriter out, PacketReader in, ConnectionContext context)
      throws SQLException, IOException {
    if (authenticationData == null || authenticationData.isEmpty()) {
      out.writeEmptyPacket();
    } else {
      try {

        byte[] truncatedSeed;
        if (seed.length > 0) {
          // Seed is ended with a null byte value.
          truncatedSeed = Arrays.copyOfRange(seed, 0, seed.length - 1);
        } else {
          truncatedSeed = new byte[0];
        }
        out.writeBytes(
            encryptPassword(authenticationData, truncatedSeed, passwordCharacterEncoding));
        out.flush();
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("Could not use SHA-1, failing", e);
      }
    }

    return in.readPacket(true);
  }

  /**
   * Encrypts a password.
   *
   * <p>protocol for authentication is like this: 1. Server sends a random array of bytes (the seed)
   * 2. client makes a sha1 digest of the password 3. client hashes the output of 2 4. client
   * digests the seed 5. client updates the digest with the output from 3 6. an xor of the output of
   * 5 and 2 is sent to server 7. server does the same thing and verifies that the scrambled
   * passwords match
   *
   * @param password the password to encrypt
   * @param seed the seed to use
   * @param passwordCharacterEncoding password character encoding
   * @return a scrambled password
   * @throws NoSuchAlgorithmException if SHA1 is not available on the platform we are using
   * @throws UnsupportedEncodingException if passwordCharacterEncoding is not a valid charset name
   */
  public static byte[] encryptPassword(
      final String password, final byte[] seed, String passwordCharacterEncoding)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {

    if (password == null || password.isEmpty()) {
      return new byte[0];
    }

    final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    byte[] bytePwd;
    if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
      bytePwd = password.getBytes(passwordCharacterEncoding);
    } else {
      bytePwd = password.getBytes();
    }

    final byte[] stage1 = messageDigest.digest(bytePwd);
    messageDigest.reset();

    final byte[] stage2 = messageDigest.digest(stage1);
    messageDigest.reset();

    messageDigest.update(seed);
    messageDigest.update(stage2);

    final byte[] digest = messageDigest.digest();
    final byte[] returnBytes = new byte[digest.length];
    for (int i = 0; i < digest.length; i++) {
      returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
    }
    return returnBytes;
  }
}

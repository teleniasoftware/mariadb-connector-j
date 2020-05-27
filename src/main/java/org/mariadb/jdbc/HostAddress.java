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

package org.mariadb.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.mariadb.jdbc.util.constants.HaMode;

public class HostAddress {

  public String host;
  public int port;
  public Boolean master;

  /**
   * Constructor. type is master.
   *
   * @param host host
   * @param port port
   */
  public HostAddress(String host, int port) {
    this.host = host;
    this.port = port;
    this.master = null;
  }

  /**
   * Constructor.
   *
   * @param host host
   * @param port port
   * @param master is master
   */
  public HostAddress(String host, int port, Boolean master) {
    this.host = host;
    this.port = port;
    this.master = master;
  }

  /**
   * parse - parse server addresses from the URL fragment.
   *
   * @param spec list of endpoints in one of the forms 1 - host1,....,hostN:port (missing port
   *     default to MariaDB default 3306 2 - host:port,...,host:port
   * @param haMode High availability mode
   * @return parsed endpoints
   */
  public static List<HostAddress> parse(String spec, HaMode haMode) {
    if (spec == null) {
      throw new IllegalArgumentException("Invalid connection URL, host address must not be empty ");
    }
    if ("".equals(spec)) {
      return new ArrayList<>(0);
    }
    String[] tokens = spec.trim().split(",");
    int size = tokens.length;
    List<HostAddress> arr = new ArrayList<>(size);

    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      if (token.startsWith("address=")) {
        arr.add(parseParameterHostAddress(token, haMode, i == 0));
      } else {
        arr.add(parseSimpleHostAddress(token, haMode, i == 0));
      }
    }

    return arr;
  }

  private static HostAddress parseSimpleHostAddress(String str, HaMode haMode, boolean first) {
    String host = null;
    int port = 3306;
    Boolean master = null;

    if (str.charAt(0) == '[') {
      /* IPv6 addresses in URLs are enclosed in square brackets */
      int ind = str.indexOf(']');
      host = str.substring(1, ind);
      if (ind != (str.length() - 1) && str.charAt(ind + 1) == ':') {
        port = getPort(str.substring(ind + 2));
      }
    } else if (str.contains(":")) {
      /* Parse host:port */
      String[] hostPort = str.split(":");
      host = hostPort[0];
      port = getPort(hostPort[1]);
    } else {
      /* Just host name is given */
      host = str;
    }

    if (master == null) {
      switch (haMode) {
        case REPLICATION:
          master = first;
          break;

        default:
          master = true;
          break;
      }
    }

    return new HostAddress(host, port, master);
  }

  private static int getPort(String portString) {
    try {
      return Integer.parseInt(portString);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Incorrect port value : " + portString);
    }
  }

  private static HostAddress parseParameterHostAddress(String str, HaMode haMode, boolean first) {
    String host = null;
    int port = 3306;
    Boolean master = null;

    String[] array = str.split("(?=\\()|(?<=\\))");
    for (int i = 1; i < array.length; i++) {
      String[] token = array[i].replace("(", "").replace(")", "").trim().split("=");
      if (token.length != 2) {
        throw new IllegalArgumentException(
            "Invalid connection URL, expected key=value pairs, found " + array[i]);
      }
      String key = token[0].toLowerCase();
      String value = token[1].toLowerCase();

      switch (key) {
        case "host":
          host = value.replace("[", "").replace("]", "");
          break;
        case "port":
          port = getPort(value);
          break;
        case "type":
          master = "master".equals(key);
          break;
      }
    }

    if (master == null) {
      switch (haMode) {
        case REPLICATION:
          master = first;
          break;

        default:
          master = true;
          break;
      }
    }

    return new HostAddress(host, port, master);
  }

  /**
   * ToString implementation of addresses.
   *
   * @param addrs address list
   * @return String value
   */
  public static String toString(List<HostAddress> addrs) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < addrs.size(); i++) {
      if (addrs.get(i).master != null) {
        str.append("address=(host=")
            .append(addrs.get(i).host)
            .append(")(port=")
            .append(addrs.get(i).port)
            .append(")(type=")
            .append(addrs.get(i).master ? "master" : "slave")
            .append(")");
      } else {
        boolean isIPv6 = addrs.get(i).host != null && addrs.get(i).host.contains(":");
        String host = (isIPv6) ? ("[" + addrs.get(i).host + "]") : addrs.get(i).host;
        str.append(host).append(":").append(addrs.get(i).port);
      }
      if (i < addrs.size() - 1) {
        str.append(",");
      }
    }
    return str.toString();
  }

  /**
   * ToString implementation of addresses.
   *
   * @param addrs address array
   * @return String value
   */
  @SuppressWarnings("unused")
  public static String toString(HostAddress[] addrs) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < addrs.length; i++) {
      if (addrs[i].master != null) {
        str.append("address=(host=")
            .append(addrs[i].host)
            .append(")(port=")
            .append(addrs[i].port)
            .append(")(type=")
            .append(addrs[i].master ? "master" : "slave")
            .append(")");
      } else {
        boolean isIPv6 = addrs[i].host != null && addrs[i].host.contains(":");
        String host = (isIPv6) ? ("[" + addrs[i].host + "]") : addrs[i].host;
        str.append(host).append(":").append(addrs[i].port);
      }
      if (i < addrs.length - 1) {
        str.append(",");
      }
    }
    return str.toString();
  }

  @Override
  public String toString() {
    return String.format(
        "address=(host=%s)(port=%s)%s",
        host, port, ((master != null) ? ("(type=" + (master ? "master)" : "slave)")) : ""));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostAddress that = (HostAddress) o;
    return port == that.port
        && Objects.equals(host, that.host)
        && Objects.equals(master, that.master);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, master);
  }
}

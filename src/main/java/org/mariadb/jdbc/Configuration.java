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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.mariadb.jdbc.plugin.credential.CredentialPlugin;
import org.mariadb.jdbc.plugin.credential.CredentialPluginLoader;
import org.mariadb.jdbc.util.constants.HaMode;
import org.mariadb.jdbc.util.options.DefaultOptions;
import org.mariadb.jdbc.util.options.Options;

/**
 * parse and verification of URL.
 *
 * <p>basic syntax :<br>
 * {@code
 * jdbc:mariadb:[replication:|failover|loadbalance:|aurora:]//<hostDescription>[,<hostDescription>]/[database>]
 * [?<key1>=<value1>[&<key2>=<value2>]] }
 *
 * <p>hostDescription:<br>
 * - simple :<br>
 * {@code <host>:<portnumber>}<br>
 * (for example localhost:3306)<br>
 * <br>
 * - complex :<br>
 * {@code address=[(type=(master|slave))][(port=<portnumber>)](host=<host>)}<br>
 * <br>
 * <br>
 * type is by default master<br>
 * port is by default 3306<br>
 *
 * <p>host can be dns name, ipv4 or ipv6.<br>
 * in case of ipv6 and simple host description, the ip must be written inside bracket.<br>
 * exemple : {@code jdbc:mariadb://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 *
 * <p>Some examples :<br>
 * {@code jdbc:mariadb://localhost:3306/database?user=greg&password=pass}<br>
 * {@code
 * jdbc:mariadb://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass}
 * <br>
 */
public class Configuration implements Cloneable {

  private static final Pattern URL_PARAMETER =
      Pattern.compile("(\\/([^\\?]*))?(\\?(.+))*", Pattern.DOTALL);

  private String database;
  private Options options = null;
  private List<HostAddress> addresses;
  private HaMode haMode;
  private String initialUrl;
  private boolean multiMaster;
  private CredentialPlugin credentialPlugin;

  private Configuration() {}

  /**
   * Constructor.
   *
   * @param database database
   * @param addresses list of hosts
   * @param options connection option
   * @param haMode High availability mode
   * @throws SQLException if credential plugin cannot be loaded
   */
  public Configuration(String database, List<HostAddress> addresses, Options options, HaMode haMode)
      throws SQLException {
    this.options = options;
    this.database = database;
    this.addresses = addresses;
    this.haMode = haMode;
    this.credentialPlugin = CredentialPluginLoader.get(options.credentialType);
    DefaultOptions.postOptionProcess(options, credentialPlugin);
    setInitialUrl();
    loadMultiMasterValue();
  }

  /**
   * Tell if mariadb driver accept url string. (Correspond to interface
   * java.jdbc.Driver.acceptsURL() method)
   *
   * @param url url String
   * @return true if url string correspond.
   */
  public static boolean acceptsUrl(String url) {
    return url != null && url.startsWith("jdbc:mariadb:");
  }

  public static Configuration parse(final String url) throws SQLException {
    return parse(url, new Properties());
  }

  /**
   * Parse url connection string with additional properties.
   *
   * @param url connection string
   * @param prop properties
   * @return UrlParser instance
   * @throws SQLException if parsing exception occur
   */
  public static Configuration parse(final String url, Properties prop) throws SQLException {
    if (url != null && url.startsWith("jdbc:mariadb:")) {
      Configuration configuration = new Configuration();
      parseInternal(configuration, url, (prop == null) ? new Properties() : prop);
      return configuration;
    }
    return null;
  }

  /**
   * Parses the connection URL in order to set the UrlParser instance with all the information
   * provided through the URL.
   *
   * @param configuration object instance in which all data from the connection url is stored
   * @param url connection URL
   * @param properties properties
   * @throws SQLException if format is incorrect
   */
  private static void parseInternal(Configuration configuration, String url, Properties properties)
      throws SQLException {
    try {
      configuration.initialUrl = url;
      int separator = url.indexOf("//");
      if (separator == -1) {
        throw new IllegalArgumentException(
            "url parsing error : '//' is not present in the url " + url);
      }

      configuration.haMode = parseHaMode(url, separator);

      String urlSecondPart = url.substring(separator + 2);
      int dbIndex = urlSecondPart.indexOf("/");
      int paramIndex = urlSecondPart.indexOf("?");

      String hostAddressesString;
      String additionalParameters;
      if ((dbIndex < paramIndex && dbIndex < 0) || (dbIndex > paramIndex && paramIndex > -1)) {
        hostAddressesString = urlSecondPart.substring(0, paramIndex);
        additionalParameters = urlSecondPart.substring(paramIndex);
      } else if ((dbIndex < paramIndex && dbIndex > -1)
          || (dbIndex > paramIndex && paramIndex < 0)) {
        hostAddressesString = urlSecondPart.substring(0, dbIndex);
        additionalParameters = urlSecondPart.substring(dbIndex);
      } else {
        hostAddressesString = urlSecondPart;
        additionalParameters = null;
      }

      defineUrlParserParameters(
          configuration, properties, hostAddressesString, additionalParameters);
      configuration.loadMultiMasterValue();
    } catch (IllegalArgumentException i) {
      throw new SQLException("error parsing url : " + i.getMessage(), i);
    }
  }

  /*
  Parse ConnectorJ compatible urls
  jdbc:[mariadb|mysql]://host:port/database
  Example: jdbc:mariadb://localhost:3306/test?user=root&password=passwd
  */

  /**
   * Sets the parameters of the UrlParser instance: addresses, database and options. It parses
   * through the additional parameters given in order to extract the database and the options for
   * the connection.
   *
   * @param configuration object instance in which all data from the connection URL is stored
   * @param properties properties
   * @param hostAddressesString string that holds all the host addresses
   * @param additionalParameters string that holds all parameters defined for the connection
   * @throws SQLException if credential plugin cannot be loaded
   */
  private static void defineUrlParserParameters(
      Configuration configuration,
      Properties properties,
      String hostAddressesString,
      String additionalParameters)
      throws SQLException {

    if (additionalParameters != null) {
      //noinspection Annotator
      Matcher matcher = URL_PARAMETER.matcher(additionalParameters);
      matcher.find();
      configuration.database = matcher.group(2);
      configuration.options =
          DefaultOptions.parse(
              configuration.haMode, matcher.group(4), properties, configuration.options);
      if (configuration.database != null && configuration.database.isEmpty()) {
        configuration.database = null;
      }
    } else {
      configuration.database = null;
      configuration.options =
          DefaultOptions.parse(configuration.haMode, "", properties, configuration.options);
    }
    configuration.credentialPlugin =
        CredentialPluginLoader.get(configuration.options.credentialType);
    DefaultOptions.postOptionProcess(configuration.options, configuration.credentialPlugin);
    configuration.addresses = HostAddress.parse(hostAddressesString, configuration.haMode);
  }

  private static HaMode parseHaMode(String url, int separator) {
    // parser is sure to have at least 2 colon, since jdbc:[mysql|mariadb]: is tested.
    int firstColonPos = url.indexOf(':');
    int secondColonPos = url.indexOf(':', firstColonPos + 1);
    int thirdColonPos = url.indexOf(':', secondColonPos + 1);

    if (thirdColonPos > separator || thirdColonPos == -1) {
      if (secondColonPos == separator - 1) {
        return HaMode.NONE;
      }
      thirdColonPos = separator;
    }

    try {
      String haModeString =
          url.substring(secondColonPos + 1, thirdColonPos).toUpperCase(Locale.ROOT);
      if ("FAILOVER".equals(haModeString)) {
        haModeString = "LOADBALANCE";
      }
      return HaMode.valueOf(haModeString);
    } catch (IllegalArgumentException i) {
      throw new IllegalArgumentException(
          "wrong failover parameter format in connection String " + url);
    }
  }

  private void setInitialUrl() {
    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:mariadb:");
    if (haMode != HaMode.NONE) {
      sb.append(haMode.toString().toLowerCase(Locale.ROOT)).append(":");
    }
    sb.append("//");

    for (int i = 0; i < addresses.size(); i++) {
      HostAddress hostAddress = addresses.get(i);
      if (i > 0) {
        sb.append(",");
      }
      sb.append(hostAddress);
    }

    sb.append("/");
    if (database != null) {
      sb.append(database);
    }
    DefaultOptions.propertyString(options, haMode, sb);
    initialUrl = sb.toString();
  }

  /**
   * Parse url connection string.
   *
   * @param url connection string
   * @throws SQLException if url format is incorrect
   */
  public void parseUrl(String url) throws SQLException {
    if (acceptsUrl(url)) {
      parseInternal(this, url, new Properties());
    }
  }

  public String getUsername() {
    return options.user;
  }

  public void setUsername(String username) {
    options.user = username;
  }

  public String getPassword() {
    return options.password;
  }

  public void setPassword(String password) {
    options.password = password;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public List<HostAddress> getHostAddresses() {
    return this.addresses;
  }

  public Options getOptions() {
    return options;
  }

  protected void setProperties(String urlParameters) {
    DefaultOptions.parse(this.haMode, urlParameters, this.options);
    setInitialUrl();
  }

  public CredentialPlugin getCredentialPlugin() {
    return credentialPlugin;
  }

  /**
   * ToString implementation.
   *
   * @return String value
   */
  public String toString() {
    return initialUrl;
  }

  public String getInitialUrl() {
    return initialUrl;
  }

  public HaMode getHaMode() {
    return haMode;
  }

  @Override
  public boolean equals(Object parser) {
    if (this == parser) {
      return true;
    }
    if (!(parser instanceof Configuration)) {
      return false;
    }

    Configuration configuration = (Configuration) parser;
    return (initialUrl != null
            ? initialUrl.equals(configuration.getInitialUrl())
            : configuration.getInitialUrl() == null)
        && (getUsername() != null
            ? getUsername().equals(configuration.getUsername())
            : configuration.getUsername() == null)
        && (getPassword() != null
            ? getPassword().equals(configuration.getPassword())
            : configuration.getPassword() == null);
  }

  @Override
  public int hashCode() {
    int result = options.password != null ? options.password.hashCode() : 0;
    result = 31 * result + (options.user != null ? options.user.hashCode() : 0);
    result = 31 * result + initialUrl.hashCode();
    return result;
  }

  private void loadMultiMasterValue() {
    boolean firstMaster = true;
    for (HostAddress host : addresses) {
      if (host.master == Boolean.TRUE) {
        if (firstMaster) {
          multiMaster = true;
          return;
        } else {
          firstMaster = true;
        }
      }
    }
    multiMaster = false;
  }

  public boolean isMultiMaster() {
    return multiMaster;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Configuration tmpConfiguration = (Configuration) super.clone();
    tmpConfiguration.options = (Options) options.clone();
    tmpConfiguration.addresses = new ArrayList<>();
    tmpConfiguration.addresses.addAll(addresses);
    return tmpConfiguration;
  }
}

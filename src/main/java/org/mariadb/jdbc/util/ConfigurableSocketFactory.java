package org.mariadb.jdbc.util;

import javax.net.SocketFactory;
import org.mariadb.jdbc.util.options.Options;

public abstract class ConfigurableSocketFactory extends SocketFactory {
  public abstract void setConfiguration(Options options, String host);
}

package org.mariadb.jdbc.util.log;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.regex.Matcher;

public abstract class Loggers {

  public static final String FALLBACK_PROPERTY = "mariadb.logging.fallback";

  private static LoggerFactory LOGGER_FACTORY;

  static {
    resetLoggerFactory();
  }

  Loggers() {}

  public static final void resetLoggerFactory() {
    try {
      useSl4jLoggers();
    } catch (Throwable t) {
      if (isFallbackToJdk()) {
        useJdkLoggers();
      } else {
        useConsoleLoggers();
      }
    }
  }

  static final boolean isFallbackToJdk() {
    return "JDK".equalsIgnoreCase(System.getProperty(FALLBACK_PROPERTY));
  }

  public static final void useConsoleLoggers() {
    String name = LoggerFactory.class.getName();
    LoggerFactory loggerFactory = new ConsoleLoggerFactory(false);
    loggerFactory.getLogger(name).debug("Using Console logging");
    LOGGER_FACTORY = loggerFactory;
  }

  public static final void useVerboseConsoleLoggers() {
    String name = LoggerFactory.class.getName();
    LoggerFactory loggerFactory = new ConsoleLoggerFactory(true);
    loggerFactory.getLogger(name).debug("Using Verbose Console logging");
    LOGGER_FACTORY = loggerFactory;
  }

  public static final void useCustomLoggers(
      final Function<String, ? extends Logger> loggerFactory) {
    String name = LoggerFactory.class.getName();
    loggerFactory.apply(name).debug("Using custom logging");
    LOGGER_FACTORY = loggerFactory::apply;
  }

  public static final void useJdkLoggers() {
    String name = LoggerFactory.class.getName();
    LoggerFactory loggerFactory = new JdkLoggerFactory();
    loggerFactory.getLogger(name).debug("Using JDK logging framework");
    LOGGER_FACTORY = loggerFactory;
  }

  public static final void useSl4jLoggers() {
    String name = LoggerFactory.class.getName();
    LoggerFactory loggerFactory = new Slf4JLoggerFactory();
    loggerFactory.getLogger(name).debug("Using Slf4j logging framework");
    LOGGER_FACTORY = loggerFactory;
  }

  public static Logger getLogger(String name) {
    return LOGGER_FACTORY.getLogger(name);
  }

  public static Logger getLogger(Class<?> cls) {
    return LOGGER_FACTORY.getLogger(cls.getName());
  }

  private interface LoggerFactory {
    Logger getLogger(String name);
  }

  private static class Slf4JLoggerFactory implements LoggerFactory {

    @Override
    public Logger getLogger(String name) {
      return new Slf4JLogger(org.slf4j.LoggerFactory.getLogger(name));
    }
  }

  private static class Slf4JLogger implements Logger {

    private final org.slf4j.Logger logger;

    public Slf4JLogger(org.slf4j.Logger logger) {
      this.logger = logger;
    }

    @Override
    public String getName() {
      return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
      return logger.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
      logger.trace(msg);
    }

    @Override
    public void trace(String format, Object... arguments) {
      logger.trace(format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
      logger.trace(msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
      return logger.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
      logger.debug(msg);
    }

    @Override
    public void debug(String format, Object... arguments) {
      logger.debug(format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
      logger.debug(msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
      return logger.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
      logger.info(msg);
    }

    @Override
    public void info(String format, Object... arguments) {
      logger.info(format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
      logger.info(msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
      return logger.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
      logger.warn(msg);
    }

    @Override
    public void warn(String format, Object... arguments) {
      logger.warn(format, arguments);
    }

    @Override
    public void warn(String msg, Throwable t) {
      logger.warn(msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
      return logger.isErrorEnabled();
    }

    @Override
    public void error(String msg) {
      logger.error(msg);
    }

    @Override
    public void error(String format, Object... arguments) {
      logger.error(format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
      logger.error(msg, t);
    }
  }

  /** Wrapper over JDK logger */
  static final class JdkLogger implements Logger {

    private final java.util.logging.Logger logger;

    public JdkLogger(java.util.logging.Logger logger) {
      this.logger = logger;
    }

    @Override
    public String getName() {
      return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
      return logger.isLoggable(Level.FINEST);
    }

    @Override
    public void trace(String msg) {
      logger.log(Level.FINEST, msg);
    }

    @Override
    public void trace(String format, Object... arguments) {
      logger.log(Level.FINEST, format(format, arguments));
    }

    @Override
    public void trace(String msg, Throwable t) {
      logger.log(Level.FINEST, msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
      return logger.isLoggable(Level.FINE);
    }

    @Override
    public void debug(String msg) {
      logger.log(Level.FINE, msg);
    }

    @Override
    public void debug(String format, Object... arguments) {
      logger.log(Level.FINE, format(format, arguments));
    }

    @Override
    public void debug(String msg, Throwable t) {
      logger.log(Level.FINE, msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
      return logger.isLoggable(Level.INFO);
    }

    @Override
    public void info(String msg) {
      logger.log(Level.INFO, msg);
    }

    @Override
    public void info(String format, Object... arguments) {
      logger.log(Level.INFO, format(format, arguments));
    }

    @Override
    public void info(String msg, Throwable t) {
      logger.log(Level.INFO, msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
      return logger.isLoggable(Level.WARNING);
    }

    @Override
    public void warn(String msg) {
      logger.log(Level.WARNING, msg);
    }

    @Override
    public void warn(String format, Object... arguments) {
      logger.log(Level.WARNING, format(format, arguments));
    }

    @Override
    public void warn(String msg, Throwable t) {
      logger.log(Level.WARNING, msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
      return logger.isLoggable(Level.SEVERE);
    }

    @Override
    public void error(String msg) {
      logger.log(Level.SEVERE, msg);
    }

    @Override
    public void error(String format, Object... arguments) {
      logger.log(Level.SEVERE, format(format, arguments));
    }

    @Override
    public void error(String msg, Throwable t) {
      logger.log(Level.SEVERE, msg, t);
    }

    final String format(String from, Object... arguments) {
      if (from != null) {
        String computed = from;
        if (arguments != null && arguments.length != 0) {
          for (Object argument : arguments) {
            computed =
                computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(String.valueOf(argument)));
          }
        }
        return computed;
      }
      return null;
    }
  }

  private static class JdkLoggerFactory implements LoggerFactory {

    @Override
    public Logger getLogger(String name) {
      return new JdkLogger(java.util.logging.Logger.getLogger(name));
    }
  }

  /**
   * A {@link Logger} that has all levels enabled. error and warn log to System.err while all other
   * levels log to System.out (printstreams can be changed via constructor).
   */
  static final class ConsoleLogger implements Logger {

    private final String name;
    private final PrintStream err;
    private final PrintStream log;
    private final boolean verbose;

    ConsoleLogger(String name, PrintStream log, PrintStream err, boolean verbose) {
      this.name = name;
      this.log = log;
      this.err = err;
      this.verbose = verbose;
    }

    ConsoleLogger(String name, boolean verbose) {
      this(name, System.out, System.err, verbose);
    }

    @Override
    public String getName() {
      return this.name;
    }

    final String format(String from, Object... arguments) {
      if (from != null) {
        String computed = from;
        if (arguments != null && arguments.length != 0) {
          for (Object argument : arguments) {
            computed =
                computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(String.valueOf(argument)));
          }
        }
        return computed;
      }
      return null;
    }

    @Override
    public boolean isTraceEnabled() {
      return verbose;
    }

    @Override
    public synchronized void trace(String msg) {
      if (!verbose) {
        return;
      }
      this.log.format("[TRACE] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void trace(String format, Object... arguments) {
      if (!verbose) {
        return;
      }
      this.log.format(
          "[TRACE] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
    }

    @Override
    public synchronized void trace(String msg, Throwable t) {
      if (!verbose) {
        return;
      }
      this.log.format("[TRACE] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.log);
    }

    @Override
    public boolean isDebugEnabled() {
      return verbose;
    }

    @Override
    public synchronized void debug(String msg) {
      if (!verbose) {
        return;
      }
      this.log.format("[DEBUG] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void debug(String format, Object... arguments) {
      if (!verbose) {
        return;
      }
      this.log.format(
          "[DEBUG] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
    }

    @Override
    public synchronized void debug(String msg, Throwable t) {
      if (!verbose) {
        return;
      }
      this.log.format("[DEBUG] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.log);
    }

    @Override
    public boolean isInfoEnabled() {
      return true;
    }

    @Override
    public synchronized void info(String msg) {
      this.log.format("[ INFO] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void info(String format, Object... arguments) {
      this.log.format(
          "[ INFO] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
    }

    @Override
    public synchronized void info(String msg, Throwable t) {
      this.log.format("[ INFO] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.log);
    }

    @Override
    public boolean isWarnEnabled() {
      return true;
    }

    @Override
    public synchronized void warn(String msg) {
      this.err.format("[ WARN] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void warn(String format, Object... arguments) {
      this.err.format(
          "[ WARN] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
    }

    @Override
    public synchronized void warn(String msg, Throwable t) {
      this.err.format("[ WARN] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.err);
    }

    @Override
    public boolean isErrorEnabled() {
      return true;
    }

    @Override
    public synchronized void error(String msg) {
      this.err.format("[ERROR] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void error(String format, Object... arguments) {
      this.err.format(
          "[ERROR] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
    }

    @Override
    public synchronized void error(String msg, Throwable t) {
      this.err.format("[ERROR] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.err);
    }
  }

  private static final class ConsoleLoggerFactory implements LoggerFactory {

    private static final HashMap<String, Logger> consoleLoggers = new HashMap<>();

    final boolean verbose;

    private ConsoleLoggerFactory(boolean verbose) {
      this.verbose = verbose;
    }

    @Override
    public Logger getLogger(String name) {
      return consoleLoggers.computeIfAbsent(name, n -> new ConsoleLogger(n, verbose));
    }
  }
}

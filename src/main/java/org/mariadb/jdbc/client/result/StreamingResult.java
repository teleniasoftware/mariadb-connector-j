package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketReader;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class StreamingResult extends Result {

  private final ReentrantLock lock;
  private int dataFetchTime = 0;
  private int fetchSize;

  public StreamingResult(
          Statement stmt,
      boolean text,
      ColumnDefinitionPacket[] metadataList,
      PacketReader reader,
      ConnectionContext context,
      int maxRows,
      int fetchSize,
      ReentrantLock lock,
      int resultSetScrollType, boolean closeOnCompletion)
      throws IOException, SQLException {

    super(stmt, text, metadataList, reader, context, maxRows, resultSetScrollType, closeOnCompletion);
    this.lock = lock;
    this.dataFetchTime = 0;
    this.fetchSize = fetchSize;
    this.data = new ArrayList<>(fetchSize);

    addStreamingValue();
  }

  @Override
  public boolean streaming() {
    return true;
  }

  private void addStreamingValue() throws IOException, SQLException {
    lock.lock();
    try {
      // read only fetchSize values
      int fetchSizeTmp = fetchSize;
      while (fetchSizeTmp > 0 && readNext()) {
        fetchSizeTmp--;
      }
      dataFetchTime++;
    } finally {
      lock.unlock();
    }
  }

  /**
   * When protocol has a current Streaming result (this) fetch all to permit another query is
   * executing.
   *
   * @throws SQLException if any error occur
   */
  public void fetchRemaining() throws SQLException {
    if (!loaded) {
      lock.lock();
      try {

        while (!loaded) {
          addStreamingValue();
        }

      } catch (SQLException queryException) {
        throw exceptionFactory.create(queryException);
      } catch (IOException ioe) {
        throw exceptionFactory.create("Error while streaming resultset data", "08000", ioe);
      } finally {
        lock.unlock();
      }
      dataFetchTime++;
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (closed) {
      throw exceptionFactory.create("Operation not permit on a closed resultSet", "HY000");
    }
    if (rowPointer < data.size() - 1) {
      rowPointer++;
      row.setRow(data.get(rowPointer));
      return true;
    } else {
      if (!loaded) {
        lock.lock();
        try {
          if (!loaded) {
            addStreamingValue();
          }
        } catch (IOException ioe) {
          throw exceptionFactory.create("Error while streaming resultset data", "08000", ioe);
        } finally {
          lock.unlock();
        }

        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
          // resultSet has been cleared. next value is pointer 0.
          rowPointer = 0;
          if (data.size() > 0) {
            row.setRow(data.get(rowPointer));
            return true;
          }
          row.setRow(null);
          return false;
        } else {
          // cursor can move backward, so driver must keep the results.
          // results have been added to current resultSet
          rowPointer++;
          if (data.size() > rowPointer) {
            row.setRow(data.get(rowPointer));
            return true;
          }
          row.setRow(null);
          return false;
        }
      }

      // all data are reads and pointer is after last
      rowPointer = data.size();
      row.setRow(null);
      return false;
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClose();
    if (rowPointer < data.size()) {
      // has remaining results
      return false;
    } else {
      if (!loaded) {

        // has to read more result to know if it's finished or not
        // (next packet may be new data or an EOF packet indicating that there is no more data)
        lock.lock();
        try {
          // this time, fetch is added even for streaming forward type only to keep current pointer
          // row.
          if (!loaded) {
            addStreamingValue();
          }
        } catch (IOException ioe) {
          throw exceptionFactory.create("Error while streaming resultset data", "08000", ioe);
        } finally {
          lock.unlock();
        }

        return data.size() == rowPointer;
      }

      // has read all data and pointer is after last result
      // so result would have to always to be true,
      // but when result contain no row at all jdbc say that must return false
      return data.size() > 0 || dataFetchTime > 1;
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClose();
    return dataFetchTime == 1 && rowPointer == 0 && data.size() > 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClose();
    if (rowPointer < data.size() - 1) {
      return false;
    } else if (loaded) {
      return rowPointer == data.size() - 1 && data.size() > 0;
    } else {
      // when streaming and not having read all results,
      // must read next packet to know if next packet is an EOF packet or some additional data
      lock.lock();
      try {
        if (!loaded) {
          addStreamingValue();
        }
      } catch (IOException ioe) {
        throw exceptionFactory.create("Error while streaming result-set data", "08000", ioe);
      } finally {
        lock.unlock();
      }

      if (loaded) {
        // now driver is sure when data ends.
        return rowPointer == data.size() - 1 && data.size() > 0;
      }

      // There is data remaining
      return false;
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClose();
    if (resultSetScrollType == TYPE_FORWARD_ONLY) {
      throw exceptionFactory.create("Invalid operation for result set type TYPE_FORWARD_ONLY");
    }
    row.setRow(null);
    rowPointer = -1;
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    fetchRemaining();
    row.setRow(null);
    rowPointer = data.size();
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();

    if (resultSetScrollType == TYPE_FORWARD_ONLY) {
      throw exceptionFactory.create("Invalid operation for result set type TYPE_FORWARD_ONLY");
    }

    rowPointer = 0;
    if (data.size() > 0) {
      row.setRow(data.get(rowPointer));
      return true;
    }
    row.setRow(null);
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    fetchRemaining();
    rowPointer = data.size() - 1;
    if (data.size() > 0) {
      row.setRow(data.get(rowPointer));
      return true;
    }
    row.setRow(null);
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    checkClose();
    if (resultSetScrollType == TYPE_FORWARD_ONLY) {
      return 0;
    }
    return rowPointer + 1;
  }

  @Override
  public boolean absolute(int idx) throws SQLException {
    checkClose();

    if (resultSetScrollType == TYPE_FORWARD_ONLY) {
      throw exceptionFactory.create("Invalid operation for result set type TYPE_FORWARD_ONLY");
    }

    if (idx >= 0 && idx <= data.size()) {
      rowPointer = idx - 1;
      row.setRow(data.get(rowPointer));
      return true;
    }

    // if streaming, must read additional results.
    fetchRemaining();

    if (idx >= 0) {

      if (idx <= data.size()) {
        rowPointer = idx - 1;
        row.setRow(data.get(rowPointer));
        return true;
      }

      rowPointer = data.size(); // go to afterLast() position
      row.setRow(null);
      return false;

    } else {

      if (data.size() + idx >= 0) {
        // absolute position reverse from ending resultSet
        rowPointer = data.size() + idx;
        row.setRow(data.get(rowPointer));
        return true;
      }
      row.setRow(null);
      rowPointer = -1; // go to before first position
      return false;
    }
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClose();
    if (resultSetScrollType == TYPE_FORWARD_ONLY) {
      throw exceptionFactory.create("Invalid operation for result set type TYPE_FORWARD_ONLY");
    }
    int newPos = rowPointer + rows;
    if (newPos <= -1) {
      rowPointer = -1;
      row.setRow(null);
      return false;
    } else if (newPos >= data.size()) {
      rowPointer = data.size();
      row.setRow(null);
      return false;
    } else {
      rowPointer = newPos;
      row.setRow(data.get(rowPointer));
      return true;
    }
  }

  @Override
  public boolean previous() throws SQLException {
    checkClose();
    if (resultSetScrollType == TYPE_FORWARD_ONLY) {
      throw exceptionFactory.create("Invalid operation for result set type TYPE_FORWARD_ONLY");
    }
    if (rowPointer > -1) {
      rowPointer--;
      if (rowPointer != -1) {
        row.setRow(data.get(rowPointer));
        return true;
      }
    }
    row.setRow(null);
    return false;
  }

  @Override
  public int getFetchSize() {
    return this.fetchSize;
  }

  @Override
  public void setFetchSize(int fetchSize) throws SQLException {
    if (fetchSize == 0) {
      lock.lock();
      try {
        // fetch all results
        while (!loaded) {
          addStreamingValue();
        }
      } catch (IOException ioe) {
        throw exceptionFactory.create("Error while streaming result-set data", "08000", ioe);
      } finally {
        lock.unlock();
      }
    }
    this.fetchSize = fetchSize;
  }
}

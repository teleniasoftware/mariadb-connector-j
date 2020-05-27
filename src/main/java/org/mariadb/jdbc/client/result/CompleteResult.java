package org.mariadb.jdbc.client.result;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketReader;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public class CompleteResult extends Result {

  public CompleteResult(
          Statement stmt,
          boolean text,
      ColumnDefinitionPacket[] metadataList,
      PacketReader reader,
      ConnectionContext context,
      int maxRows,
      int resultSetScrollType, boolean closeOnCompletion)
      throws IOException, SQLException {

    super(stmt, text, metadataList, reader, context, maxRows, resultSetScrollType, closeOnCompletion);
    this.data = new ArrayList<>(10);
    while (readNext()) {}
    loaded = true;
  }

  public CompleteResult(
      ColumnDefinitionPacket[] metadataList,
      List<ReadableByteBuf> data,
      ExceptionFactory exceptionFactory) {
    super(metadataList, data, exceptionFactory);
  }

  public static ResultSet createResultSet(
      String columnName, DataType columnType, String[] data, ExceptionFactory exceptionFactory) {
    return createResultSet(
        new String[] {columnName},
        new DataType[] {columnType},
        new String[][] {data},
        exceptionFactory);
  }
  /**
   * Create a result set from given data. Useful for creating "fake" resultSets for
   * DatabaseMetaData, (one example is MariaDbDatabaseMetaData.getTypeInfo())
   *
   * @param columnNames - string array of column names
   * @param columnTypes - column types
   * @param data - each element of this array represents a complete row in the ResultSet. Each value
   *     is given in its string representation, as in MariaDB text protocol, except boolean (BIT(1))
   *     values that are represented as "1" or "0" strings
   * @return resultset
   */
  public static ResultSet createResultSet(
      String[] columnNames,
      DataType[] columnTypes,
      String[][] data,
      ExceptionFactory exceptionFactory) {

    int columnNameLength = columnNames.length;
    ColumnDefinitionPacket[] columns = new ColumnDefinitionPacket[columnNameLength];

    for (int i = 0; i < columnNameLength; i++) {
      columns[i] = ColumnDefinitionPacket.create(columnNames[i], columnTypes[i]);
    }

    List<ReadableByteBuf> rows = new ArrayList<>();
    try {
      for (String[] rowData : data) {
        assert rowData.length == columnNameLength;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (int i = 0; i < rowData.length; i++) {

          if (rowData[i] != null) {
            byte[] bb = rowData[i].getBytes();
            int len = bb.length;
            if (len < 251) {
              baos.write((byte) len);
            } else if (len < 65536) {
              baos.write((byte) 0xfc);
              baos.write((byte) len);
              baos.write((byte) (len >>> 8));
            } else if (len < 16777216) {
              baos.write((byte) 0xfd);
              baos.write((byte) len);
              baos.write((byte) (len >>> 8));
              baos.write((byte) (len >>> 16));
            } else {

              baos.write((byte) 0xfe);
              baos.write((byte) len);
              baos.write((byte) (len >>> 8));
              baos.write((byte) (len >>> 16));
              baos.write((byte) (len >>> 24));
              baos.write((byte) (len >>> 32));
              baos.write((byte) (len >>> 40));
              baos.write((byte) (len >>> 48));
              baos.write((byte) (len >>> 56));
            }
            baos.write(bb);
          } else {
            baos.write((byte) 0xfb);
          }
        }
        byte[] bb = baos.toByteArray();
        rows.add(new ReadableByteBuf(null, bb, bb.length));
      }
    } catch (IOException ioe) {
      // cannot occur
      ioe.printStackTrace();
    }
    return new CompleteResult(columns, rows, exceptionFactory);
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
      // all data are reads and pointer is after last
      row.setRow(null);
      rowPointer = data.size();
      return false;
    }
  }

  @Override
  public boolean streaming() {
    return false;
  }

  @Override
  public void fetchRemaining() throws SQLException {}

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClose();
    if (rowPointer < data.size()) {
      // has remaining results
      return false;
    } else {

      // has read all data and pointer is after last result
      // so result would have to always to be true,
      // but when result contain no row at all jdbc say that must return false
      return data.size() > 0;
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClose();
    return rowPointer == 0 && data.size() > 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClose();
    return rowPointer == data.size() - 1 && data.size() > 0;
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClose();
    rowPointer = -1;
    row.setRow(null);
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    row.setRow(null);
    rowPointer = data.size();
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();
    rowPointer = 0;
    row.setRow(data.get(rowPointer));
    return data.size() > 0;
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    rowPointer = data.size() - 1;
    row.setRow(data.get(rowPointer));
    return data.size() > 0;
  }

  @Override
  public int getRow() throws SQLException {
    checkClose();
    return rowPointer + 1;
  }

  @Override
  public boolean absolute(int idx) throws SQLException {
    checkClose();

    if (idx >= 0 && idx <= data.size()) {
      rowPointer = idx - 1;
      row.setRow(data.get(rowPointer));
      return true;
    }

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

      rowPointer = -1; // go to before first position
      row.setRow(null);
      return false;
    }
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClose();
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
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {}
}

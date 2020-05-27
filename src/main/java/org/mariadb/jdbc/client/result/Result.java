package org.mariadb.jdbc.client.result;

import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.ConnectionContext;
import org.mariadb.jdbc.client.PacketReader;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.codec.RowDecoder;
import org.mariadb.jdbc.codec.TextRowDecoder;
import org.mariadb.jdbc.codec.list.*;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Result implements Completion, ResultSet {

  private final boolean text;
  private final ColumnDefinitionPacket[] metadataList;
  private final ConnectionContext context;
  private final int maxRows;
  private Statement statement;
  private int currRows = 0;
  protected RowDecoder row;

  protected final int resultSetScrollType;
  protected final ExceptionFactory exceptionFactory;
  protected final PacketReader reader;
  protected List<ReadableByteBuf> data;
  protected boolean loaded;
  protected int rowPointer = -1;
  protected boolean closed;

  public Result(
      boolean text,
      ColumnDefinitionPacket[] metadataList,
      PacketReader reader,
      ConnectionContext context,
      int maxRows,
      int resultSetScrollType) {
    this.text = text;
    this.metadataList = metadataList;
    this.reader = reader;
    this.exceptionFactory = context.getExceptionFactory();
    this.context = context;
    this.maxRows = maxRows;
    this.resultSetScrollType = resultSetScrollType;
    row = new TextRowDecoder(metadataList.length, metadataList);
  }

  public Result(
      ColumnDefinitionPacket[] metadataList,
      List<ReadableByteBuf> data,
      ExceptionFactory exceptionFactory) {
    this.text = true;
    this.metadataList = metadataList;
    this.reader = null;
    this.exceptionFactory = exceptionFactory;
    this.context = null;
    this.data = data;
    this.maxRows = 0;
    this.resultSetScrollType = TYPE_FORWARD_ONLY;
    row = new TextRowDecoder(metadataList.length, metadataList);
  }

  protected boolean readNext() throws SQLException, IOException {
    ReadableByteBuf buf = reader.readPacket(false);
    switch (buf.getUnsignedByte()) {
      case 0x00:
        loaded = true;
        ErrorPacket errorPacket = ErrorPacket.decode(buf, context);
        throw exceptionFactory.create(
            errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

      case 0xFE:
        if ((context.isEofDeprecated() && buf.readableBytes() < 0xffffff)
            || (!context.isEofDeprecated() && buf.readableBytes() < 8)) {

          buf.skip(); // skip header
          int serverStatus;
          int warnings;

          if (!context.isEofDeprecated()) {
            // EOF_Packet
            warnings = buf.readUnsignedShort();
            serverStatus = buf.readUnsignedShort();
          } else {
            // OK_Packet with a 0xFE header
            buf.skip(buf.readLength()); // skip update count
            buf.skip(buf.readLength()); // skip insert id
            serverStatus = buf.readUnsignedShort();
            warnings = buf.readUnsignedShort();
          }
          context.setServerStatus(serverStatus);
          context.setWarning(warnings);
          loaded = true;
          return false;
        }

        // continue reading rows

      default:
        if (maxRows == 0 || (maxRows > 0 &&  currRows++ < maxRows)) {
          data.add(buf);
        } else {
          skipRemaining();
          return false;
        }
    }
    return true;
  }

  private void skipRemaining() throws SQLException, IOException {
    loaded = true;
    while (true) {
      ReadableByteBuf buf = reader.readPacket(false);
      switch (buf.getUnsignedByte()) {
        case 0x00:
          ErrorPacket errorPacket = ErrorPacket.decode(buf, context);
          throw exceptionFactory.create(
              errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

        case 0xFE:
          if ((context.isEofDeprecated() && buf.readableBytes() < 0xffffff)
              || (!context.isEofDeprecated() && buf.readableBytes() < 8)) {

            buf.skip(); // skip header
            int serverStatus;
            int warnings;

            if (!context.isEofDeprecated()) {
              // EOF_Packet
              warnings = buf.readUnsignedShort();
              serverStatus = buf.readUnsignedShort();
            } else {
              // OK_Packet with a 0xFE header
              buf.skip(buf.readLength()); // skip update count
              buf.skip(buf.readLength()); // skip insert id
              serverStatus = buf.readUnsignedShort();
              warnings = buf.readUnsignedShort();
            }
            context.setServerStatus(serverStatus);
            context.setWarning(warnings);
            return;
          }
          break;
      }
    }
  }

  public abstract boolean next() throws SQLException;

  public abstract boolean streaming();

  public abstract void fetchRemaining() throws SQLException;

  public boolean loaded() {
    return loaded;
  }

  @Override
  public void close() throws SQLException {
    this.closed = true;
  }
  @Override
  public boolean wasNull() throws SQLException {
    return row.wasNull();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return row.get(columnIndex, StringCodec.INSTANCE);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Boolean b = row.get(columnIndex, BooleanCodec.INSTANCE);
    return (b == null) ? false : b;
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    Byte b = row.get(columnIndex, ByteCodec.INSTANCE);
    return (b == null) ? 0 : b;
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    Short b = row.get(columnIndex, ShortCodec.INSTANCE);
    return (b == null) ? 0 : b;
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    Integer b = row.get(columnIndex, IntCodec.INSTANCE);
    return (b == null) ? 0 : b;
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    Long b = row.get(columnIndex, LongCodec.INSTANCE);
    return (b == null) ? 0L : b;
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    Float b = row.get(columnIndex, FloatCodec.INSTANCE);
    return (b == null) ? 0F : b;
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    Double b = row.get(columnIndex, DoubleCodec.INSTANCE);
    return (b == null) ? 0D : b;
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return row.get(columnIndex, BigDecimalCodec.INSTANCE);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return row.get(columnIndex, ByteArrayCodec.INSTANCE);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    LocalDate d = row.get(columnIndex, LocalDateCodec.INSTANCE);
    return (d == null) ? null : Date.valueOf(d);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    LocalTime d = row.get(columnIndex, LocalTimeCodec.INSTANCE);
    return (d == null) ? null : Time.valueOf(d);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    LocalDateTime d = row.get(columnIndex, LocalDateTimeCodec.INSTANCE);
    return (d == null) ? null : Timestamp.valueOf(d);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return row.get(columnIndex, StreamCodec.INSTANCE);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return row.get(columnIndex, StreamCodec.INSTANCE);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return row.get(columnIndex, StreamCodec.INSTANCE);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return row.get(columnLabel, StringCodec.INSTANCE);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    Boolean b = row.get(columnLabel, BooleanCodec.INSTANCE);
    return (b == null) ? false : b;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    Byte b = row.get(columnLabel, ByteCodec.INSTANCE);
    return (b == null) ? 0 : b;
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    Short b = row.get(columnLabel, ShortCodec.INSTANCE);
    return (b == null) ? 0 : b;
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    Integer b = row.get(columnLabel, IntCodec.INSTANCE);
    return (b == null) ? 0 : b;
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    Long b = row.get(columnLabel, LongCodec.INSTANCE);
    return (b == null) ? 0L : b;
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    Float b = row.get(columnLabel, FloatCodec.INSTANCE);
    return (b == null) ? 0F : b;
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    Double b = row.get(columnLabel, DoubleCodec.INSTANCE);
    return (b == null) ? 0D : b;
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return row.get(columnLabel, BigDecimalCodec.INSTANCE);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return row.get(columnLabel, ByteArrayCodec.INSTANCE);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    LocalDate d = row.get(columnLabel, LocalDateCodec.INSTANCE);
    return (d == null) ? null : Date.valueOf(d);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    LocalTime d = row.get(columnLabel, LocalTimeCodec.INSTANCE);
    return (d == null) ? null : Time.valueOf(d);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    LocalDateTime d = row.get(columnLabel, LocalDateTimeCodec.INSTANCE);
    return (d == null) ? null : Timestamp.valueOf(d);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return row.get(columnLabel, StreamCodec.INSTANCE);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return row.get(columnLabel, StreamCodec.INSTANCE);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return row.get(columnLabel, StreamCodec.INSTANCE);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO implement getting warnings.
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO implement getting warnings.
  }

  @Override
  public String getCursorName() throws SQLException {
    throw exceptionFactory.notSupported("Cursors not supported");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    //TODO implement medata
    return null;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (columnIndex < 1 || columnIndex > metadataList.length) {
      throw new SQLException(
              String.format(
                      "Wrong index position. Is %s but must be in 1-%s range", columnIndex, metadataList.length));
    }
    Codec defaultCodec = metadataList[columnIndex].getDefaultCodec();
    return row.get(columnIndex, defaultCodec);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(row.getIndex(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return row.getIndex(columnLabel);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return new StringReader(row.get(columnIndex, StringCodec.INSTANCE));
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return new StringReader(row.get(columnLabel, StringCodec.INSTANCE));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return row.get(columnIndex, BigDecimalCodec.INSTANCE);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return row.get(columnLabel, BigDecimalCodec.INSTANCE);
  }

  protected void checkClose() throws SQLException {
    if (closed) {
      throw exceptionFactory.create("Operation not permit on a closed resultSet", "HY000");
    }
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClose();
    return rowPointer == -1 && data.size() > 0;
  }

  public abstract boolean isAfterLast() throws SQLException;

  public abstract boolean isFirst() throws SQLException;

  public abstract boolean isLast() throws SQLException;

  public abstract void beforeFirst() throws SQLException;

  public abstract void afterLast() throws SQLException;

  public abstract boolean first() throws SQLException;

  public abstract boolean last() throws SQLException;

  public abstract int getRow() throws SQLException;

  public abstract boolean absolute(int row) throws SQLException;

  public abstract boolean relative(int rows) throws SQLException;

  public abstract boolean previous() throws SQLException;


  @Override
  public int getFetchDirection() {
    return FETCH_UNKNOWN;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction == FETCH_REVERSE) {
      throw exceptionFactory.create(
              "Invalid operation. Allowed direction are ResultSet.FETCH_FORWARD and ResultSet.FETCH_UNKNOWN");
    }
  }


  @Override
  public int getType() {
    return resultSetScrollType;
  }

  @Override
  public int getConcurrency() {
    return CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void insertRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }

  public void setStatement(Statement stmt) throws SQLException {
    statement = stmt;
  }

  public void useAliasAsName() {
    for (ColumnDefinitionPacket packet : metadataList) {
      packet.useAliasAsName();
    }
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw exceptionFactory.notSupported(
            "Method ResultSet.getObject(int columnIndex, Map<String, Class<?>> map) not supported");
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getRef not supported");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return row.get(columnIndex, BlobCodec.INSTANCE);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return row.get(columnIndex, ClobCodec.INSTANCE);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getArray not supported");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw exceptionFactory.notSupported(
            "Method ResultSet.getObject(String columnLabel, Map<String, Class<?>> map) not supported");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getRef not supported");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return row.get(columnLabel, BlobCodec.INSTANCE);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return row.get(columnLabel, ClobCodec.INSTANCE);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getArray not supported");
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    // no calendar consideration, we don't have time value
    return getDate(columnIndex);
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    // no calendar consideration, we don't have time value
    return getDate(columnLabel);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // no calendar consideration, we don't have Date value
    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    // no calendar consideration, we don't have Date value
    return getTime(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    LocalDateTime d = row.get(columnIndex, LocalDateTimeCodec.INSTANCE);

    if (d == null) return null;
    ZonedDateTime d2 = d.atZone(cal.getTimeZone().toZoneId());
    Timestamp timestamp = new Timestamp(d2.toEpochSecond());
    timestamp.setNanos(d2.getNano());
    return timestamp;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(row.getIndex(columnLabel));
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    String s = row.get(columnIndex, StringCodec.INSTANCE);
    if (s == null) return  null;
    try {
      return new URL(s);
    } catch (MalformedURLException e) {
      throw exceptionFactory.create(String.format("Could not parse '%s' as URL", s));
    }
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return getURL(row.getIndex(columnLabel));
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getRef not supported");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw exceptionFactory.notSupported("Method ResultSet.getRef not supported");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw exceptionFactory.notSupported("Array are not supported");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw exceptionFactory.notSupported("Array are not supported");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw exceptionFactory.notSupported("RowId are not supported");
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return row.get(columnIndex, type);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(row.getIndex(columnLabel), type);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      if (isWrapperFor(iface)) {
        return iface.cast(this);
      } else {
        throw new SQLException("The receiver is not a wrapper for " + iface.getName());
      }
    } catch (Exception e) {
      throw new SQLException("The receiver is not a wrapper and does not implement the interface");
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    throw exceptionFactory.notSupported("Not supported when using CONCUR_READ_ONLY concurrency");
  }
}

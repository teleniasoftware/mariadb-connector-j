package org.mariadb.jdbc.client;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadableByteBuf {
  private final AtomicInteger sequence;
  byte[] buf;
  private int pos;
  private int limit;
  private Integer mark;

  public ReadableByteBuf(AtomicInteger sequence, byte[] buf, int limit) {
    this.sequence = sequence;
    this.pos = 0;
    this.buf = buf;
    this.limit = limit;
  }

  public int readableBytes() {
    return limit - pos;
  }

  public int pos() {
    return pos;
  };

  public byte[] buf() {
    return buf;
  };

  public ReadableByteBuf pos(int pos) {
    this.pos = pos;
    return this;
  }

  public ReadableByteBuf mark() {
    mark = pos;
    return this;
  }

  public ReadableByteBuf reset() {
    if (mark == null) throw new IllegalStateException("mark was not set");
    pos = mark;
    return this;
  }

  public ReadableByteBuf skip() {
    pos++;
    return this;
  }

  public ReadableByteBuf skip(int length) {
    pos += length;
    return this;
  }

  public AtomicInteger getSequence() {
    return sequence;
  }

  public byte getByte() {
    return buf[pos];
  }

  public byte getByte(int index) {
    return buf[index];
  }

  public short getUnsignedByte() {
    return (short) (buf[pos] & 0xff);
  }

  public short getUnsignedByte(int index) {
    return (short) (buf[index] & 0xff);
  }

  public short getShort(int index) {
    return (short) ((buf[index] & 0xff) | (buf[index + 1] << 8));
  }

  public int getUnsignedShort(int index) {
    return getShort(index) & 0xffff;
  }

  public int getMedium(int index) {
    int value = getUnsignedMedium(index);
    if ((value & 0x800000) != 0) {
      value |= 0xff000000;
    }
    return value;
  }

  public int getUnsignedMedium(int index) {
    return (buf[index] & 0xff) + ((buf[index + 1] & 0xff) << 8) | (buf[index + 2] << 16);
  }

  public int getInt(int index) {
    return ((buf[index] & 0xff)
        + ((buf[index + 1] & 0xff) << 8)
        + ((buf[index + 2] & 0xff) << 16)
        + ((buf[index + 3] & 0xff) << 24));
  }

  public long getUnsignedInt(int index) {
    return getInt(index) & 0xffffffff;
  }

  public long getLong(int index) {
    return ((buf[index] & 0xff)
        + ((buf[index + 1] & 0xff) << 8)
        + ((buf[index + 2] & 0xff) << 16)
        + ((buf[index + 3] & 0xff) << 24)
        + ((buf[index + 4] & 0xff) << 32)
        + ((buf[index + 5] & 0xff) << 40)
        + ((buf[index + 6] & 0xff) << 48)
        + ((buf[index + 7] & 0xff) << 56));
  }

  public ReadableByteBuf getBytes(int index, byte[] dst) {
    System.arraycopy(buf, index, dst, 0, dst.length);
    return this;
  }

  public int readLengthNotNull() {
    int type = (buf[pos++] & 0xff);
    switch (type) {
      case 251:
        throw new IllegalStateException("Must not have null length");
      case 252:
        return readUnsignedShort();
      case 253:
        return readUnsignedMedium();
      case 254:
        return (int) readLong();
      default:
        return type;
    }
  }

  public Integer readLength() {
    int type = readUnsignedByte();
    switch (type) {
      case 251:
        return null;
      case 252:
        return readUnsignedShort();
      case 253:
        return readUnsignedMedium();
      case 254:
        return (int) readLong();
      default:
        return type;
    }
  }

  public byte readByte() {
    return buf[pos++];
  }

  public short readUnsignedByte() {
    return (short) (buf[pos++] & 0xff);
  }

  public short readShort() {
    return (short) ((buf[pos++] & 0xff) | (buf[pos++] << 8));
  }

  public int readUnsignedShort() {
    return ((buf[pos++] & 0xff) | (buf[pos++] << 8)) & 0xffff;
  }

  public int readMedium() {
    int value = readUnsignedMedium();
    if ((value & 0x800000) != 0) {
      value |= 0xff000000;
    }
    return value;
  }

  public int readUnsignedMedium() {
    return (buf[pos++] & 0xff) + ((buf[pos++] & 0xff) << 8) | (buf[pos++] << 16);
  }

  public int readInt() {
    return ((buf[pos++] & 0xff)
        + ((buf[pos++] & 0xff) << 8)
        + ((buf[pos++] & 0xff) << 16)
        + ((buf[pos++] & 0xff) << 24));
  }

  public long readUnsignedInt() {
    return ((buf[pos++] & 0xff)
            + ((buf[pos++] & 0xff) << 8)
            + ((buf[pos++] & 0xff) << 16)
            + ((buf[pos++] & 0xff) << 24))
        & 0xffffffff;
  }

  public long readLong() {
    return ((buf[pos++] & 0xff)
        + ((buf[pos++] & 0xff) << 8)
        + ((buf[pos++] & 0xff) << 16)
        + ((buf[pos++] & 0xff) << 24)
        + ((buf[pos++] & 0xff) << 32)
        + ((buf[pos++] & 0xff) << 40)
        + ((buf[pos++] & 0xff) << 48)
        + ((buf[pos++] & 0xff) << 56));
  }

  public ReadableByteBuf readBytes(byte[] dst) {
    System.arraycopy(buf, pos, dst, 0, dst.length);
    pos += dst.length;
    return this;
  }

  public byte[] readBytesNullEnd() {
    int initialPosition = pos;
    int cnt = 0;
    while (readableBytes() > 0 && (buf[pos++] != 0)) {
      cnt++;
    }
    byte[] dst = new byte[cnt];
    System.arraycopy(buf, initialPosition, dst, 0, dst.length);
    return dst;
  }

  public ReadableByteBuf readLengthBuffer() {
    int len = readLengthNotNull();
    byte[] tmp = new byte[len];
    readBytes(tmp);
    return new ReadableByteBuf(sequence, tmp, len);
  }

  public String readString(int length) {
    String st = new String(buf, pos, length, StandardCharsets.UTF_8);
    pos += length;
    return st;
  }

  public String readAscii(int length) {
    String st = new String(buf, pos, length, StandardCharsets.US_ASCII);
    pos += length;
    return st;
  }

  public String readStringNullEnd() {
    int initialPosition = pos;
    int cnt = 0;
    while (readableBytes() > 0 && (buf[pos++] != 0)) {
      cnt++;
    }
    return new String(buf, initialPosition, cnt, StandardCharsets.UTF_8);
  }

  public String readStringEof() {
    int initialPosition = pos;
    pos = limit;
    return new String(buf, initialPosition, pos - initialPosition, StandardCharsets.UTF_8);
  }

  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }
}

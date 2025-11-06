package com.github.lukaszbudnik.wal;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.zip.CRC32;

/**
 * Header for WAL pages in the page-based storage format. Each page is configurable (4KB, 8KB, 16KB,
 * 32KB, or 64KB) with a 45-byte header.
 */
class WALPageHeader {
  static final int HEADER_SIZE = 45; // 4+1+8+8+8+8+2+1+1+4 = 45 bytes
  static final int MAGIC_NUMBER = 0xDEADBEEF;
  static final byte STORAGE_FORMAT_VERSION = 1;

  // Continuation flags
  static final byte NO_CONTINUATION = 0;
  static final byte FIRST_PART = 1;
  static final byte MIDDLE_PART = 2;
  static final byte LAST_PART = 4;

  private final int magicNumber;
  private final byte version;
  private final long firstSequence;
  private final long lastSequence;
  private final long firstTimestampMillis;
  private final long lastTimestampMillis;
  private final short entryCount;
  private final byte continuationFlags;
  private final byte pageSizeKB;
  private final int headerCRC;

  WALPageHeader(
      long firstSequence,
      long lastSequence,
      Instant firstTimestamp,
      Instant lastTimestamp,
      short entryCount,
      byte continuationFlags,
      byte pageSizeKB) {
    this.magicNumber = MAGIC_NUMBER;
    this.version = STORAGE_FORMAT_VERSION;
    this.firstSequence = firstSequence;
    this.lastSequence = lastSequence;
    this.firstTimestampMillis = firstTimestamp.toEpochMilli();
    this.lastTimestampMillis = lastTimestamp.toEpochMilli();
    this.entryCount = entryCount;
    this.continuationFlags = continuationFlags;
    this.pageSizeKB = pageSizeKB;
    this.headerCRC = calculateHeaderCRC();
  }

  private WALPageHeader(
      int magicNumber,
      byte version,
      long firstSequence,
      long lastSequence,
      long firstTimestampMillis,
      long lastTimestampMillis,
      short entryCount,
      byte continuationFlags,
      byte pageSizeKB,
      int headerCRC) {
    this.magicNumber = magicNumber;
    this.version = version;
    this.firstSequence = firstSequence;
    this.lastSequence = lastSequence;
    this.firstTimestampMillis = firstTimestampMillis;
    this.lastTimestampMillis = lastTimestampMillis;
    this.entryCount = entryCount;
    this.continuationFlags = continuationFlags;
    this.pageSizeKB = pageSizeKB;
    this.headerCRC = headerCRC;
  }

  private int calculateHeaderCRC() {
    ByteBuffer buffer = ByteBuffer.allocate(41); // Header size (45) minus CRC (4) = 41
    buffer.putInt(magicNumber);
    buffer.put(version);
    buffer.putLong(firstSequence);
    buffer.putLong(lastSequence);
    buffer.putLong(firstTimestampMillis);
    buffer.putLong(lastTimestampMillis);
    buffer.putShort(entryCount);
    buffer.put(continuationFlags);
    buffer.put(pageSizeKB);

    CRC32 crc = new CRC32();
    crc.update(buffer.array());
    return (int) crc.getValue();
  }

  byte[] serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
    buffer.putInt(magicNumber);
    buffer.put(version);
    buffer.putLong(firstSequence);
    buffer.putLong(lastSequence);
    buffer.putLong(firstTimestampMillis);
    buffer.putLong(lastTimestampMillis);
    buffer.putShort(entryCount);
    buffer.put(continuationFlags);
    buffer.put(pageSizeKB);
    buffer.putInt(headerCRC);
    return buffer.array();
  }

  static WALPageHeader deserialize(byte[] data) throws WALException {
    if (data.length != HEADER_SIZE) {
      throw new WALException("Invalid header size: " + data.length);
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);
    int magicNumber = buffer.getInt();
    byte version = buffer.get();
    long firstSequence = buffer.getLong();
    long lastSequence = buffer.getLong();
    long firstTimestampMillis = buffer.getLong();
    long lastTimestampMillis = buffer.getLong();
    short entryCount = buffer.getShort();
    byte continuationFlags = buffer.get();
    byte pageSizeKB = buffer.get();
    int headerCRC = buffer.getInt();

    WALPageHeader header =
        new WALPageHeader(
            magicNumber,
            version,
            firstSequence,
            lastSequence,
            firstTimestampMillis,
            lastTimestampMillis,
            entryCount,
            continuationFlags,
            pageSizeKB,
            headerCRC);

    if (magicNumber != MAGIC_NUMBER) {
      throw new WALException("Invalid magic number: " + Integer.toHexString(magicNumber));
    }

    if (header.calculateHeaderCRC() != headerCRC) {
      throw new WALException("Header CRC mismatch");
    }

    return header;
  }

  // Getters
  byte getVersion() {
    return version;
  }

  long getFirstSequence() {
    return firstSequence;
  }

  long getLastSequence() {
    return lastSequence;
  }

  Instant getFirstTimestamp() {
    return Instant.ofEpochMilli(firstTimestampMillis);
  }

  Instant getLastTimestamp() {
    return Instant.ofEpochMilli(lastTimestampMillis);
  }

  short getEntryCount() {
    return entryCount;
  }

  byte getContinuationFlags() {
    return continuationFlags;
  }

  byte getPageSizeKB() {
    return pageSizeKB;
  }

  boolean isSpanningRecord() {
    return continuationFlags != NO_CONTINUATION;
  }

  boolean isFirstPart() {
    return (continuationFlags & FIRST_PART) != 0;
  }

  boolean isMiddlePart() {
    return (continuationFlags & MIDDLE_PART) != 0;
  }

  boolean isLastPart() {
    return (continuationFlags & LAST_PART) != 0;
  }
}

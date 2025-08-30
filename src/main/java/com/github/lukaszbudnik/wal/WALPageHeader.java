package com.github.lukaszbudnik.wal;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.zip.CRC32;

/**
 * Header for WAL pages in the page-based storage format. Each page is 4KB (4096 bytes) with a
 * 52-byte header.
 */
class WALPageHeader {
  static final int HEADER_SIZE = 52; // 4+8+8+8+4+8+4+2+2+4 = 52 bytes
  static final int MAGIC_NUMBER = 0xDEADBEEF;

  // Continuation flags
  static final short NO_CONTINUATION = 0;
  static final short FIRST_PART = 1;
  static final short MIDDLE_PART = 2;
  static final short LAST_PART = 4;

  private final int magicNumber;
  private final long firstSequence;
  private final long lastSequence;
  private final long firstTimestampSeconds;
  private final int firstTimestampNanos;
  private final long lastTimestampSeconds;
  private final int lastTimestampNanos;
  private final short entryCount;
  private final short continuationFlags;
  private final int headerCRC;

  WALPageHeader(
      long firstSequence,
      long lastSequence,
      Instant firstTimestamp,
      Instant lastTimestamp,
      short entryCount,
      short continuationFlags) {
    this.magicNumber = MAGIC_NUMBER;
    this.firstSequence = firstSequence;
    this.lastSequence = lastSequence;
    this.firstTimestampSeconds = firstTimestamp.getEpochSecond();
    this.firstTimestampNanos = firstTimestamp.getNano();
    this.lastTimestampSeconds = lastTimestamp.getEpochSecond();
    this.lastTimestampNanos = lastTimestamp.getNano();
    this.entryCount = entryCount;
    this.continuationFlags = continuationFlags;
    this.headerCRC = calculateHeaderCRC();
  }

  private WALPageHeader(
      int magicNumber,
      long firstSequence,
      long lastSequence,
      long firstTimestampSeconds,
      int firstTimestampNanos,
      long lastTimestampSeconds,
      int lastTimestampNanos,
      short entryCount,
      short continuationFlags,
      int headerCRC) {
    this.magicNumber = magicNumber;
    this.firstSequence = firstSequence;
    this.lastSequence = lastSequence;
    this.firstTimestampSeconds = firstTimestampSeconds;
    this.firstTimestampNanos = firstTimestampNanos;
    this.lastTimestampSeconds = lastTimestampSeconds;
    this.lastTimestampNanos = lastTimestampNanos;
    this.entryCount = entryCount;
    this.continuationFlags = continuationFlags;
    this.headerCRC = headerCRC;
  }

  private int calculateHeaderCRC() {
    ByteBuffer buffer = ByteBuffer.allocate(48); // Header size (52) minus CRC (4) = 48
    buffer.putInt(magicNumber);
    buffer.putLong(firstSequence);
    buffer.putLong(lastSequence);
    buffer.putLong(firstTimestampSeconds);
    buffer.putInt(firstTimestampNanos);
    buffer.putLong(lastTimestampSeconds);
    buffer.putInt(lastTimestampNanos);
    buffer.putShort(entryCount);
    buffer.putShort(continuationFlags);

    CRC32 crc = new CRC32();
    crc.update(buffer.array());
    return (int) crc.getValue();
  }

  byte[] serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
    buffer.putInt(magicNumber);
    buffer.putLong(firstSequence);
    buffer.putLong(lastSequence);
    buffer.putLong(firstTimestampSeconds);
    buffer.putInt(firstTimestampNanos);
    buffer.putLong(lastTimestampSeconds);
    buffer.putInt(lastTimestampNanos);
    buffer.putShort(entryCount);
    buffer.putShort(continuationFlags);
    buffer.putInt(headerCRC);
    return buffer.array();
  }

  static WALPageHeader deserialize(byte[] data) throws WALException {
    if (data.length != HEADER_SIZE) {
      throw new WALException("Invalid header size: " + data.length);
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);
    int magicNumber = buffer.getInt();
    long firstSequence = buffer.getLong();
    long lastSequence = buffer.getLong();
    long firstTimestampSeconds = buffer.getLong();
    int firstTimestampNanos = buffer.getInt();
    long lastTimestampSeconds = buffer.getLong();
    int lastTimestampNanos = buffer.getInt();
    short entryCount = buffer.getShort();
    short continuationFlags = buffer.getShort();
    int headerCRC = buffer.getInt();

    WALPageHeader header =
        new WALPageHeader(
            magicNumber,
            firstSequence,
            lastSequence,
            firstTimestampSeconds,
            firstTimestampNanos,
            lastTimestampSeconds,
            lastTimestampNanos,
            entryCount,
            continuationFlags,
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
  long getFirstSequence() {
    return firstSequence;
  }

  long getLastSequence() {
    return lastSequence;
  }

  Instant getFirstTimestamp() {
    return Instant.ofEpochSecond(firstTimestampSeconds, firstTimestampNanos);
  }

  Instant getLastTimestamp() {
    return Instant.ofEpochSecond(lastTimestampSeconds, lastTimestampNanos);
  }

  short getEntryCount() {
    return entryCount;
  }

  short getContinuationFlags() {
    return continuationFlags;
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

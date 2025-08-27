package com.github.lukaszbudnik.wal;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents a single entry in the Write Ahead Log. Each entry contains a sequence number,
 * timestamp, and data payload.
 *
 * <p>This is a simple POJO - serialization is handled by the WAL implementation. Record types and
 * transaction IDs should be embedded in the data payload by the application.
 */
public class WALEntry {

  private final long sequenceNumber;
  private final Instant timestamp;
  private final ByteBuffer data;

  WALEntry(long sequenceNumber, Instant timestamp, ByteBuffer data) {
    this.sequenceNumber = sequenceNumber;
    this.timestamp = timestamp;

    // Create a defensive copy of the data
    if (data != null) {
      ByteBuffer duplicate = data.duplicate();
      duplicate.rewind();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      this.data = ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    } else {
      this.data = null;
    }
  }

  // Getters
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public ByteBuffer getData() {
    return data != null ? data.asReadOnlyBuffer() : null;
  }

  /** Get data as byte array for convenience */
  public byte[] getDataAsBytes() {
    if (data == null) {
      return null;
    }
    ByteBuffer duplicate = data.duplicate();
    duplicate.rewind();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WALEntry walEntry = (WALEntry) o;
    return sequenceNumber == walEntry.sequenceNumber
        && Objects.equals(timestamp, walEntry.timestamp)
        && Objects.equals(data, walEntry.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sequenceNumber, timestamp, data);
  }

  @Override
  public String toString() {
    return String.format(
        "WALEntry{seq=%d, timestamp=%s, dataSize=%d}",
        sequenceNumber, timestamp, data != null ? data.remaining() : 0);
  }
}

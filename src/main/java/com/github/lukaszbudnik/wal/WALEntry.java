package com.github.lukaszbudnik.wal;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;
import java.util.zip.CRC32;

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

  /**
   * Deserialize a WAL entry from binary data with CRC32 validation.
   *
   * @param data the serialized entry data
   * @return the deserialized WAL entry
   * @throws WALException if the data is invalid or CRC32 validation fails
   */
  static WALEntry deserialize(byte[] data) throws WALException {
    if (data.length < FileBasedWAL.ENTRY_HEADER_SIZE) { // Minimum size: 1+8+8+4+4 bytes
      throw new WALException("Invalid entry data: too short");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);

    // Read entry fields
    byte entryType = buffer.get();
    long sequenceNumber = buffer.getLong();
    long timestampMillis = buffer.getLong();
    int dataLength = buffer.getInt();

    int crc32Lenght = 4;

    if (dataLength < 0 || buffer.remaining() < dataLength + crc32Lenght) {
      throw new WALException("Invalid entry data: invalid data length");
    }

    // Read data and CRC
    byte[] entryData = new byte[dataLength];
    buffer.get(entryData);
    int storedCrc = buffer.getInt();

    // Validate CRC32
    CRC32 crc = new CRC32();
    ByteBuffer validationBuffer =
        ByteBuffer.allocate(
            FileBasedWAL.ENTRY_HEADER_SIZE + dataLength - crc32Lenght); // header + data - crc32
    validationBuffer.put(entryType);
    validationBuffer.putLong(sequenceNumber);
    validationBuffer.putLong(timestampMillis);
    validationBuffer.putInt(dataLength);
    validationBuffer.put(entryData);
    validationBuffer.flip();
    crc.update(validationBuffer);

    if ((int) crc.getValue() != storedCrc) {
      throw new WALException("Entry CRC32 validation failed for sequence " + sequenceNumber);
    }

    // Create entry
    Instant timestamp = Instant.ofEpochMilli(timestampMillis);
    ByteBuffer dataBuffer = dataLength > 0 ? ByteBuffer.wrap(entryData) : null;
    return new WALEntry(sequenceNumber, timestamp, dataBuffer);
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

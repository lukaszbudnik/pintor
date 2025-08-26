package com.github.lukaszbudnik.wal;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * High-level manager for Write Ahead Log operations. Provides convenience methods and manages
 * sequence number generation. All methods are thread-safe.
 */
public class WALManager implements AutoCloseable {

  private final WriteAheadLog wal;

  public WALManager(WriteAheadLog wal) {
    this.wal = wal;
  }

  public WALManager(Path walDirectory) throws WALException {
    this.wal = new FileBasedWAL(walDirectory);
  }

  /**
   * Create a new WAL entry with auto-generated sequence number and current timestamp. This method
   * is thread-safe - sequence number generation and appending are atomic.
   */
  public WALEntry createEntry(ByteBuffer data) throws WALException {
    return wal.createAndAppend(data);
  }

  /**
   * Create a new WAL entry with auto-generated sequence number (convenience method for byte array
   * data). This method is thread-safe - sequence number generation and appending are atomic.
   */
  public WALEntry createEntry(byte[] data) throws WALException {
    ByteBuffer buffer = data != null ? ByteBuffer.wrap(data) : null;
    return wal.createAndAppend(buffer);
  }

  /**
   * Create multiple WAL entries atomically with auto-generated consecutive sequence numbers. This
   * method is thread-safe - all entries are created and appended as a single atomic operation.
   */
  public List<WALEntry> createEntryBatch(List<ByteBuffer> dataList) throws WALException {
    return wal.createAndAppendBatch(dataList);
  }

  /**
   * Create multiple WAL entries atomically (convenience method for byte array data). This method is
   * thread-safe - all entries are created and appended as a single atomic operation.
   */
  public List<WALEntry> createEntryBatchFromBytes(List<byte[]> dataList) throws WALException {
    List<ByteBuffer> buffers = new java.util.ArrayList<>(dataList.size());
    for (byte[] data : dataList) {
      buffers.add(data != null ? ByteBuffer.wrap(data) : null);
    }
    return wal.createAndAppendBatch(buffers);
  }

  /** Force sync to persistent storage */
  public void sync() throws WALException {
    wal.sync();
  }

  /** Read all entries from a specific sequence number */
  public List<WALEntry> readFrom(long fromSequenceNumber) throws WALException {
    return wal.readFrom(fromSequenceNumber);
  }

  /** Read entries in a specific range */
  public List<WALEntry> readRange(long fromSequenceNumber, long toSequenceNumber)
      throws WALException {
    return wal.readRange(fromSequenceNumber, toSequenceNumber);
  }

  /** Read all entries from a specific timestamp */
  public List<WALEntry> readFrom(Instant fromTimestamp) throws WALException {
    return wal.readFrom(fromTimestamp);
  }

  /** Read entries in a specific timestamp range */
  public List<WALEntry> readRange(Instant fromTimestamp, Instant toTimestamp) throws WALException {
    return wal.readRange(fromTimestamp, toTimestamp);
  }

  /** Get the current sequence number */
  public long getCurrentSequenceNumber() {
    return wal.getCurrentSequenceNumber();
  }

  /**
   * Get the next sequence number that will be used. Note: This is for informational purposes only.
   * For thread-safe entry creation, use createEntry() or createEntryBatch() methods instead.
   */
  public long getNextSequenceNumber() {
    return wal.getNextSequenceNumber();
  }

  /** Truncate the log up to a specific sequence number */
  public void truncate(long upToSequenceNumber) throws WALException {
    wal.truncate(upToSequenceNumber);
  }

  /** Get the total number of entries */
  public long size() {
    return wal.size();
  }

  /** Check if the WAL is empty */
  public boolean isEmpty() {
    return wal.isEmpty();
  }

  @Override
  public void close() throws Exception {
    sync();
    wal.close();
  }
}

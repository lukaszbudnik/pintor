package com.github.lukaszbudnik.wal;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import org.reactivestreams.Publisher;

/**
 * Interface defining the contract for Write Ahead Log implementations.
 *
 * <p>A Write Ahead Log ensures durability by writing all changes to persistent storage before they
 * are applied to the main data structure.
 */
public interface WriteAheadLog extends AutoCloseable {

  /**
   * Create and append a single entry atomically with auto-generated sequence number. This method is
   * thread-safe and handles sequence number generation internally.
   *
   * @param data the data to store in the entry
   * @return the created WALEntry with assigned sequence number
   * @throws WALException if the operation fails
   */
  WALEntry createAndAppend(ByteBuffer data) throws WALException;

  /**
   * Create and append multiple entries atomically with auto-generated sequence numbers. This method
   * is thread-safe and handles sequence number generation internally.
   *
   * @param dataList list of data to store in entries
   * @return list of created WALEntries with assigned sequence numbers
   * @throws WALException if the operation fails
   */
  List<WALEntry> createAndAppendBatch(List<ByteBuffer> dataList) throws WALException;

  /**
   * Force all buffered entries to be written to persistent storage
   *
   * @throws WALException if the operation fails
   */
  void sync() throws WALException;

  /**
   * Read entries starting from the given sequence number
   *
   * @param fromSequenceNumber the starting sequence number (inclusive)
   * @return publisher of entries
   */
  Publisher<WALEntry> readFrom(long fromSequenceNumber);

  /**
   * Read entries in a sequence number range
   *
   * @param fromSequenceNumber the starting sequence number (inclusive)
   * @param toSequenceNumber the ending sequence number (inclusive)
   * @return publisher of entries
   */
  Publisher<WALEntry> readRange(long fromSequenceNumber, long toSequenceNumber);

  /**
   * Read entries starting from the given timestamp
   *
   * @param fromTimestamp the starting timestamp (inclusive)
   * @return publisher of entries with timestamps >= fromTimestamp
   */
  Publisher<WALEntry> readFrom(Instant fromTimestamp);

  /**
   * Read entries in a timestamp range
   *
   * @param fromTimestamp the starting timestamp (inclusive)
   * @param toTimestamp the ending timestamp (inclusive)
   * @return publisher of entries with timestamps in the specified range
   */
  Publisher<WALEntry> readRange(Instant fromTimestamp, Instant toTimestamp);

  /**
   * Get the current sequence number (last written entry)
   *
   * @return the current sequence number, or -1 if no entries exist
   */
  long getCurrentSequenceNumber();

  /**
   * Get the next sequence number that should be used for new entries
   *
   * @return the next sequence number
   */
  long getNextSequenceNumber();

  /**
   * Truncate the log up to the given sequence number (exclusive) This is typically called after a
   * checkpoint to remove old entries
   *
   * @param upToSequenceNumber entries before this sequence number will be removed
   * @throws WALException if the operation fails
   */
  void truncate(long upToSequenceNumber) throws WALException;

  /**
   * Get the total number of entries in the WAL
   *
   * @return the number of entries
   */
  long size();

  /**
   * Check if the WAL is empty
   *
   * @return true if empty, false otherwise
   */
  boolean isEmpty();

  /**
   * Get operational metrics for monitoring and performance analysis
   *
   * @return WAL metrics instance with cumulative statistics
   */
  WALMetrics getMetrics();
}

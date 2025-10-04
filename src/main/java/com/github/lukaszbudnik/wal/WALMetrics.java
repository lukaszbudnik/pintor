package com.github.lukaszbudnik.wal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe metrics tracking for Write Ahead Log operations.
 *
 * <p>Provides comprehensive operational statistics including write operations, read operations, and
 * I/O efficiency metrics. All counters are cumulative and thread-safe using atomic operations.
 */
public class WALMetrics {

  // Write metrics
  private final AtomicLong entriesWritten = new AtomicLong();
  private final AtomicLong bytesWritten = new AtomicLong();
  private final AtomicLong pagesWritten = new AtomicLong();
  private final AtomicLong filesCreated = new AtomicLong();

  // Read metrics
  private final AtomicLong entriesRead = new AtomicLong();
  private final AtomicLong pagesRead = new AtomicLong();
  private final AtomicLong rangeQueries = new AtomicLong();

  // I/O efficiency metrics
  private final AtomicLong filesScanned = new AtomicLong();
  private final AtomicLong pagesScanned = new AtomicLong();

  /**
   * Get the total number of entries written to the WAL.
   *
   * @return cumulative count of entries written
   */
  public long getEntriesWritten() {
    return entriesWritten.get();
  }

  /**
   * Get the total number of bytes written to the WAL.
   *
   * @return cumulative bytes written
   */
  public long getBytesWritten() {
    return bytesWritten.get();
  }

  /**
   * Get the total number of pages written to the WAL.
   *
   * @return cumulative count of pages written
   */
  public long getPagesWritten() {
    return pagesWritten.get();
  }

  /**
   * Get the total number of WAL files created.
   *
   * @return cumulative count of files created
   */
  public long getFilesCreated() {
    return filesCreated.get();
  }

  /**
   * Get the total number of entries read from the WAL.
   *
   * @return cumulative count of entries read
   */
  public long getEntriesRead() {
    return entriesRead.get();
  }

  /**
   * Get the total number of pages read from the WAL.
   *
   * @return cumulative count of pages read
   */
  public long getPagesRead() {
    return pagesRead.get();
  }

  /**
   * Get the total number of range queries performed.
   *
   * @return cumulative count of range queries
   */
  public long getRangeQueries() {
    return rangeQueries.get();
  }

  /**
   * Get the total number of WAL files scanned during read operations.
   *
   * @return cumulative count of files scanned
   */
  public long getFilesScanned() {
    return filesScanned.get();
  }

  /**
   * Get the total number of pages scanned during read operations.
   *
   * @return cumulative count of pages scanned
   */
  public long getPagesScanned() {
    return pagesScanned.get();
  }

  // Package-private methods for internal metric updates

  void incrementEntriesWritten() {
    entriesWritten.incrementAndGet();
  }

  void incrementEntriesWritten(long count) {
    entriesWritten.addAndGet(count);
  }

  void incrementBytesWritten(long bytes) {
    bytesWritten.addAndGet(bytes);
  }

  void incrementPagesWritten() {
    pagesWritten.incrementAndGet();
  }

  void incrementFilesCreated() {
    filesCreated.incrementAndGet();
  }

  void incrementEntriesRead() {
    entriesRead.incrementAndGet();
  }

  void incrementEntriesRead(long count) {
    entriesRead.addAndGet(count);
  }

  void incrementPagesRead() {
    pagesRead.incrementAndGet();
  }

  void incrementRangeQueries() {
    rangeQueries.incrementAndGet();
  }

  void incrementFilesScanned() {
    filesScanned.incrementAndGet();
  }

  void incrementFilesScanned(long count) {
    filesScanned.addAndGet(count);
  }

  void incrementPagesScanned() {
    pagesScanned.incrementAndGet();
  }

  void incrementPagesScanned(long count) {
    pagesScanned.addAndGet(count);
  }
}

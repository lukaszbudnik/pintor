package com.github.lukaszbudnik.wal;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Functional interface for file rotation callbacks. Called when a WAL file is rotated to enable
 * archival, backup, and disaster recovery.
 */
@FunctionalInterface
public interface FileRotationCallback {

  /**
   * Called when a WAL file rotation occurs.
   *
   * @param firstSequenceNumber sequence number of the first entry in the rotated file
   * @param firstTimestamp timestamp of the first entry in the rotated file
   * @param lastSequenceNumber sequence number of the last entry in the rotated file
   * @param lastTimestamp timestamp of the last entry in the rotated file
   * @param filePath path to the rotated file
   */
  void onFileRotated(
      long firstSequenceNumber,
      Instant firstTimestamp,
      long lastSequenceNumber,
      Instant lastTimestamp,
      Path filePath);
}

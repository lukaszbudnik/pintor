package com.github.lukaszbudnik.wal;

import java.time.Instant;

/**
 * Helper class to represent the first entry in a WAL file. Used for efficient sequence range
 * building.
 */
class SequenceTimestamp {
  final long sequence;
  final Instant timestamp;

  SequenceTimestamp(long sequence, Instant timestamp) {
    this.sequence = sequence;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return String.format("SequenceTimestamp[seq=%d, time=%s]", sequence, timestamp);
  }
}

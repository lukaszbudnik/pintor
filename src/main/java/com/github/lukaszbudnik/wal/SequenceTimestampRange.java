package com.github.lukaszbudnik.wal;

import java.time.Instant;

/**
 * Represents the sequence number and timestamp ranges contained in a WAL file. Used for efficient
 * file selection during range queries.
 */
public class SequenceTimestampRange {
  private final long minSequence;
  private final long maxSequence;
  private final Instant minTimestamp;
  private final Instant maxTimestamp;

  public SequenceTimestampRange(
      long minSequence, long maxSequence, Instant minTimestamp, Instant maxTimestamp) {
    if (minSequence > maxSequence) {
      throw new IllegalArgumentException("minSequence cannot be greater than maxSequence");
    }
    if (minTimestamp.isAfter(maxTimestamp)) {
      throw new IllegalArgumentException("minTimestamp cannot be after maxTimestamp");
    }
    this.minSequence = minSequence;
    this.maxSequence = maxSequence;
    this.minTimestamp = minTimestamp;
    this.maxTimestamp = maxTimestamp;
  }

  public long getMinSequence() {
    return minSequence;
  }

  public long getMaxSequence() {
    return maxSequence;
  }

  public Instant getMinTimestamp() {
    return minTimestamp;
  }

  public Instant getMaxTimestamp() {
    return maxTimestamp;
  }

  /** Checks if this range contains the given sequence number. */
  public boolean contains(long sequenceNumber) {
    return sequenceNumber >= minSequence && sequenceNumber <= maxSequence;
  }

  /** Checks if this range contains the given timestamp. */
  public boolean contains(Instant timestamp) {
    return !timestamp.isBefore(minTimestamp) && !timestamp.isAfter(maxTimestamp);
  }

  /** Checks if this range overlaps with the given sequence range. */
  public boolean overlaps(long fromSequence, long toSequence) {
    return !(toSequence < minSequence || fromSequence > maxSequence);
  }

  /** Checks if this range overlaps with the given timestamp range. */
  public boolean overlaps(Instant fromTimestamp, Instant toTimestamp) {
    return !(toTimestamp.isBefore(minTimestamp) || fromTimestamp.isAfter(maxTimestamp));
  }

  /** Checks if this range is completely before the given sequence. */
  public boolean isBefore(long sequenceNumber) {
    return maxSequence < sequenceNumber;
  }

  /** Checks if this range is completely before the given timestamp. */
  public boolean isBefore(Instant timestamp) {
    return maxTimestamp.isBefore(timestamp);
  }

  /** Checks if this range is completely after the given sequence. */
  public boolean isAfter(long sequenceNumber) {
    return minSequence > sequenceNumber;
  }

  /** Checks if this range is completely after the given timestamp. */
  public boolean isAfter(Instant timestamp) {
    return minTimestamp.isAfter(timestamp);
  }

  @Override
  public String toString() {
    return String.format(
        "SequenceRange[seq:%d-%d, time:%s-%s]",
        minSequence, maxSequence, minTimestamp, maxTimestamp);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    SequenceTimestampRange that = (SequenceTimestampRange) obj;
    return minSequence == that.minSequence
        && maxSequence == that.maxSequence
        && minTimestamp.equals(that.minTimestamp)
        && maxTimestamp.equals(that.maxTimestamp);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(minSequence)
        ^ Long.hashCode(maxSequence)
        ^ minTimestamp.hashCode()
        ^ maxTimestamp.hashCode();
  }
}

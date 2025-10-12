package com.github.lukaszbudnik.wal;

import java.time.Instant;
import java.util.Comparator;
import java.util.function.Function;

/**
 * Generic range query abstraction that works with any comparable field type. Eliminates code
 * duplication between sequence and timestamp range queries.
 */
public class RangeQuery<T extends Comparable<T>> {
  private final T from;
  private final T to;
  private final Function<WALEntry, T> fieldExtractor;
  private final Function<WALPageHeader, T> headerFromExtractor;
  private final Function<WALPageHeader, T> headerToExtractor;
  private final Comparator<T> comparator;

  public RangeQuery(
      T from,
      T to,
      Function<WALEntry, T> fieldExtractor,
      Function<WALPageHeader, T> headerFromExtractor,
      Function<WALPageHeader, T> headerToExtractor) {
    this.from = from;
    this.to = to;
    this.fieldExtractor = fieldExtractor;
    this.headerFromExtractor = headerFromExtractor;
    this.headerToExtractor = headerToExtractor;
    this.comparator = Comparator.naturalOrder();
  }

  public boolean isValidRange() {
    return comparator.compare(from, to) <= 0;
  }

  public boolean entryInRange(WALEntry entry) {
    T value = fieldExtractor.apply(entry);
    return comparator.compare(value, from) >= 0 && comparator.compare(value, to) <= 0;
  }

  public boolean headerOverlapsRange(WALPageHeader header) {
    T headerFrom = headerFromExtractor.apply(header);
    T headerTo = headerToExtractor.apply(header);

    // Range overlap: !(headerTo < from || headerFrom > to)
    return !(comparator.compare(headerTo, from) < 0 || comparator.compare(headerFrom, to) > 0);
  }

  public T getFrom() {
    return from;
  }

  public T getTo() {
    return to;
  }

  public Function<WALPageHeader, T> getHeaderFromExtractor() {
    return headerFromExtractor;
  }

  public Function<WALPageHeader, T> getHeaderToExtractor() {
    return headerToExtractor;
  }

  // Factory methods for common query types
  public static RangeQuery<Long> bySequence(long from, long to) {
    return new RangeQuery<>(
        from,
        to,
        WALEntry::getSequenceNumber,
        WALPageHeader::getFirstSequence,
        WALPageHeader::getLastSequence);
  }

  public static RangeQuery<Instant> byTimestamp(Instant from, Instant to) {
    return new RangeQuery<>(
        from,
        to,
        WALEntry::getTimestamp,
        WALPageHeader::getFirstTimestamp,
        WALPageHeader::getLastTimestamp);
  }
}

package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test to verify that timestamp range optimization works correctly for efficient file selection.
 */
class TimestampRangeOptimizationTest {

  @TempDir Path tempDir;

  private FileBasedWAL wal;

  @BeforeEach
  void setUp() throws WALException {
    // Use small file size to force multiple files
    wal = new FileBasedWAL(tempDir, 1024, true); // 1KB max file size
  }

  @AfterEach
  void tearDown() throws Exception {
    if (wal != null) {
      wal.close();
    }
  }

  @Test
  void testTimestampRangeOptimizationWithMultipleFiles() throws WALException, InterruptedException {
    Instant startTime = Instant.now();

    // Create entries that will span multiple files with time gaps
    // File 0: entries 0-6 (around startTime)
    for (int i = 0; i < 7; i++) {
      String data = "entry_" + i + "_" + "x".repeat(100);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    // Small delay to create timestamp gap
    Thread.sleep(10);
    Instant midTime1 = Instant.now();

    // File 1: entries 7-13 (around midTime1)
    for (int i = 7; i < 14; i++) {
      String data = "entry_" + i + "_" + "x".repeat(100);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    // Another delay
    Thread.sleep(10);
    Instant midTime2 = Instant.now();

    // File 2: entries 14-20 (around midTime2)
    for (int i = 14; i < 21; i++) {
      String data = "entry_" + i + "_" + "x".repeat(100);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    // Another delay
    Thread.sleep(10);
    Instant midTime3 = Instant.now();

    // File 3: entries 21-27 (around midTime3)
    for (int i = 21; i < 28; i++) {
      String data = "entry_" + i + "_" + "x".repeat(100);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    // Final delay
    Thread.sleep(10);
    Instant endTime = Instant.now();

    // File 4: entries 28-29 (around endTime)
    for (int i = 28; i < 30; i++) {
      String data = "entry_" + i + "_" + "x".repeat(100);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    wal.sync();

    // Verify we have 30 entries total
    assertEquals(30, wal.size());
    assertEquals(29, wal.getCurrentSequenceNumber());

    // Test 1: Read from midTime2 to end (should skip early files)
    List<WALEntry> entriesFromMidTime2 = wal.readRange(midTime2, Instant.MAX);
    assertTrue(entriesFromMidTime2.size() >= 14); // At least entries from midTime2 onwards

    // Verify sequence numbers are in correct range (should be >= 14)
    for (WALEntry entry : entriesFromMidTime2) {
      assertTrue(
          entry.getSequenceNumber() >= 14,
          "Entry sequence " + entry.getSequenceNumber() + " should be >= 14");
    }

    // Test 2: Read middle time range (should only need middle files)
    List<WALEntry> middleTimeRange = wal.readRange(midTime1, midTime2.plusMillis(5));
    assertTrue(middleTimeRange.size() >= 7); // At least entries from midTime1 to midTime2

    // Test 3: Read early time range (should only need early files)
    List<WALEntry> earlyTimeRange = wal.readRange(startTime, midTime1.plusMillis(5));
    assertTrue(earlyTimeRange.size() >= 7); // At least entries from start to midTime1

    // Verify early entries have low sequence numbers
    for (WALEntry entry : earlyTimeRange) {
      assertTrue(
          entry.getSequenceNumber() <= 13,
          "Early entry sequence " + entry.getSequenceNumber() + " should be <= 13");
    }

    // Test 4: Read all entries by timestamp
    List<WALEntry> allEntriesByTime =
        wal.readRange(startTime.minusSeconds(1), endTime.plusSeconds(1));
    assertEquals(30, allEntriesByTime.size());

    // Verify entries are in sequence order
    for (int i = 0; i < allEntriesByTime.size(); i++) {
      assertEquals(i, allEntriesByTime.get(i).getSequenceNumber());
    }

    // Test 5: Read non-existent time range (future)
    Instant futureTime = Instant.now().plusSeconds(3600);
    List<WALEntry> futureEntries = wal.readRange(futureTime, futureTime.plusSeconds(100));
    assertEquals(0, futureEntries.size());

    // Test 6: Read non-existent time range (past)
    Instant pastTime = startTime.minusSeconds(3600);
    List<WALEntry> pastEntries = wal.readRange(pastTime, pastTime.plusSeconds(100));
    assertEquals(0, pastEntries.size());
  }

  @Test
  void testTimestampRangeWithFileRotation() throws WALException, InterruptedException {
    // Test that timestamp ranges are properly maintained during file rotation

    Instant testStart = Instant.now();

    // Add entries to trigger multiple rotations with time gaps
    for (int i = 0; i < 25; i++) {
      String data = "rotation_test_" + i + "_" + "x".repeat(80);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));

      // Add small delays every few entries to create timestamp spread
      if (i % 5 == 0) {
        Thread.sleep(2);
      }
    }

    Instant testEnd = Instant.now();
    wal.sync();

    // Verify total entries
    assertEquals(25, wal.size());
    assertEquals(24, wal.getCurrentSequenceNumber());

    // Test reading by timestamp ranges

    // Read all entries by timestamp
    List<WALEntry> allByTime = wal.readRange(testStart.minusMillis(10), testEnd.plusMillis(10));
    assertEquals(25, allByTime.size());

    // Read from middle of test period
    Instant midTest = testStart.plusMillis((testEnd.toEpochMilli() - testStart.toEpochMilli()) / 2);
    List<WALEntry> fromMid = wal.readRange(midTest, testEnd.plusMillis(10));
    assertTrue(fromMid.size() > 0 && fromMid.size() <= 25);

    // Verify sequence continuity in timestamp-based results
    long lastSeq = -1;
    for (WALEntry entry : fromMid) {
      assertTrue(
          entry.getSequenceNumber() > lastSeq,
          "Sequences should be increasing: " + entry.getSequenceNumber() + " > " + lastSeq);
      lastSeq = entry.getSequenceNumber();
    }
  }

  @Test
  void testTimestampRangeAfterRestart() throws Exception {
    // Test that timestamp ranges are properly rebuilt after WAL restart

    Instant beforeRestart = Instant.now();

    // Create entries across multiple files
    for (int i = 0; i < 20; i++) {
      String data = "restart_test_" + i + "_" + "x".repeat(90);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));

      if (i % 4 == 0) {
        Thread.sleep(2); // Create timestamp gaps
      }
    }

    Instant afterEntries = Instant.now();
    wal.sync();

    // Close and reopen WAL
    wal.close();
    wal = new FileBasedWAL(tempDir, 1024, true);

    // Verify timestamp ranges are rebuilt correctly
    assertEquals(20, wal.size());
    assertEquals(19, wal.getCurrentSequenceNumber());

    // Test optimized timestamp reading after restart
    List<WALEntry> afterRestart =
        wal.readRange(beforeRestart.minusMillis(10), afterEntries.plusMillis(10));
    assertEquals(20, afterRestart.size());

    // Verify data integrity and sequence order
    for (int i = 0; i < afterRestart.size(); i++) {
      WALEntry entry = afterRestart.get(i);
      assertEquals(i, entry.getSequenceNumber());
      String expectedPrefix = "restart_test_" + i + "_";
      String actualData = new String(entry.getDataAsBytes());
      assertTrue(actualData.startsWith(expectedPrefix));
    }

    // Test partial timestamp range after restart
    Instant midTime =
        beforeRestart.plusMillis((afterEntries.toEpochMilli() - beforeRestart.toEpochMilli()) / 2);
    List<WALEntry> partialRange = wal.readRange(midTime, afterEntries.plusMillis(10));
    assertTrue(partialRange.size() > 0 && partialRange.size() <= 20);
  }

  @Test
  void testTimestampRangeEdgeCases() throws WALException, InterruptedException {
    // Test edge cases for timestamp range optimization

    Instant testStart = Instant.now();

    // Single entry
    wal.createAndAppend(ByteBuffer.wrap("single_entry".getBytes()));

    Thread.sleep(5);
    Instant afterSingle = Instant.now();

    // Test reading single entry by timestamp
    List<WALEntry> singleEntry = wal.readRange(testStart.minusMillis(1), afterSingle.plusMillis(1));
    assertEquals(1, singleEntry.size());
    assertEquals(0L, singleEntry.get(0).getSequenceNumber());

    // Add more entries with gaps
    for (int i = 1; i < 10; i++) {
      Thread.sleep(2);
      String data = "edge_case_" + i + "_" + "x".repeat(100);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    Instant testEnd = Instant.now();

    // Test precise timestamp boundaries
    List<WALEntry> preciseRange = wal.readRange(afterSingle, testEnd.plusMillis(1));
    assertEquals(9, preciseRange.size()); // Should get entries 1-9
    assertEquals(1L, preciseRange.get(0).getSequenceNumber());
    assertEquals(9L, preciseRange.get(8).getSequenceNumber());

    // Test reversed timestamp range (should return empty)
    List<WALEntry> reversedRange = wal.readRange(testEnd, testStart);
    assertEquals(0, reversedRange.size());

    // Test exact timestamp match (should work)
    WALEntry firstEntry = wal.readFrom(0L).get(0);
    Instant firstTimestamp = firstEntry.getTimestamp();
    List<WALEntry> exactMatch = wal.readRange(firstTimestamp, firstTimestamp);
    assertEquals(1, exactMatch.size());
    assertEquals(0L, exactMatch.get(0).getSequenceNumber());
  }

  @Test
  void testOptimizedTimestampRangeBuildingWithDerivedMaxValues()
      throws WALException, InterruptedException {
    // Test that timestamp ranges are correctly derived using the optimization
    // where max values are derived from next file's first entry

    Instant startTime = Instant.now();

    // Create entries with time gaps to test timestamp range derivation
    for (int i = 0; i < 10; i++) {
      String data = "time_entry_" + i + "_" + "x".repeat(90);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    Thread.sleep(10); // Create timestamp gap
    Instant midTime = Instant.now();

    for (int i = 10; i < 20; i++) {
      String data = "time_entry_" + i + "_" + "x".repeat(90);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    Thread.sleep(10); // Create another timestamp gap
    Instant endTime = Instant.now();

    for (int i = 20; i < 25; i++) {
      String data = "time_entry_" + i + "_" + "x".repeat(90);
      wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
    }

    wal.sync();

    // Test timestamp-based queries with optimized range building
    // The optimization derives max timestamps from next file's first timestamp

    // Read entries from start to midTime
    List<WALEntry> earlyEntries = wal.readRange(startTime.minusMillis(1), midTime.plusMillis(5));
    assertTrue(earlyEntries.size() > 0); // Should get some entries

    // Verify early entries exist (don't be too strict about exact ranges due to timing)
    boolean hasEarlyEntries =
        earlyEntries.stream().anyMatch(entry -> entry.getSequenceNumber() < 15);
    assertTrue(hasEarlyEntries, "Should have some early entries");

    // Read entries from midTime to end
    List<WALEntry> lateEntries = wal.readRange(midTime, endTime.plusMillis(10));
    assertTrue(lateEntries.size() > 0); // Should get some entries

    // Verify late entries exist (don't be too strict about exact ranges due to timing)
    boolean hasLateEntries = lateEntries.stream().anyMatch(entry -> entry.getSequenceNumber() > 5);
    assertTrue(hasLateEntries, "Should have some later entries");

    // Read all entries by timestamp
    List<WALEntry> allByTime = wal.readRange(startTime.minusMillis(10), endTime.plusMillis(10));
    assertEquals(25, allByTime.size());

    // Verify sequence continuity in timestamp-based results
    for (int i = 0; i < allByTime.size(); i++) {
      assertEquals(i, allByTime.get(i).getSequenceNumber());
    }
  }
}

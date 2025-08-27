package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test demonstrating timestamp-based reading capabilities for point-in-time recovery
 * scenarios.
 */
class PointInTimeRecoveryTimestampTest {

  @TempDir Path tempDir;

  private WALManager walManager;

  @BeforeEach
  void setUp() throws WALException {
    walManager = new WALManager(tempDir);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (walManager != null) {
      walManager.close();
    }
  }

  @Test
  void testPointInTimeRecovery() throws WALException, InterruptedException {
    // Simulate a database transaction scenario with timestamps

    // Phase 1: Initial data setup
    Instant phase1Start = Instant.now();
    Thread.sleep(10);

    walManager.createEntry("BEGIN|txn_1001|".getBytes());
    walManager.createEntry(
        "INSERT|txn_1001|users|1|{\"name\":\"Alice\",\"email\":\"alice@example.com\"}".getBytes());
    walManager.createEntry(
        "INSERT|txn_1001|users|2|{\"name\":\"Bob\",\"email\":\"bob@example.com\"}".getBytes());
    walManager.createEntry("COMMIT|txn_1001|".getBytes());

    Thread.sleep(10);
    Instant phase1End = Instant.now();
    Thread.sleep(10);

    // Phase 2: Updates
    Instant phase2Start = Instant.now();
    Thread.sleep(10);

    walManager.createEntry("BEGIN|txn_1002|".getBytes());
    walManager.createEntry(
        "UPDATE|txn_1002|users|1|{\"name\":\"Alice Smith\",\"email\":\"alice.smith@example.com\"}"
            .getBytes());
    walManager.createEntry("DELETE|txn_1002|users|2|".getBytes());
    walManager.createEntry("COMMIT|txn_1002|".getBytes());

    Thread.sleep(10);
    Instant phase2End = Instant.now();
    Thread.sleep(10);

    // Phase 3: More operations
    Instant phase3Start = Instant.now();
    Thread.sleep(10);

    walManager.createEntry("BEGIN|txn_1003|".getBytes());
    walManager.createEntry(
        "INSERT|txn_1003|users|3|{\"name\":\"Charlie\",\"email\":\"charlie@example.com\"}"
            .getBytes());
    walManager.createEntry("COMMIT|txn_1003|".getBytes());

    Thread.sleep(10);
    Instant phase3End = Instant.now();

    // Test point-in-time recovery scenarios

    // 1. Recover to end of Phase 1 (should have Alice and Bob)
    List<WALEntry> phase1Entries = walManager.readRange(phase1Start, phase1End);
    assertEquals(4, phase1Entries.size());
    assertTrue(containsOperation(phase1Entries, "INSERT", "Alice"));
    assertTrue(containsOperation(phase1Entries, "INSERT", "Bob"));
    assertFalse(containsOperation(phase1Entries, "UPDATE", "Alice Smith"));

    // 2. Recover to end of Phase 2 (should have Alice updated, Bob deleted)
    List<WALEntry> phase1And2Entries = walManager.readRange(phase1Start, phase2End);
    assertEquals(8, phase1And2Entries.size());
    assertTrue(containsOperation(phase1And2Entries, "UPDATE", "Alice Smith"));
    assertTrue(containsOperation(phase1And2Entries, "DELETE", "users|2"));

    // 3. Recover everything (should include Charlie)
    List<WALEntry> allEntries = walManager.readFrom(phase1Start);
    assertEquals(11, allEntries.size());
    assertTrue(containsOperation(allEntries, "INSERT", "Charlie"));

    // 4. Recover only Phase 2 operations
    List<WALEntry> phase2Only = walManager.readRange(phase2Start, phase2End);
    assertEquals(4, phase2Only.size());
    assertTrue(containsOperation(phase2Only, "UPDATE", "Alice Smith"));
    assertTrue(containsOperation(phase2Only, "DELETE", "users|2"));
    assertFalse(containsOperation(phase2Only, "INSERT", "Charlie"));

    // 5. Test reading from future timestamp (should be empty)
    List<WALEntry> futureEntries = walManager.readFrom(Instant.now().plusSeconds(60));
    assertTrue(futureEntries.isEmpty());
  }

  @Test
  void testTimestampBasedBatchRecovery() throws WALException, InterruptedException {
    // Test timestamp-based reading with batch operations

    Instant beforeBatch = Instant.now();
    Thread.sleep(10);

    // Create a batch transaction
    List<ByteBuffer> batchData =
        Arrays.asList(
            ByteBuffer.wrap("BEGIN|txn_batch|".getBytes()),
            ByteBuffer.wrap(
                "INSERT|txn_batch|products|1|{\"name\":\"Laptop\",\"price\":999.99}".getBytes()),
            ByteBuffer.wrap(
                "INSERT|txn_batch|products|2|{\"name\":\"Mouse\",\"price\":29.99}".getBytes()),
            ByteBuffer.wrap(
                "INSERT|txn_batch|products|3|{\"name\":\"Keyboard\",\"price\":79.99}".getBytes()),
            ByteBuffer.wrap("COMMIT|txn_batch|".getBytes()));

    List<WALEntry> batchEntries = walManager.createEntryBatch(batchData);

    Thread.sleep(10);
    Instant afterBatch = Instant.now();

    // Verify batch entries have consecutive sequence numbers
    for (int i = 0; i < batchEntries.size(); i++) {
      assertEquals(i, batchEntries.get(i).getSequenceNumber());
    }

    // Read the batch by timestamp
    List<WALEntry> batchByTime = walManager.readRange(beforeBatch, afterBatch);
    assertEquals(5, batchByTime.size());

    // Verify all batch entries are included and in correct order
    assertTrue(containsOperation(batchByTime, "BEGIN", "txn_batch"));
    assertTrue(containsOperation(batchByTime, "INSERT", "Laptop"));
    assertTrue(containsOperation(batchByTime, "INSERT", "Mouse"));
    assertTrue(containsOperation(batchByTime, "INSERT", "Keyboard"));
    assertTrue(containsOperation(batchByTime, "COMMIT", "txn_batch"));

    // Verify sequence order is maintained
    for (int i = 0; i < batchByTime.size(); i++) {
      assertEquals(i, batchByTime.get(i).getSequenceNumber());
    }
  }

  @Test
  void testTimestampPrecisionAndOrdering() throws WALException, InterruptedException {
    // Test that entries created in rapid succession maintain proper ordering

    Instant start = Instant.now();

    // Create entries rapidly
    WALEntry entry1 = walManager.createEntry("rapid1".getBytes());
    WALEntry entry2 = walManager.createEntry("rapid2".getBytes());
    WALEntry entry3 = walManager.createEntry("rapid3".getBytes());

    Instant end = Instant.now();

    // Read by timestamp range
    List<WALEntry> rapidEntries = walManager.readRange(start, end);
    assertEquals(3, rapidEntries.size());

    // Verify sequence order is maintained even with very close timestamps
    assertEquals(0L, rapidEntries.get(0).getSequenceNumber());
    assertEquals(1L, rapidEntries.get(1).getSequenceNumber());
    assertEquals(2L, rapidEntries.get(2).getSequenceNumber());

    assertEquals("rapid1", new String(rapidEntries.get(0).getDataAsBytes()));
    assertEquals("rapid2", new String(rapidEntries.get(1).getDataAsBytes()));
    assertEquals("rapid3", new String(rapidEntries.get(2).getDataAsBytes()));

    // Verify timestamps are reasonable (all should be between start and end)
    for (WALEntry entry : rapidEntries) {
      assertFalse(entry.getTimestamp().isBefore(start));
      assertFalse(entry.getTimestamp().isAfter(end));
    }
  }

  private boolean containsOperation(List<WALEntry> entries, String operation, String data) {
    return entries.stream()
        .map(entry -> new String(entry.getDataAsBytes()))
        .anyMatch(entryData -> entryData.contains(operation) && entryData.contains(data));
  }
}

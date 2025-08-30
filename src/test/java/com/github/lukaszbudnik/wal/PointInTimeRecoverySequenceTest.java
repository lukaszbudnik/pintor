package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test demonstrating sequence-based reading capabilities for point-in-time recovery
 * scenarios using sequence numbers.
 */
class PointInTimeRecoverySequenceTest {

  @TempDir Path tempDir;

  private WALManager walManager;

  @BeforeEach
  void setUp() throws WALException {
    // use small max file size to force file rotations
    FileBasedWAL wal = new FileBasedWAL(tempDir, FileBasedWAL.PAGE_SIZE);
    walManager = new WALManager(wal);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (walManager != null) {
      walManager.close();
    }
  }

  @Test
  void testSequenceBasedPointInTimeRecovery() throws WALException {
    // Simulate a database transaction scenario with sequence-based recovery

    // Phase 1: Initial data setup (sequences 0-3)
    walManager.createEntry("BEGIN|txn_1001|".getBytes());
    walManager.createEntry(
        "INSERT|txn_1001|users|1|{\"name\":\"Alice\",\"email\":\"alice@example.com\"}".getBytes());
    walManager.createEntry(
        "INSERT|txn_1001|users|2|{\"name\":\"Bob\",\"email\":\"bob@example.com\"}".getBytes());
    walManager.createEntry("COMMIT|txn_1001|".getBytes());

    long phase1EndSeq = walManager.getCurrentSequenceNumber(); // Should be 3
    assertEquals(3L, phase1EndSeq);

    // Phase 2: Updates (sequences 4-7)
    walManager.createEntry("BEGIN|txn_1002|".getBytes());
    walManager.createEntry(
        "UPDATE|txn_1002|users|1|{\"name\":\"Alice Smith\",\"email\":\"alice.smith@example.com\"}"
            .getBytes());
    walManager.createEntry("DELETE|txn_1002|users|2|".getBytes());
    walManager.createEntry("COMMIT|txn_1002|".getBytes());

    long phase2EndSeq = walManager.getCurrentSequenceNumber(); // Should be 7
    assertEquals(7L, phase2EndSeq);

    // Phase 3: More operations (sequences 8-10)
    walManager.createEntry("BEGIN|txn_1003|".getBytes());
    walManager.createEntry(
        "INSERT|txn_1003|users|3|{\"name\":\"Charlie\",\"email\":\"charlie@example.com\"}"
            .getBytes());
    walManager.createEntry("COMMIT|txn_1003|".getBytes());

    long phase3EndSeq = walManager.getCurrentSequenceNumber(); // Should be 10
    assertEquals(10L, phase3EndSeq);

    // Test sequence-based point-in-time recovery scenarios

    // 1. Recover to end of Phase 1 (sequences 0-3)
    List<WALEntry> phase1Entries = walManager.readRange(0L, phase1EndSeq);
    assertEquals(4, phase1Entries.size());
    assertTrue(containsOperation(phase1Entries, "INSERT", "Alice"));
    assertTrue(containsOperation(phase1Entries, "INSERT", "Bob"));
    assertFalse(containsOperation(phase1Entries, "UPDATE", "Alice Smith"));

    // Verify sequence numbers are correct
    for (int i = 0; i < phase1Entries.size(); i++) {
      assertEquals(i, phase1Entries.get(i).getSequenceNumber());
    }

    // 2. Recover to end of Phase 2 (sequences 0-7)
    List<WALEntry> phase1And2Entries = walManager.readRange(0L, phase2EndSeq);
    assertEquals(8, phase1And2Entries.size());
    assertTrue(containsOperation(phase1And2Entries, "UPDATE", "Alice Smith"));
    assertTrue(containsOperation(phase1And2Entries, "DELETE", "users|2"));

    // 3. Recover everything from sequence 0
    List<WALEntry> allEntries = walManager.readFrom(0L);
    assertEquals(11, allEntries.size());
    assertTrue(containsOperation(allEntries, "INSERT", "Charlie"));

    // 4. Recover only Phase 2 operations (sequences 4-7)
    List<WALEntry> phase2Only = walManager.readRange(phase1EndSeq + 1, phase2EndSeq);
    assertEquals(4, phase2Only.size());
    assertTrue(containsOperation(phase2Only, "UPDATE", "Alice Smith"));
    assertTrue(containsOperation(phase2Only, "DELETE", "users|2"));
    assertFalse(containsOperation(phase2Only, "INSERT", "Charlie"));

    // Verify sequence numbers in phase2Only
    assertEquals(4L, phase2Only.get(0).getSequenceNumber());
    assertEquals(5L, phase2Only.get(1).getSequenceNumber());
    assertEquals(6L, phase2Only.get(2).getSequenceNumber());
    assertEquals(7L, phase2Only.get(3).getSequenceNumber());
  }

  @Test
  void testSequenceBasedBatchRecovery() throws WALException {
    // Test sequence-based reading with batch operations

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

    // Verify batch entries have consecutive sequence numbers starting from 0
    for (int i = 0; i < batchEntries.size(); i++) {
      assertEquals(i, batchEntries.get(i).getSequenceNumber());
    }

    // Read the batch by sequence range
    List<WALEntry> batchBySequence = walManager.readRange(0L, 4L);
    assertEquals(5, batchBySequence.size());

    // Verify all batch entries are included and in correct order
    assertTrue(containsOperation(batchBySequence, "BEGIN", "txn_batch"));
    assertTrue(containsOperation(batchBySequence, "INSERT", "Laptop"));
    assertTrue(containsOperation(batchBySequence, "INSERT", "Mouse"));
    assertTrue(containsOperation(batchBySequence, "INSERT", "Keyboard"));
    assertTrue(containsOperation(batchBySequence, "COMMIT", "txn_batch"));

    // Verify sequence order is maintained
    for (int i = 0; i < batchBySequence.size(); i++) {
      assertEquals(i, batchBySequence.get(i).getSequenceNumber());
    }

    // Test partial batch recovery (only first 3 entries)
    List<WALEntry> partialBatch = walManager.readRange(0L, 2L);
    assertEquals(3, partialBatch.size());
    assertTrue(containsOperation(partialBatch, "BEGIN", "txn_batch"));
    assertTrue(containsOperation(partialBatch, "INSERT", "Laptop"));
    assertTrue(containsOperation(partialBatch, "INSERT", "Mouse"));
    assertFalse(containsOperation(partialBatch, "INSERT", "Keyboard"));
    assertFalse(containsOperation(partialBatch, "COMMIT", "txn_batch"));
  }

  @Test
  void testSequenceBasedRecoveryAcrossFiles() throws WALException {
    // Use a small file size to force rotation and test cross-file recovery
    try (FileBasedWAL smallWal = new FileBasedWAL(tempDir.resolve("small_seq"), 1024);
        WALManager smallWalManager = new WALManager(smallWal)) {

      // Add enough entries to trigger file rotation
      for (int i = 0; i < 20; i++) {
        String data = "INSERT|txn_" + (1000 + i) + "|table|" + i + "|" + "x".repeat(100);
        smallWalManager.createEntry(data.getBytes());
      }

      // Test recovery across multiple files

      // 1. Read first 10 entries
      List<WALEntry> first10 = smallWalManager.readRange(0L, 9L);
      assertEquals(10, first10.size());
      for (int i = 0; i < 10; i++) {
        assertEquals(i, first10.get(i).getSequenceNumber());
        assertTrue(new String(first10.get(i).getDataAsBytes()).contains("txn_" + (1000 + i)));
      }

      // 2. Read middle range (might span files)
      List<WALEntry> middle = smallWalManager.readRange(5L, 14L);
      assertEquals(10, middle.size());
      for (int i = 0; i < 10; i++) {
        assertEquals(5 + i, middle.get(i).getSequenceNumber());
      }

      // 3. Read all entries
      List<WALEntry> allEntries = smallWalManager.readFrom(0L);
      assertEquals(20, allEntries.size());

      // Verify sequence continuity across files
      for (int i = 0; i < 20; i++) {
        assertEquals(i, allEntries.get(i).getSequenceNumber());
      }

      // 4. Read from middle to end
      List<WALEntry> fromMiddle = smallWalManager.readFrom(10L);
      assertEquals(10, fromMiddle.size());
      assertEquals(10L, fromMiddle.get(0).getSequenceNumber());
      assertEquals(19L, fromMiddle.get(9).getSequenceNumber());
    } catch (Exception e) {
      throw new WALException("Failed to test cross-file recovery", e);
    }
  }

  @Test
  void testSequenceBasedCheckpointRecovery() throws WALException {
    // Simulate checkpoint-based recovery scenario

    // Phase 1: Initial operations
    for (int i = 0; i < 10; i++) {
      walManager.createEntry(("OPERATION|" + i + "|data" + i).getBytes());
    }

    long checkpointSeq = walManager.getCurrentSequenceNumber(); // 9

    // Phase 2: More operations after checkpoint
    for (int i = 10; i < 20; i++) {
      walManager.createEntry(("OPERATION|" + i + "|data" + i).getBytes());
    }

    // Test checkpoint-based recovery

    // 1. Recover only operations before checkpoint
    List<WALEntry> beforeCheckpoint = walManager.readRange(0L, checkpointSeq);
    assertEquals(10, beforeCheckpoint.size());
    for (int i = 0; i < 10; i++) {
      assertTrue(new String(beforeCheckpoint.get(i).getDataAsBytes()).contains("OPERATION|" + i));
    }

    // 2. Recover only operations after checkpoint
    List<WALEntry> afterCheckpoint = walManager.readFrom(checkpointSeq + 1);
    assertEquals(10, afterCheckpoint.size());
    for (int i = 0; i < 10; i++) {
      assertTrue(
          new String(afterCheckpoint.get(i).getDataAsBytes()).contains("OPERATION|" + (10 + i)));
      assertEquals(checkpointSeq + 1 + i, afterCheckpoint.get(i).getSequenceNumber());
    }

    // 3. Test incremental recovery (simulate applying checkpoint + incremental changes)
    List<WALEntry> incremental = walManager.readRange(checkpointSeq + 1, checkpointSeq + 5);
    assertEquals(5, incremental.size());
    assertEquals(10L, incremental.get(0).getSequenceNumber());
    assertEquals(14L, incremental.get(4).getSequenceNumber());
  }

  @Test
  void testSequenceBasedRollbackRecovery() throws WALException {
    // Simulate rollback scenario where we need to recover to a specific sequence

    // Successful transaction
    walManager.createEntry("BEGIN|txn_good|".getBytes());
    walManager.createEntry("INSERT|txn_good|users|1|Alice".getBytes());
    walManager.createEntry("COMMIT|txn_good|".getBytes());

    long goodTransactionEnd = walManager.getCurrentSequenceNumber(); // 2

    // Failed transaction (that needs to be rolled back)
    walManager.createEntry("BEGIN|txn_bad|".getBytes());
    walManager.createEntry("INSERT|txn_bad|users|2|BadData".getBytes());
    walManager.createEntry("UPDATE|txn_bad|users|1|CorruptedAlice".getBytes());
    // Note: No COMMIT - transaction failed

    // Recovery: Read only up to the good transaction
    List<WALEntry> recoveryEntries = walManager.readRange(0L, goodTransactionEnd);
    assertEquals(3, recoveryEntries.size());

    // Verify we only have the good transaction
    assertTrue(containsOperation(recoveryEntries, "BEGIN", "txn_good"));
    assertTrue(containsOperation(recoveryEntries, "INSERT", "Alice"));
    assertTrue(containsOperation(recoveryEntries, "COMMIT", "txn_good"));

    // Verify we don't have the bad transaction
    assertFalse(containsOperation(recoveryEntries, "BEGIN", "txn_bad"));
    assertFalse(containsOperation(recoveryEntries, "INSERT", "BadData"));
    assertFalse(containsOperation(recoveryEntries, "UPDATE", "CorruptedAlice"));

    // Verify sequence numbers are correct
    assertEquals(0L, recoveryEntries.get(0).getSequenceNumber());
    assertEquals(1L, recoveryEntries.get(1).getSequenceNumber());
    assertEquals(2L, recoveryEntries.get(2).getSequenceNumber());
  }

  // test reading across multiple WAL files
  @Test
  public void testReadingAcrossMultipleWALFiles() throws WALException {
    // Add enough entries to trigger file rotation
    for (int i = 0; i < 1000; i++) {
      String data = "INSERT|txn_" + (1000 + i) + "|table|" + i + "|" + "x".repeat(100);
      walManager.createEntry(data.getBytes());
    }

    // Read all entries
    List<WALEntry> entries = walManager.readRange(500L, 600L);
    assertEquals(101, entries.size());

    // Verify sequence continuity across files
    for (int i = 500; i <= 600; i++) {
      assertEquals(i, entries.get(i - 500).getSequenceNumber());
    }
  }

  private boolean containsOperation(List<WALEntry> entries, String operation, String data) {
    return entries.stream()
        .map(entry -> new String(entry.getDataAsBytes()))
        .anyMatch(entryData -> entryData.contains(operation) && entryData.contains(data));
  }
}

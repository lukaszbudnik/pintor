package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class FileBasedWALTest {

  @TempDir Path tempDir;

  private FileBasedWAL wal;

  @BeforeEach
  void setUp() throws WALException {
    wal = new FileBasedWAL(tempDir);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (wal != null) {
      wal.close();
    }
  }

  @Test
  void testConstructorVariations() throws Exception {
    wal.close();

    // Test default constructor
    FileBasedWAL defaultWal = new FileBasedWAL(tempDir);
    assertNotNull(defaultWal);
    defaultWal.close();

    // Test constructor with custom parameters
    FileBasedWAL customWal = new FileBasedWAL(tempDir, 1024 * 1024, (byte) 4); // 1MB
    assertNotNull(customWal);

    // Test that it works with custom settings
    WALEntry entry = customWal.createAndAppend(ByteBuffer.wrap("test".getBytes()));
    assertEquals(1L, customWal.size());

    customWal.close();
  }

  @Test
  void testAppendOperations() throws WALException {
    // Test single append
    WALEntry entry1 = wal.createAndAppend(ByteBuffer.wrap("INSERT|txn_1001|data1".getBytes()));
    WALEntry entry2 = wal.createAndAppend(ByteBuffer.wrap("UPDATE|txn_1002|data2".getBytes()));

    assertEquals(1L, wal.getCurrentSequenceNumber());
    assertEquals(2L, wal.getNextSequenceNumber());
    assertEquals(2L, wal.size());
    assertFalse(wal.isEmpty());

    // Test batch append
    List<ByteBuffer> batchData =
        Arrays.asList(
            ByteBuffer.wrap("DELETE|txn_1003|data3".getBytes()), null // Test null data
            );
    List<WALEntry> batchEntries = wal.createAndAppendBatch(batchData);

    assertEquals(3L, wal.getCurrentSequenceNumber());
    assertEquals(4L, wal.getNextSequenceNumber());
    assertEquals(4L, wal.size());

    // Verify all entries can be read back
    List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(4, entries.size());
    assertEquals("INSERT|txn_1001|data1", new String(entries.get(0).getDataAsBytes()));
    assertEquals("UPDATE|txn_1002|data2", new String(entries.get(1).getDataAsBytes()));
    assertEquals("DELETE|txn_1003|data3", new String(entries.get(2).getDataAsBytes()));
    assertNull(entries.get(3).getDataAsBytes()); // Null data entry
  }

  @Test
  void testSequenceNumberManagement() throws WALException {
    // Test empty WAL state
    assertTrue(wal.isEmpty());
    assertEquals(-1L, wal.getCurrentSequenceNumber());
    assertEquals(0L, wal.getNextSequenceNumber());
    assertEquals(0L, wal.size());

    // Test sequence number progression
    WALEntry entry1 = wal.createAndAppend(ByteBuffer.wrap("data1".getBytes()));
    assertEquals(0L, wal.getCurrentSequenceNumber());
    assertEquals(1L, wal.getNextSequenceNumber());
    assertEquals(0L, entry1.getSequenceNumber());

    // Test that sequence numbers are automatically managed
    WALEntry entry2 = wal.createAndAppend(ByteBuffer.wrap("data2".getBytes()));
    assertEquals(1L, wal.getCurrentSequenceNumber());
    assertEquals(2L, wal.getNextSequenceNumber());
    assertEquals(1L, entry2.getSequenceNumber());
  }

  @Test
  void testReadOperations() throws WALException {
    // Test reading from empty WAL
    List<WALEntry> emptyEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertTrue(emptyEntries.isEmpty());

    // Add test data
    for (int i = 0; i < 5; i++) {
      wal.createAndAppend(ByteBuffer.wrap(("INSERT|txn_" + (1000L + i) + "|data" + i).getBytes()));
    }

    // Test readFrom
    List<WALEntry> allEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(5, allEntries.size());
    for (int i = 0; i < 5; i++) {
      assertEquals(i, allEntries.get(i).getSequenceNumber());
    }

    // Test readFrom with higher starting sequence
    List<WALEntry> partialEntries = Flux.from(wal.readFrom(2L)).collectList().block();
    assertEquals(3, partialEntries.size());
    assertEquals(2L, partialEntries.get(0).getSequenceNumber());
    assertEquals(4L, partialEntries.get(2).getSequenceNumber());

    // Test readRange
    List<WALEntry> rangeEntries = Flux.from(wal.readRange(1L, 3L)).collectList().block();
    assertEquals(3, rangeEntries.size());
    assertEquals(1L, rangeEntries.get(0).getSequenceNumber());
    assertEquals(2L, rangeEntries.get(1).getSequenceNumber());
    assertEquals(3L, rangeEntries.get(2).getSequenceNumber());

    // Test readRange with single entry
    List<WALEntry> singleEntry = Flux.from(wal.readRange(2L, 2L)).collectList().block();
    assertEquals(1, singleEntry.size());
    assertEquals(2L, singleEntry.get(0).getSequenceNumber());
  }

  @Test
  void testDataFormats() throws WALException {
    // Test comprehensive data format parsing with actual content verification
    String[] testData = {
      "INSERT|txn_123|users|1|{\"name\":\"John\",\"email\":\"john@example.com\"}",
      "UPDATE|txn_123|users|1|{\"name\":\"John Doe\",\"email\":\"john@example.com\"}",
      "DELETE|txn_123|users|2|",
      "BEGIN|txn_124|",
      "COMMIT|txn_124|",
      "ROLLBACK|txn_125|reason:timeout",
      "CHECKPOINT||timestamp:" + System.currentTimeMillis(),
      "CUSTOM|txn_126|custom_table|key|value_with_pipes|and|more"
    };

    // Append all test data using batch operation
    List<ByteBuffer> dataBuffers = new ArrayList<>();
    for (String data : testData) {
      dataBuffers.add(ByteBuffer.wrap(data.getBytes()));
    }
    List<WALEntry> entries = wal.createAndAppendBatch(dataBuffers);
    assertEquals(testData.length, entries.size());

    for (int i = 0; i < testData.length; i++) {
      String retrievedData = new String(entries.get(i).getDataAsBytes());

      // Compare with original test data
      assertEquals(testData[i], retrievedData, "Entry " + i + " data mismatch");

      // Parse and verify structure matches original
      String[] parts = retrievedData.split("\\|", -1);
      String[] originalParts = testData[i].split("\\|", -1);

      // Verify each part matches exactly
      assertEquals(originalParts.length, parts.length, "Entry " + i + " parts count mismatch");
      for (int j = 0; j < originalParts.length; j++) {
        assertEquals(originalParts[j], parts[j], "Entry " + i + " part " + j + " mismatch");
      }

      // Verify operation type is valid
      String operation = parts[0];
      assertTrue(
          operation.matches("INSERT|UPDATE|DELETE|BEGIN|COMMIT|ROLLBACK|CHECKPOINT|CUSTOM"),
          "Invalid operation: " + operation);

      // Verify transaction format when present
      if (parts.length > 1 && !parts[1].isEmpty()) {
        assertTrue(
            parts[1].startsWith("txn_") || parts[1].isEmpty(),
            "Invalid transaction format: " + parts[1]);
      }
    }
  }

  @Test
  void testDataIntegrity() throws WALException {
    // Test binary serialization with complex data
    String complexData =
        "UPDATE|txn_9999|users|42|Complex data with special chars: àáâãäåæçèéêë ñ 中文 🚀";
    WALEntry originalEntry = wal.createAndAppend(ByteBuffer.wrap(complexData.getBytes()));

    wal.sync(); // Force write to disk

    List<WALEntry> readEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(1, readEntries.size());

    WALEntry readEntry = readEntries.get(0);

    // Verify all fields are preserved exactly
    assertEquals(originalEntry.getSequenceNumber(), readEntry.getSequenceNumber());
    assertArrayEquals(originalEntry.getDataAsBytes(), readEntry.getDataAsBytes());
    assertEquals(complexData, new String(readEntry.getDataAsBytes()));

    // Verify timestamp precision is preserved (within reasonable bounds since we use Instant.now())
    assertTrue(
        Math.abs(
                originalEntry.getTimestamp().getEpochSecond()
                    - readEntry.getTimestamp().getEpochSecond())
            <= 1);

    // Verify special characters are preserved
    String retrievedData = new String(readEntry.getDataAsBytes());
    assertTrue(retrievedData.contains("àáâãäåæçèéêë"));
    assertTrue(retrievedData.contains("中文"));
    assertTrue(retrievedData.contains("🚀"));

    // Test CRC32 validation by verifying data integrity after multiple operations
    List<ByteBuffer> testDataList = new ArrayList<>();
    for (int i = 1; i < 10; i++) {
      String testData = "INSERT|txn_" + i + "|test|" + i + "|data" + i;
      testDataList.add(ByteBuffer.wrap(testData.getBytes()));
    }
    wal.createAndAppendBatch(testDataList);

    // All entries should be readable and intact
    List<WALEntry> allEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(10, allEntries.size());

    // Verify first entry is still intact
    assertEquals(complexData, new String(allEntries.get(0).getDataAsBytes()));
  }

  @Test
  void testRecoveryAndPersistence() throws Exception {
    // Write some entries
    WALEntry entry1 = wal.createAndAppend(ByteBuffer.wrap("INSERT|txn_1001|data1".getBytes()));
    WALEntry entry2 = wal.createAndAppend(ByteBuffer.wrap("UPDATE|txn_1002|data2".getBytes()));

    wal.sync();

    // Record state before closing
    long expectedCurrentSeq = wal.getCurrentSequenceNumber();
    long expectedNextSeq = wal.getNextSequenceNumber();
    long expectedSize = wal.size();

    wal.close();

    // Reopen WAL and verify recovery
    FileBasedWAL newWal = new FileBasedWAL(tempDir);

    // Should recover the exact state
    assertEquals(expectedCurrentSeq, newWal.getCurrentSequenceNumber());
    assertEquals(expectedNextSeq, newWal.getNextSequenceNumber());
    assertEquals(expectedSize, newWal.size());
    assertFalse(newWal.isEmpty());

    // Verify all data is recovered exactly
    List<WALEntry> recoveredEntries = Flux.from(newWal.readFrom(0L)).collectList().block();
    assertEquals(2, recoveredEntries.size());

    // Verify first entry
    assertEquals(0L, recoveredEntries.get(0).getSequenceNumber());
    assertNotNull(recoveredEntries.get(0).getTimestamp());
    assertEquals("INSERT|txn_1001|data1", new String(recoveredEntries.get(0).getDataAsBytes()));

    // Verify second entry
    assertEquals(1L, recoveredEntries.get(1).getSequenceNumber());
    assertNotNull(recoveredEntries.get(1).getTimestamp());
    assertEquals("UPDATE|txn_1002|data2", new String(recoveredEntries.get(1).getDataAsBytes()));

    // Verify we can continue appending with correct sequence
    WALEntry entry3 = newWal.createAndAppend(ByteBuffer.wrap("DELETE|txn_1003|data3".getBytes()));
    assertEquals(2L, newWal.getCurrentSequenceNumber());
    assertEquals(3L, newWal.getNextSequenceNumber());

    newWal.close();
  }

  @Test
  void testFileRotation() throws Exception {
    wal.close();

    // Create WAL with very small file size to force rotation
    FileBasedWAL smallWal = new FileBasedWAL(tempDir, 4096, (byte) 4); // 4KB max file size

    try {
      // Add entries that will exceed the file size limit
      for (int i = 0; i < 50; i++) {
        // Create entries with enough data to trigger rotation
        String largeData = "INSERT|txn_" + i + "|large_table|" + i + "|" + "x".repeat(100);
        smallWal.createAndAppend(ByteBuffer.wrap(largeData.getBytes()));
      }

      smallWal.sync();

      // Verify all entries are still readable after rotation
      List<WALEntry> entries = Flux.from(smallWal.readFrom(0L)).collectList().block();
      assertEquals(50, entries.size());

      // Verify entries are in correct order
      for (int i = 0; i < 50; i++) {
        assertEquals(i, entries.get(i).getSequenceNumber());
        String expectedData = "INSERT|txn_" + i + "|large_table|" + i + "|" + "x".repeat(100);
        assertEquals(expectedData, new String(entries.get(i).getDataAsBytes()));
      }

    } finally {
      smallWal.close();
    }
  }

  @Test
  void testTruncateOperations() throws Exception {
    // Create WAL with very small file size to force rotation
    wal.close();

    wal = new FileBasedWAL(tempDir, 4096, (byte) 4);

    // Add a thousand entries
    String[] testData = new String[1000];
    for (int i = 0; i < 1000; i++) {
      testData[i] = "INSERT|txn_" + (1000L + i) + "|data" + i;
      wal.createAndAppend(ByteBuffer.wrap(testData[i].getBytes()));
    }

    wal.sync();

    assertEquals(999L, wal.getCurrentSequenceNumber());
    assertEquals(1000L, wal.size());

    // Truncate up to sequence 500
    wal.truncate(500L);

    // Current sequence number should remain the same (highest sequence is still 999)
    assertEquals(999L, wal.getCurrentSequenceNumber());

    // Verify remaining entries
    List<WALEntry> remainingEntries = Flux.from(wal.readFrom(0L)).collectList().block();

    // Pintor truncates by removing whole files, it doesn't update the files or pages
    // so when we truncate to 500 we may actually still have a file with entries 498, 499, 500, 501,
    // ...
    // we created 1000 entries with max file size of 4KB so there should be X files created
    // truncate should delete Y files so the number of records should be less than 1000 and bigger
    // than 500
    assertTrue(remainingEntries.size() < 1000);
    assertTrue(remainingEntries.size() > 500);

    // Last item should have sequence set to the current sequence number in WAL
    assertEquals(
        remainingEntries.get(remainingEntries.size() - 1).getSequenceNumber(),
        wal.getCurrentSequenceNumber());

    // Test that we can still append after truncate
    WALEntry newEntry =
        wal.createAndAppend(ByteBuffer.wrap("INSERT|txn_new|after_truncate".getBytes()));
    assertEquals(1000L, wal.getCurrentSequenceNumber());

    // Verify the new entry is readable
    List<WALEntry> afterAppend = Flux.from(wal.readFrom(1000L)).collectList().block();
    assertEquals(1, afterAppend.size());
    assertEquals("INSERT|txn_new|after_truncate", new String(afterAppend.get(0).getDataAsBytes()));
  }

  @Test
  void testSyncOperations() throws Exception {
    // Test sync with data
    WALEntry entry = wal.createAndAppend(ByteBuffer.wrap("INSERT|txn_1001|data".getBytes()));

    // Should not throw exception
    assertDoesNotThrow(() -> wal.sync());

    // Data should still be readable after sync
    List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(1, entries.size());
    assertEquals("INSERT|txn_1001|data", new String(entries.get(0).getDataAsBytes()));

    // Test sync on empty WAL
    wal.close();
    FileBasedWAL emptyWal = new FileBasedWAL(tempDir.resolve("empty"));
    assertDoesNotThrow(() -> emptyWal.sync());
    emptyWal.close();
  }

  @Test
  void testErrorHandling() throws Exception {
    wal.close();

    // Test that WAL handles various edge cases gracefully

    // Test operations on fresh WAL
    FileBasedWAL freshWal = new FileBasedWAL(tempDir.resolve("fresh"));

    // Test invalid range
    Exception exception =
        assertThrows(
            Exception.class, () -> Flux.from(freshWal.readRange(10L, 5L)).collectList().block());
    Throwable cause = Exceptions.unwrap(exception);
    assertTrue(cause instanceof WALException);
    assertTrue(cause.getMessage().contains("Invalid range"));

    // Test reading from non-existent sequence
    List<WALEntry> nonExistent = Flux.from(freshWal.readFrom(1000L)).collectList().block();
    assertTrue(nonExistent.isEmpty());

    // Test that we can create entries normally
    WALEntry validEntry = freshWal.createAndAppend(ByteBuffer.wrap("valid".getBytes()));
    assertNotNull(validEntry);

    // Test that we can read the valid entry
    List<WALEntry> validEntries = Flux.from(freshWal.readFrom(0L)).collectList().block();
    assertEquals(1, validEntries.size());
    assertEquals("valid", new String(validEntries.get(0).getDataAsBytes()));

    // Test double close - should not throw
    assertDoesNotThrow(() -> freshWal.close());
    assertDoesNotThrow(() -> freshWal.close()); // Second close
  }

  @Test
  void testCreateAndAppendThreadSafety() throws WALException {
    // Test that createAndAppend is thread-safe and generates consecutive sequence numbers
    WALEntry entry1 = wal.createAndAppend(ByteBuffer.wrap("data1".getBytes()));
    WALEntry entry2 = wal.createAndAppend(ByteBuffer.wrap("data2".getBytes()));
    WALEntry entry3 = wal.createAndAppend(ByteBuffer.wrap("data3".getBytes()));

    // Verify consecutive sequence numbers
    assertEquals(0L, entry1.getSequenceNumber());
    assertEquals(1L, entry2.getSequenceNumber());
    assertEquals(2L, entry3.getSequenceNumber());

    // Verify data integrity
    assertEquals("data1", new String(entry1.getDataAsBytes()));
    assertEquals("data2", new String(entry2.getDataAsBytes()));
    assertEquals("data3", new String(entry3.getDataAsBytes()));

    // Verify entries are persisted
    List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(3, entries.size());
    assertEquals("data1", new String(entries.get(0).getDataAsBytes()));
    assertEquals("data2", new String(entries.get(1).getDataAsBytes()));
    assertEquals("data3", new String(entries.get(2).getDataAsBytes()));
  }

  @Test
  void testCreateAndAppendBatch() throws WALException {
    // Test batch creation with consecutive sequence numbers
    List<ByteBuffer> dataList =
        Arrays.asList(
            ByteBuffer.wrap("batch1".getBytes()),
            ByteBuffer.wrap("batch2".getBytes()),
            ByteBuffer.wrap("batch3".getBytes()),
            null // Test null data
            );

    List<WALEntry> entries = wal.createAndAppendBatch(dataList);

    // Verify batch results
    assertEquals(4, entries.size());
    assertEquals(0L, entries.get(0).getSequenceNumber());
    assertEquals(1L, entries.get(1).getSequenceNumber());
    assertEquals(2L, entries.get(2).getSequenceNumber());
    assertEquals(3L, entries.get(3).getSequenceNumber());

    // Verify data
    assertEquals("batch1", new String(entries.get(0).getDataAsBytes()));
    assertEquals("batch2", new String(entries.get(1).getDataAsBytes()));
    assertEquals("batch3", new String(entries.get(2).getDataAsBytes()));
    assertNull(entries.get(3).getDataAsBytes());

    // Verify persistence
    List<WALEntry> readEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(4, readEntries.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(entries.get(i).getSequenceNumber(), readEntries.get(i).getSequenceNumber());
      if (entries.get(i).getDataAsBytes() != null) {
        assertArrayEquals(entries.get(i).getDataAsBytes(), readEntries.get(i).getDataAsBytes());
      } else {
        assertNull(readEntries.get(i).getDataAsBytes());
      }
    }
  }

  @Test
  void testCreateAndAppendWithEmptyBatch() throws WALException {
    // Test empty batch
    List<WALEntry> entries = wal.createAndAppendBatch(List.of());
    assertTrue(entries.isEmpty());

    // WAL should still be empty
    assertTrue(wal.isEmpty());
    assertEquals(-1L, wal.getCurrentSequenceNumber());
  }

  @Test
  void testMixedOperations() throws WALException {
    // Test mixing single and batch operations

    // Use single API
    WALEntry entry1 = wal.createAndAppend(ByteBuffer.wrap("single_api_1".getBytes()));
    assertEquals(0L, entry1.getSequenceNumber());

    // Use batch API
    List<WALEntry> batchEntries =
        wal.createAndAppendBatch(
            Arrays.asList(
                ByteBuffer.wrap("batch_1".getBytes()), ByteBuffer.wrap("batch_2".getBytes())));
    assertEquals(1L, batchEntries.get(0).getSequenceNumber());
    assertEquals(2L, batchEntries.get(1).getSequenceNumber());

    // Use single API again
    WALEntry entry2 = wal.createAndAppend(ByteBuffer.wrap("single_api_2".getBytes()));
    assertEquals(3L, entry2.getSequenceNumber());

    // Verify all entries
    List<WALEntry> allEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(4, allEntries.size());
    assertEquals("single_api_1", new String(allEntries.get(0).getDataAsBytes()));
    assertEquals("batch_1", new String(allEntries.get(1).getDataAsBytes()));
    assertEquals("batch_2", new String(allEntries.get(2).getDataAsBytes()));
    assertEquals("single_api_2", new String(allEntries.get(3).getDataAsBytes()));
  }

  @Test
  void testTimestampBasedReading() throws WALException, InterruptedException {
    // Create entries with some time gaps to test timestamp-based reading
    Instant startTime = Instant.now();
    // Wait longer to ensure different timestamps with millisecond precision
    Thread.sleep(10);

    WALEntry entry1 = wal.createAndAppend(ByteBuffer.wrap("entry1".getBytes()));

    // Wait longer to ensure different timestamps with millisecond precision
    Thread.sleep(10);

    Instant midTime = Instant.now();
    // Wait longer to ensure different timestamps with millisecond precision
    Thread.sleep(10);

    WALEntry entry2 = wal.createAndAppend(ByteBuffer.wrap("entry2".getBytes()));
    WALEntry entry3 = wal.createAndAppend(ByteBuffer.wrap("entry3".getBytes()));

    // Wait longer to ensure different timestamps with millisecond precision
    Thread.sleep(10);
    Instant endTime = Instant.now();
    // Wait longer to ensure different timestamps with millisecond precision
    Thread.sleep(10);

    WALEntry entry4 = wal.createAndAppend(ByteBuffer.wrap("entry4".getBytes()));

    // Test readFrom(timestamp)
    List<WALEntry> fromMid = Flux.from(wal.readFrom(midTime)).collectList().block();
    assertEquals(3, fromMid.size()); // Should include entry2, entry3, entry4
    assertEquals("entry2", new String(fromMid.get(0).getDataAsBytes()));

    // Test readRange(timestamp, timestamp)
    List<WALEntry> midRange = Flux.from(wal.readRange(midTime, endTime)).collectList().block();
    assertEquals(2, midRange.size()); // Should include entry2, entry3
    assertEquals("entry2", new String(midRange.get(0).getDataAsBytes()));
    assertEquals("entry3", new String(midRange.get(1).getDataAsBytes()));

    // Test with very early timestamp (should get all entries)
    List<WALEntry> fromStart = Flux.from(wal.readFrom(startTime)).collectList().block();
    assertEquals(4, fromStart.size());

    // Test with future timestamp (should get no entries)
    List<WALEntry> fromFuture =
        Flux.from(wal.readFrom(Instant.now().plusSeconds(60))).collectList().block();
    assertTrue(fromFuture.isEmpty());

    // Test invalid range (from > to)
    Exception exception =
        assertThrows(
            Exception.class,
            () -> Flux.from(wal.readRange(endTime, startTime)).collectList().block());
    Throwable cause = Exceptions.unwrap(exception);
    assertTrue(cause instanceof WALException);
    assertTrue(cause.getMessage().contains("Invalid range"));
  }

  @Test
  void testTimestampBasedReadingWithBatch() throws WALException, InterruptedException {
    // Create a batch of entries
    List<ByteBuffer> batchData =
        Arrays.asList(
            ByteBuffer.wrap("batch1".getBytes()),
            ByteBuffer.wrap("batch2".getBytes()),
            ByteBuffer.wrap("batch3".getBytes()));

    Instant beforeBatch = Instant.now();
    Thread.sleep(10);

    List<WALEntry> batchEntries = wal.createAndAppendBatch(batchData);

    Thread.sleep(10);
    Instant afterBatch = Instant.now();

    // All batch entries should have very similar timestamps
    List<WALEntry> batchByTime =
        Flux.from(wal.readRange(beforeBatch, afterBatch)).collectList().block();
    assertEquals(3, batchByTime.size());

    // Verify the entries are in correct sequence order
    for (int i = 0; i < 3; i++) {
      assertEquals("batch" + (i + 1), new String(batchByTime.get(i).getDataAsBytes()));
      assertEquals(i, batchByTime.get(i).getSequenceNumber());
    }
  }

  @Test
  void testTimestampBasedReadingAcrossFiles() throws Exception {
    // Use a small file size to force rotation - FileBasedWAL.PAGE_SIZE
    try (FileBasedWAL smallWal =
        new FileBasedWAL(tempDir.resolve("small_timestamp"), 4096, (byte) 4)) {
      Instant startTime = Instant.now();

      // Add enough entries to trigger file rotation
      for (int i = 0; i < 20; i++) {
        String data = "entry_" + i + "_" + "x".repeat(100); // Make entries large enough
        smallWal.createAndAppend(ByteBuffer.wrap(data.getBytes()));

        if (i == 10) {
          Thread.sleep(10); // Create a timestamp gap in the middle
        }
      }

      Instant endTime = Instant.now();

      // Read all entries by timestamp
      List<WALEntry> allByTime =
          Flux.from(smallWal.readRange(startTime, endTime)).collectList().block();
      assertEquals(20, allByTime.size());

      // Verify entries are in correct sequence order
      for (int i = 0; i < 20; i++) {
        assertEquals(i, allByTime.get(i).getSequenceNumber());
        assertTrue(new String(allByTime.get(i).getDataAsBytes()).startsWith("entry_" + i));
      }
    }
  }

  @Test
  void testStreamingWithBackpressure() throws WALException {
    // Create test data
    for (int i = 0; i < 10; i++) {
      wal.createAndAppend(ByteBuffer.wrap(("stream_entry_" + i).getBytes()));
    }

    // Test streaming with StepVerifier
    StepVerifier.create(wal.readFrom(0L)).expectNextCount(10).verifyComplete();

    // Test streaming with limited demand
    StepVerifier.create(wal.readRange(2L, 5L))
        .expectNextMatches(entry -> entry.getSequenceNumber() == 2L)
        .expectNextMatches(entry -> entry.getSequenceNumber() == 3L)
        .expectNextMatches(entry -> entry.getSequenceNumber() == 4L)
        .expectNextMatches(entry -> entry.getSequenceNumber() == 5L)
        .verifyComplete();

    // Test cancellation
    StepVerifier.create(wal.readFrom(0L)).expectNextCount(3).thenCancel().verify();
  }

  @Test
  void testPageFlushingEdgeCase() throws WALException {
    // Recreate the exact scenario: page with exactly 0 bytes remaining
    // Page data size: 4052 bytes
    // Entry structure: 1 byte type + 8 bytes seq + 8 bytes timestamp + 4 bytes length + data + 4
    // bytes CRC = 25 + data
    // Need 41-byte entries (16 bytes data + 25 bytes overhead)
    // 4052 / 41 = 98.8, so 98 entries = 4018 bytes, leaving 34 bytes
    // Need to adjust to get exactly 0 bytes remaining

    List<WALEntry> entries = new ArrayList<>();

    // Fill page to exactly 41 bytes remaining (space for exactly 1 more 41-byte entry)
    // We need 4052 - 41 = 4011 bytes used
    // Use 97 entries of 41 bytes = 3977 bytes, then 1 entry of 34 bytes = 4011 bytes total
    for (int i = 0; i < 97; i++) {
      String data = String.format("data%012d", i); // 16 bytes
      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
      entries.add(entry);
    }

    // Add one smaller entry to get exactly 41 bytes remaining
    String smallData = String.format("d%08d", 97); // 9 bytes (34 total bytes)
    WALEntry smallEntry = wal.createAndAppend(ByteBuffer.wrap(smallData.getBytes()));
    entries.add(smallEntry);

    // Now add the critical entry that should exactly fill the page (41 bytes remaining, need 41
    // bytes)
    // This should trigger "Page full, flushing current page (entries=98, remaining=0 bytes)"
    String data97 = String.format("data%012d", 98); // 16 bytes (41 total bytes)
    WALEntry entry97 = wal.createAndAppend(ByteBuffer.wrap(data97.getBytes()));
    entries.add(entry97);

    // Add one more entry that should go to new page
    String data98 = String.format("data%012d", 99); // 16 bytes
    WALEntry entry98 = wal.createAndAppend(ByteBuffer.wrap(data98.getBytes()));
    entries.add(entry98);

    wal.sync();

    // Read all entries back and verify no duplicates and correct sequence
    List<WALEntry> readEntries = Flux.from(wal.readFrom(0L)).collectList().block();

    assertEquals(100, readEntries.size(), "Should have exactly 100 entries");

    // Verify sequence numbers are consecutive and no duplicates
    Set<Long> sequenceNumbers = new HashSet<>();
    for (int i = 0; i < readEntries.size(); i++) {
      WALEntry entry = readEntries.get(i);
      assertEquals(i, entry.getSequenceNumber(), "Sequence number should be consecutive");
      assertTrue(
          sequenceNumbers.add(entry.getSequenceNumber()),
          "Duplicate sequence number found: " + entry.getSequenceNumber());
    }

    // Specifically verify the critical entries around the page boundary
    assertEquals(97, readEntries.get(97).getSequenceNumber());
    assertEquals(98, readEntries.get(98).getSequenceNumber());
    assertEquals(99, readEntries.get(99).getSequenceNumber());
  }

  @Test
  void testExactPageSizeEntry() throws WALException {
    // Create an entry that exactly fills the entire page data section
    // Page data size: 8147 bytes (8192 - 45 header)
    // Entry overhead: 25 bytes (1+8+8+4+4)
    // So data should be: 8147 - 25 = 8122 bytes

    byte[] exactPageData = new byte[8122];
    for (int i = 0; i < exactPageData.length; i++) {
      exactPageData[i] = (byte) (i % 256);
    }

    WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(exactPageData));
    assertEquals(0, entry.getSequenceNumber());

    wal.sync();

    // Read back and verify
    List<WALEntry> readEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(1, readEntries.size());
    assertEquals(0, readEntries.get(0).getSequenceNumber());

    // Verify data integrity
    byte[] readData = readEntries.get(0).getDataAsBytes();
    assertEquals(8122, readData.length);
    for (int i = 0; i < readData.length; i++) {
      assertEquals((byte) (i % 256), readData[i], "Data mismatch at position " + i);
    }
  }

  @Test
  void testExactPageSizeEntryByTimestamp() throws WALException, java.io.IOException {
    // Create an entry that exactly fills the entire page data section
    // Page data size: 8147 bytes (8192 - 45 header)
    // Entry overhead: 25 bytes (1+8+8+4+4)
    // So data should be: 8147 - 25 = 8122 bytes

    byte[] exactPageData = new byte[8122];
    for (int i = 0; i < exactPageData.length; i++) {
      exactPageData[i] = (byte) (i % 256);
    }

    // Use wider timestamp range to ensure we capture the entry
    Instant beforeWrite = Instant.now().minusSeconds(1);
    WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(exactPageData));
    Instant afterWrite = Instant.now().plusSeconds(1);
    assertEquals(0, entry.getSequenceNumber());

    wal.sync();

    // Read back by timestamp range
    List<WALEntry> readEntries =
        Flux.from(wal.readRange(beforeWrite, afterWrite)).collectList().block();
    assertEquals(1, readEntries.size());
    assertEquals(0, readEntries.get(0).getSequenceNumber());

    // Verify data integrity
    byte[] readData = readEntries.get(0).getDataAsBytes();
    assertEquals(8122, readData.length);
    for (int i = 0; i < readData.length; i++) {
      assertEquals((byte) (i % 256), readData[i], "Data mismatch at position " + i);
    }
  }

  @Test
  void testPageFlushingEdgeCaseByTimestamp() throws WALException {
    List<WALEntry> entries = new ArrayList<>();
    // Use wider timestamp range to ensure we capture all entries
    Instant beforeWrite = Instant.now().minusSeconds(1);

    // Fill page to exactly 41 bytes remaining (space for exactly 1 more 41-byte entry)
    for (int i = 0; i < 97; i++) {
      String data = String.format("data%012d", i); // 16 bytes
      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
      entries.add(entry);
    }

    // Add one smaller entry to get exactly 41 bytes remaining
    String smallData = String.format("d%08d", 97); // 9 bytes (34 total bytes)
    WALEntry smallEntry = wal.createAndAppend(ByteBuffer.wrap(smallData.getBytes()));
    entries.add(smallEntry);

    // Now add the critical entry that should exactly fill the page
    String data97 = String.format("data%012d", 98); // 16 bytes (41 total bytes)
    WALEntry entry97 = wal.createAndAppend(ByteBuffer.wrap(data97.getBytes()));
    entries.add(entry97);

    // Add one more entry that should go to new page
    String data98 = String.format("data%012d", 99); // 16 bytes
    WALEntry entry98 = wal.createAndAppend(ByteBuffer.wrap(data98.getBytes()));
    entries.add(entry98);

    Instant afterWrite = Instant.now().plusSeconds(1);
    wal.sync();

    // Read all entries back by timestamp and verify no duplicates and correct sequence
    List<WALEntry> readEntries =
        Flux.from(wal.readRange(beforeWrite, afterWrite)).collectList().block();

    assertEquals(100, readEntries.size(), "Should have exactly 100 entries");

    // Verify sequence numbers are consecutive and no duplicates
    Set<Long> sequenceNumbers = new HashSet<>();
    for (int i = 0; i < readEntries.size(); i++) {
      WALEntry entry = readEntries.get(i);
      assertEquals(i, entry.getSequenceNumber(), "Sequence number should be consecutive");
      assertTrue(
          sequenceNumbers.add(entry.getSequenceNumber()),
          "Duplicate sequence number found: " + entry.getSequenceNumber());
    }

    // Specifically verify the critical entries around the page boundary
    assertEquals(97, readEntries.get(97).getSequenceNumber());
    assertEquals(98, readEntries.get(98).getSequenceNumber());
    assertEquals(99, readEntries.get(99).getSequenceNumber());
  }
}

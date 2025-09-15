package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/** Integration test to demonstrate that the thread safety issues have been fixed. */
class ThreadSafetyIntegrationTest {

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
  void testConcurrentCreateEntryOperations()
      throws InterruptedException, ExecutionException, WALException {
    // This test demonstrates that the thread safety issue is fixed
    int numThreads = 10;
    int entriesPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    List<Future<List<WALEntry>>> futures = new ArrayList<>();

    // Submit tasks that create entries concurrently
    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      Future<List<WALEntry>> future =
          executor.submit(
              () -> {
                List<WALEntry> threadEntries = new ArrayList<>();
                for (int j = 0; j < entriesPerThread; j++) {
                  String data = "thread_" + threadId + "_entry_" + j;
                  try {
                    WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
                    threadEntries.add(entry);
                  } catch (WALException e) {
                    throw new RuntimeException("Failed to create entry", e);
                  }
                }
                return threadEntries;
              });
      futures.add(future);
    }

    // Collect all entries from all threads
    List<WALEntry> allEntries = new ArrayList<>();
    for (Future<List<WALEntry>> future : futures) {
      allEntries.addAll(future.get());
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    // Verify results
    assertEquals(numThreads * entriesPerThread, allEntries.size());

    // Verify all sequence numbers are unique and consecutive
    List<Long> sequenceNumbers = new ArrayList<>();
    for (WALEntry entry : allEntries) {
      sequenceNumbers.add(entry.getSequenceNumber());
    }

    Collections.sort(sequenceNumbers);

    // Should have consecutive sequence numbers from 0 to (total-1)
    for (int i = 0; i < sequenceNumbers.size(); i++) {
      assertEquals(
          i,
          sequenceNumbers.get(i),
          "Sequence number " + i + " should be " + i + " but was " + sequenceNumbers.get(i));
    }

    // Verify all entries are persisted correctly
    List<WALEntry> persistedEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(numThreads * entriesPerThread, persistedEntries.size());

    // Verify sequence numbers in persisted entries are also consecutive
    for (int i = 0; i < persistedEntries.size(); i++) {
      assertEquals(i, persistedEntries.get(i).getSequenceNumber());
    }
  }

  @Test
  void testConcurrentBatchOperations()
      throws InterruptedException, ExecutionException, WALException {
    // Test concurrent batch operations
    int numThreads = 5;
    int batchSize = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    List<Future<List<WALEntry>>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      Future<List<WALEntry>> future =
          executor.submit(
              () -> {
                List<ByteBuffer> batchData = new ArrayList<>();
                for (int j = 0; j < batchSize; j++) {
                  String data = "batch_thread_" + threadId + "_entry_" + j;
                  batchData.add(ByteBuffer.wrap(data.getBytes()));
                }

                try {
                  return wal.createAndAppendBatch(batchData);
                } catch (WALException e) {
                  throw new RuntimeException("Failed to create batch", e);
                }
              });
      futures.add(future);
    }

    // Collect all entries
    List<WALEntry> allEntries = new ArrayList<>();
    for (Future<List<WALEntry>> future : futures) {
      List<WALEntry> batchEntries = future.get();
      assertEquals(batchSize, batchEntries.size());
      allEntries.addAll(batchEntries);
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    // Verify total count
    assertEquals(numThreads * batchSize, allEntries.size());

    // Verify sequence numbers are unique and consecutive
    List<Long> sequenceNumbers = new ArrayList<>();
    for (WALEntry entry : allEntries) {
      sequenceNumbers.add(entry.getSequenceNumber());
    }

    Collections.sort(sequenceNumbers);

    for (int i = 0; i < sequenceNumbers.size(); i++) {
      assertEquals(i, sequenceNumbers.get(i));
    }

    wal.sync();

    // Verify persistence
    List<WALEntry> persistedEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(numThreads * batchSize, persistedEntries.size());
  }

  @Test
  void testMixedConcurrentOperations()
      throws InterruptedException, ExecutionException, WALException {
    // Test mixing single and batch operations concurrently
    ExecutorService executor = Executors.newFixedThreadPool(4);

    List<Future<Integer>> futures = new ArrayList<>();

    // Thread 1: Single entries
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < 50; i++) {
                try {
                  wal.createAndAppend(ByteBuffer.wrap(("single_" + i).getBytes()));
                } catch (WALException e) {
                  throw new RuntimeException(e);
                }
              }
              return 50;
            }));

    // Thread 2: Batch entries
    futures.add(
        executor.submit(
            () -> {
              try {
                List<ByteBuffer> batch = new ArrayList<>();
                for (int i = 0; i < 30; i++) {
                  batch.add(ByteBuffer.wrap(("batch_" + i).getBytes()));
                }
                wal.createAndAppendBatch(batch);
                return 30;
              } catch (WALException e) {
                throw new RuntimeException(e);
              }
            }));

    // Thread 3: More single entries
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < 20; i++) {
                try {
                  wal.createAndAppend(ByteBuffer.wrap(("single2_" + i).getBytes()));
                } catch (WALException e) {
                  throw new RuntimeException(e);
                }
              }
              return 20;
            }));

    // Wait for completion
    int totalExpected = 0;
    for (Future<Integer> future : futures) {
      totalExpected += future.get();
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    wal.sync();

    // Verify total count and sequence integrity
    List<WALEntry> allEntries = Flux.from(wal.readFrom(0L)).collectList().block();
    assertEquals(totalExpected, allEntries.size());

    // Verify consecutive sequence numbers
    for (int i = 0; i < allEntries.size(); i++) {
      assertEquals(i, allEntries.get(i).getSequenceNumber());
    }
  }
}

package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Performance tests for WAL implementation. These tests are tagged as "performance" and excluded
 * from regular test runs.
 *
 * <p>Run with: ./gradlew performanceTest
 */
@Tag("performance")
class WALPerformanceTest {

  // Configuration
  private static final int TOTAL_ENTRIES = 100_000;
  private static final int ENTRY_SIZE_BYTES = 1024; // 1KB per entry - configurable
  private static final byte[] FIXED_ENTRY_DATA = createFixedEntryData(ENTRY_SIZE_BYTES);
  @TempDir Path tempDir;
  private WALManager walManager;
  private Runtime runtime;

  /** Creates fixed entry data to avoid CPU cycles during test execution. */
  private static byte[] createFixedEntryData(int sizeBytes) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT|txn_12345|performance_table|1|");

    // Fill remaining space with structured data
    String pattern = "field_value_";
    while (sb.length() < sizeBytes - pattern.length()) {
      sb.append(pattern);
    }

    // Ensure exact size
    String result = sb.toString();
    if (result.length() > sizeBytes) {
      result = result.substring(0, sizeBytes);
    } else if (result.length() < sizeBytes) {
      result = result + "x".repeat(sizeBytes - result.length());
    }

    return result.getBytes();
  }

  @BeforeEach
  void setUp() throws WALException {
    runtime = Runtime.getRuntime();
    // Force garbage collection before test to get clean memory baseline
    System.gc();
    walManager = new WALManager(tempDir);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (walManager != null) {
      walManager.close();
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void testSingleEntryWritePerformance() throws WALException, IOException {
    System.out.println("\n=== WAL Single Entry Write Performance Test ===");
    System.out.println("Configuration:");
    System.out.println("  Total entries: " + TOTAL_ENTRIES);
    System.out.println("  Entry size: " + ENTRY_SIZE_BYTES + " bytes");
    System.out.println(
        "  Expected total data: " + (TOTAL_ENTRIES * ENTRY_SIZE_BYTES / 1024 / 1024) + " MB");

    // Memory baseline
    long memoryBefore = getUsedMemory();
    long maxMemoryBefore = runtime.maxMemory();

    // Performance test
    long startTime = System.nanoTime();

    for (int i = 0; i < TOTAL_ENTRIES; i++) {
      walManager.createEntry(FIXED_ENTRY_DATA);

      // Progress indicator every 10k entries
      if ((i + 1) % 10_000 == 0) {
        System.out.printf(
            "  Progress: %d/%d entries written (%.1f%%)\n",
            i + 1, TOTAL_ENTRIES, ((i + 1) * 100.0 / TOTAL_ENTRIES));
      }
    }

    // Force sync to ensure all data is written
    walManager.sync();

    long endTime = System.nanoTime();
    long executionTimeNs = endTime - startTime;

    // Memory after test
    long memoryAfter = getUsedMemory();
    long maxMemoryAfter = runtime.maxMemory();

    // Verify correctness
    assertEquals(TOTAL_ENTRIES, walManager.size());
    assertEquals(TOTAL_ENTRIES - 1, walManager.getCurrentSequenceNumber());

    // Calculate and display metrics
    displayPerformanceMetrics(
        executionTimeNs, memoryBefore, memoryAfter, maxMemoryBefore, maxMemoryAfter);

    // Display file information
    displayFileMetrics();

    // Performance assertions (adjust thresholds as needed)
    double entriesPerSecond = TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0);
    assertTrue(
        entriesPerSecond > 1000,
        "Should write at least 1000 entries/second, got: " + entriesPerSecond);

    // Memory usage should be reasonable (less than 100MB for this test)
    long memoryUsedMB = (memoryAfter - memoryBefore) / 1024 / 1024;
    assertTrue(memoryUsedMB < 100, "Memory usage should be < 100MB, got: " + memoryUsedMB + "MB");
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testBatchWritePerformance() throws WALException, IOException {
    System.out.println("\n=== WAL Batch Write Performance Test ===");

    int batchSize = 1000;
    int numBatches = TOTAL_ENTRIES / batchSize;

    System.out.println("Configuration:");
    System.out.println("  Total entries: " + TOTAL_ENTRIES);
    System.out.println("  Batch size: " + batchSize);
    System.out.println("  Number of batches: " + numBatches);
    System.out.println("  Entry size: " + ENTRY_SIZE_BYTES + " bytes");

    // Prepare batch data (reuse same data to avoid CPU overhead)
    List<byte[]> batchData = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      batchData.add(FIXED_ENTRY_DATA);
    }

    // Memory baseline
    long memoryBefore = getUsedMemory();
    long maxMemoryBefore = runtime.maxMemory();

    // Performance test
    long startTime = System.nanoTime();

    for (int batch = 0; batch < numBatches; batch++) {
      walManager.createEntryBatchFromBytes(batchData);

      // Progress indicator
      if ((batch + 1) % 10 == 0) {
        int entriesWritten = (batch + 1) * batchSize;
        System.out.printf(
            "  Progress: %d/%d batches (%d entries, %.1f%%)\n",
            batch + 1, numBatches, entriesWritten, (entriesWritten * 100.0 / TOTAL_ENTRIES));
      }
    }

    // Force sync
    walManager.sync();

    long endTime = System.nanoTime();
    long executionTimeNs = endTime - startTime;

    // Memory after test
    long memoryAfter = getUsedMemory();
    long maxMemoryAfter = runtime.maxMemory();

    // Verify correctness
    assertEquals(TOTAL_ENTRIES, walManager.size());

    // Calculate and display metrics
    displayPerformanceMetrics(
        executionTimeNs, memoryBefore, memoryAfter, maxMemoryBefore, maxMemoryAfter);

    // Display file information
    displayFileMetrics();

    // Performance assertions - batch should be faster than single writes
    double entriesPerSecond = TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0);
    assertTrue(
        entriesPerSecond > 5000,
        "Batch writes should achieve at least 5000 entries/second, got: " + entriesPerSecond);
  }

  @Test
  @Timeout(value = 45, unit = TimeUnit.SECONDS)
  void testReadPerformance() throws WALException, IOException {
    System.out.println("\n=== WAL Read Performance Test ===");

    // First, write test data
    System.out.println("Setting up test data...");
    for (int i = 0; i < TOTAL_ENTRIES; i++) {
      walManager.createEntry(FIXED_ENTRY_DATA);
      if ((i + 1) % 20_000 == 0) {
        System.out.printf("  Setup progress: %d/%d entries\n", i + 1, TOTAL_ENTRIES);
      }
    }
    walManager.sync();

    System.out.println("Starting read performance test...");

    // Memory baseline
    long memoryBefore = getUsedMemory();

    // Test reading all entries
    long startTime = System.nanoTime();

    List<WALEntry> allEntries = walManager.readFrom(0L);

    long endTime = System.nanoTime();
    long executionTimeNs = endTime - startTime;

    // Memory after read
    long memoryAfter = getUsedMemory();

    // Verify correctness
    assertEquals(TOTAL_ENTRIES, allEntries.size());

    // Display read performance metrics
    System.out.println("\nRead Performance Results:");
    System.out.printf("  Entries read: %d\n", allEntries.size());
    System.out.printf("  Execution time: %.2f seconds\n", executionTimeNs / 1_000_000_000.0);
    System.out.printf(
        "  Read rate: %.0f entries/second\n", TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0));
    System.out.printf(
        "  Memory used for reading: %d MB\n", (memoryAfter - memoryBefore) / 1024 / 1024);

    // Display file information
    displayFileMetrics();

    // Performance assertion
    double entriesPerSecond = TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0);
    assertTrue(
        entriesPerSecond > 10000,
        "Should read at least 10000 entries/second, got: " + entriesPerSecond);
  }

  private long getUsedMemory() {
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private void displayPerformanceMetrics(
      long executionTimeNs,
      long memoryBefore,
      long memoryAfter,
      long maxMemoryBefore,
      long maxMemoryAfter) {
    double executionTimeSeconds = executionTimeNs / 1_000_000_000.0;
    double entriesPerSecond = TOTAL_ENTRIES / executionTimeSeconds;
    double mbPerSecond =
        (TOTAL_ENTRIES * ENTRY_SIZE_BYTES / 1024.0 / 1024.0) / executionTimeSeconds;

    System.out.println("\nPerformance Results:");
    System.out.printf("  Total execution time: %.2f seconds\n", executionTimeSeconds);
    System.out.printf("  Entries per second: %.0f\n", entriesPerSecond);
    System.out.printf("  Throughput: %.2f MB/second\n", mbPerSecond);
    System.out.printf(
        "  Average time per entry: %.2f microseconds\n",
        (executionTimeNs / 1000.0) / TOTAL_ENTRIES);

    System.out.println("\nMemory Usage:");
    System.out.printf("  Memory before test: %d MB\n", memoryBefore / 1024 / 1024);
    System.out.printf("  Memory after test: %d MB\n", memoryAfter / 1024 / 1024);
    System.out.printf("  Memory used by test: %d MB\n", (memoryAfter - memoryBefore) / 1024 / 1024);
    System.out.printf("  Max memory available: %d MB\n", maxMemoryAfter / 1024 / 1024);
    System.out.printf("  Memory utilization: %.1f%%\n", (memoryAfter * 100.0) / maxMemoryAfter);
  }

  private void displayFileMetrics() throws IOException {
    System.out.println("\nFile System Metrics:");

    long totalFileSize = 0;
    int fileCount = 0;

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDir, "*.log")) {
      for (Path file : stream) {
        long fileSize = Files.size(file);
        totalFileSize += fileSize;
        fileCount++;
        System.out.printf("  %s: %.2f MB\n", file.getFileName(), fileSize / 1024.0 / 1024.0);
      }
    }

    // Check for sequence file
    Path sequenceFile = tempDir.resolve("sequence.dat");
    if (Files.exists(sequenceFile)) {
      long seqFileSize = Files.size(sequenceFile);
      System.out.printf("  sequence.dat: %d bytes\n", seqFileSize);
      totalFileSize += seqFileSize;
    }

    System.out.printf("  Total WAL files: %d\n", fileCount);
    System.out.printf("  Total file size: %.2f MB\n", totalFileSize / 1024.0 / 1024.0);
    System.out.printf(
        "  Expected data size: %.2f MB\n", (TOTAL_ENTRIES * ENTRY_SIZE_BYTES) / 1024.0 / 1024.0);
    System.out.printf(
        "  Overhead: %.2f MB (%.1f%%)\n",
        (totalFileSize - (TOTAL_ENTRIES * ENTRY_SIZE_BYTES)) / 1024.0 / 1024.0,
        ((totalFileSize - (TOTAL_ENTRIES * ENTRY_SIZE_BYTES)) * 100.0)
            / (TOTAL_ENTRIES * ENTRY_SIZE_BYTES));

    // Calculate compression ratio
    double compressionRatio = (double) totalFileSize / (TOTAL_ENTRIES * ENTRY_SIZE_BYTES);
    System.out.printf("  Storage efficiency: %.3f (1.0 = no overhead)\n", compressionRatio);
  }

  @Test
  @Timeout(value = 45, unit = TimeUnit.SECONDS)
  void testOptimizedRestartPerformance() throws Exception {
    System.out.println("\n=== WAL Optimized Restart Performance Test ===");
    System.out.println("Configuration:");
    System.out.println("  Total entries: " + TOTAL_ENTRIES);
    System.out.println("  Entry size: " + ENTRY_SIZE_BYTES + " bytes");
    System.out.println("  Testing optimized sequence/timestamp range building on restart");

    // Memory baseline
    long memoryBefore = getUsedMemory();

    // Phase 1: Create WAL with many entries across multiple files
    System.out.println("\nPhase 1: Creating WAL with multiple files...");
    long creationStartTime = System.nanoTime();

    for (int i = 0; i < TOTAL_ENTRIES; i++) {
      walManager.createEntry(FIXED_ENTRY_DATA);

      // Progress indicator every 20k entries
      if ((i + 1) % 20_000 == 0) {
        System.out.printf(
            "  Creation progress: %d/%d entries (%.1f%%)\n",
            i + 1, TOTAL_ENTRIES, ((i + 1) * 100.0 / TOTAL_ENTRIES));
      }
    }

    walManager.sync();
    long creationEndTime = System.nanoTime();
    long creationTimeNs = creationEndTime - creationStartTime;

    // Display file information before restart
    System.out.println("\nFiles created:");
    displayFileMetrics();

    // Phase 2: Close WAL (this should be fast)
    System.out.println("\nPhase 2: Closing WAL...");
    long closeStartTime = System.nanoTime();

    walManager.close();

    long closeEndTime = System.nanoTime();
    long closeTimeNs = closeEndTime - closeStartTime;

    // Phase 3: Reopen WAL (this triggers optimized range building)
    System.out.println("\nPhase 3: Reopening WAL with optimized range building...");
    long reopenStartTime = System.nanoTime();

    walManager = new WALManager(tempDir);

    long reopenEndTime = System.nanoTime();
    long reopenTimeNs = reopenEndTime - reopenStartTime;

    // Memory after restart
    System.gc();
    long memoryAfter = getUsedMemory();

    // Phase 4: Verify correctness after restart
    System.out.println("\nPhase 4: Verifying correctness after restart...");
    long verifyStartTime = System.nanoTime();

    assertEquals(TOTAL_ENTRIES, walManager.size());
    assertEquals(TOTAL_ENTRIES - 1, walManager.getCurrentSequenceNumber());

    // Test optimized range queries work correctly
    List<WALEntry> middleRange = walManager.readRange(TOTAL_ENTRIES / 2 - 5, TOTAL_ENTRIES / 2 + 5);
    assertEquals(11, middleRange.size());

    // Test timestamp-based queries work correctly
    List<WALEntry> allEntries = walManager.readFrom(0L);
    assertEquals(TOTAL_ENTRIES, allEntries.size());

    Instant firstTimestamp = allEntries.get(0).getTimestamp();
    Instant lastTimestamp = allEntries.get(allEntries.size() - 1).getTimestamp();

    List<WALEntry> timestampRange =
        walManager.readRange(firstTimestamp, firstTimestamp.plusSeconds(1));
    assertTrue(timestampRange.size() > 0, "Should find entries in timestamp range");

    long verifyEndTime = System.nanoTime();
    long verifyTimeNs = verifyEndTime - verifyStartTime;

    // Display performance results
    displayRestartPerformanceMetrics(
        creationTimeNs, closeTimeNs, reopenTimeNs, verifyTimeNs, memoryBefore, memoryAfter);

    // Performance assertions
    double reopenTimeSeconds = reopenTimeNs / 1_000_000_000.0;
    double creationTimeSeconds = creationTimeNs / 1_000_000_000.0;

    // Reopen should be much faster than creation (optimized range building)
    assertTrue(
        reopenTimeSeconds < creationTimeSeconds / 10,
        String.format(
            "Optimized restart should be at least 10x faster than creation. "
                + "Creation: %.2fs, Restart: %.2fs",
            creationTimeSeconds, reopenTimeSeconds));

    // Reopen should complete within reasonable time (adjust based on hardware)
    assertTrue(
        reopenTimeSeconds < 5.0,
        "Optimized restart should complete within 5 seconds, got: " + reopenTimeSeconds + "s");

    System.out.println("\n✅ Optimized restart performance test completed successfully!");
  }

  private void displayRestartPerformanceMetrics(
      long creationTimeNs,
      long closeTimeNs,
      long reopenTimeNs,
      long verifyTimeNs,
      long memoryBefore,
      long memoryAfter)
      throws IOException {

    double creationTimeSeconds = creationTimeNs / 1_000_000_000.0;
    double closeTimeSeconds = closeTimeNs / 1_000_000_000.0;
    double reopenTimeSeconds = reopenTimeNs / 1_000_000_000.0;
    double verifyTimeSeconds = verifyTimeNs / 1_000_000_000.0;
    double totalTimeSeconds =
        creationTimeSeconds + closeTimeSeconds + reopenTimeSeconds + verifyTimeSeconds;

    System.out.println("\nRestart Performance Results:");
    System.out.printf("  Phase 1 - Creation time: %.2f seconds\n", creationTimeSeconds);
    System.out.printf("  Phase 2 - Close time: %.3f seconds\n", closeTimeSeconds);
    System.out.printf("  Phase 3 - Reopen time (optimized): %.3f seconds\n", reopenTimeSeconds);
    System.out.printf("  Phase 4 - Verification time: %.3f seconds\n", verifyTimeSeconds);
    System.out.printf("  Total test time: %.2f seconds\n", totalTimeSeconds);

    System.out.println("\nOptimization Benefits:");
    System.out.printf(
        "  Restart speedup vs creation: %.1fx faster\n", creationTimeSeconds / reopenTimeSeconds);
    System.out.printf(
        "  Restart overhead: %.1f%% of creation time\n",
        (reopenTimeSeconds * 100.0) / creationTimeSeconds);

    System.out.println("\nMemory Usage:");
    System.out.printf("  Memory before test: %d MB\n", memoryBefore / 1024 / 1024);
    System.out.printf("  Memory after restart: %d MB\n", memoryAfter / 1024 / 1024);
    System.out.printf("  Memory used by test: %d MB\n", (memoryAfter - memoryBefore) / 1024 / 1024);

    // Display file information
    displayFileMetrics();

    System.out.println("\nPerformance Analysis:");
    if (reopenTimeSeconds < 1.0) {
      System.out.println("  🚀 EXCELLENT: Restart time < 1 second");
    } else if (reopenTimeSeconds < 3.0) {
      System.out.println("  ✅ GOOD: Restart time < 3 seconds");
    } else {
      System.out.println("  ⚠️  ACCEPTABLE: Restart time < 5 seconds");
    }

    double speedupRatio = creationTimeSeconds / reopenTimeSeconds;
    if (speedupRatio > 100) {
      System.out.println("  🚀 EXCELLENT: >100x speedup from optimized range building");
    } else if (speedupRatio > 50) {
      System.out.println("  ✅ GOOD: >50x speedup from optimized range building");
    } else if (speedupRatio > 10) {
      System.out.println("  ✅ ACCEPTABLE: >10x speedup from optimized range building");
    } else {
      System.out.println("  ⚠️  WARNING: <10x speedup - optimization may not be working optimally");
    }
  }
}

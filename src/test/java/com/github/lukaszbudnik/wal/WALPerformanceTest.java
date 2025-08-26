package com.github.lukaszbudnik.wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance tests for WAL implementation.
 * These tests are tagged as "performance" and excluded from regular test runs.
 * 
 * Run with: ./gradlew performanceTest
 */
@Tag("performance")
class WALPerformanceTest {
    
    @TempDir
    Path tempDir;
    
    private WALManager walManager;
    private Runtime runtime;
    
    // Configuration
    private static final int TOTAL_ENTRIES = 100_000;
    private static final int ENTRY_SIZE_BYTES = 1024; // 1KB per entry - configurable
    private static final byte[] FIXED_ENTRY_DATA = createFixedEntryData(ENTRY_SIZE_BYTES);
    
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
    
    /**
     * Creates fixed entry data to avoid CPU cycles during test execution.
     */
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
    
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSingleEntryWritePerformance() throws WALException, IOException {
        System.out.println("\n=== WAL Single Entry Write Performance Test ===");
        System.out.println("Configuration:");
        System.out.println("  Total entries: " + TOTAL_ENTRIES);
        System.out.println("  Entry size: " + ENTRY_SIZE_BYTES + " bytes");
        System.out.println("  Expected total data: " + (TOTAL_ENTRIES * ENTRY_SIZE_BYTES / 1024 / 1024) + " MB");
        
        // Memory baseline
        long memoryBefore = getUsedMemory();
        long maxMemoryBefore = runtime.maxMemory();
        
        // Performance test
        long startTime = System.nanoTime();
        
        for (int i = 0; i < TOTAL_ENTRIES; i++) {
            walManager.createEntry(FIXED_ENTRY_DATA);
            
            // Progress indicator every 10k entries
            if ((i + 1) % 10_000 == 0) {
                System.out.printf("  Progress: %d/%d entries written (%.1f%%)\n", 
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
        displayPerformanceMetrics(executionTimeNs, memoryBefore, memoryAfter, maxMemoryBefore, maxMemoryAfter);
        
        // Display file information
        displayFileMetrics();
        
        // Performance assertions (adjust thresholds as needed)
        double entriesPerSecond = TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0);
        assertTrue(entriesPerSecond > 1000, "Should write at least 1000 entries/second, got: " + entriesPerSecond);
        
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
                System.out.printf("  Progress: %d/%d batches (%d entries, %.1f%%)\n", 
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
        displayPerformanceMetrics(executionTimeNs, memoryBefore, memoryAfter, maxMemoryBefore, maxMemoryAfter);
        
        // Display file information
        displayFileMetrics();
        
        // Performance assertions - batch should be faster than single writes
        double entriesPerSecond = TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0);
        assertTrue(entriesPerSecond > 5000, "Batch writes should achieve at least 5000 entries/second, got: " + entriesPerSecond);
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
        System.out.printf("  Read rate: %.0f entries/second\n", TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0));
        System.out.printf("  Memory used for reading: %d MB\n", (memoryAfter - memoryBefore) / 1024 / 1024);
        
        // Display file information
        displayFileMetrics();
        
        // Performance assertion
        double entriesPerSecond = TOTAL_ENTRIES / (executionTimeNs / 1_000_000_000.0);
        assertTrue(entriesPerSecond > 10000, "Should read at least 10000 entries/second, got: " + entriesPerSecond);
    }
    
    private long getUsedMemory() {
        return runtime.totalMemory() - runtime.freeMemory();
    }
    
    private void displayPerformanceMetrics(long executionTimeNs, long memoryBefore, long memoryAfter, 
                                         long maxMemoryBefore, long maxMemoryAfter) {
        double executionTimeSeconds = executionTimeNs / 1_000_000_000.0;
        double entriesPerSecond = TOTAL_ENTRIES / executionTimeSeconds;
        double mbPerSecond = (TOTAL_ENTRIES * ENTRY_SIZE_BYTES / 1024.0 / 1024.0) / executionTimeSeconds;
        
        System.out.println("\nPerformance Results:");
        System.out.printf("  Total execution time: %.2f seconds\n", executionTimeSeconds);
        System.out.printf("  Entries per second: %.0f\n", entriesPerSecond);
        System.out.printf("  Throughput: %.2f MB/second\n", mbPerSecond);
        System.out.printf("  Average time per entry: %.2f microseconds\n", (executionTimeNs / 1000.0) / TOTAL_ENTRIES);
        
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
        System.out.printf("  Expected data size: %.2f MB\n", (TOTAL_ENTRIES * ENTRY_SIZE_BYTES) / 1024.0 / 1024.0);
        System.out.printf("  Overhead: %.2f MB (%.1f%%)\n", 
            (totalFileSize - (TOTAL_ENTRIES * ENTRY_SIZE_BYTES)) / 1024.0 / 1024.0,
            ((totalFileSize - (TOTAL_ENTRIES * ENTRY_SIZE_BYTES)) * 100.0) / (TOTAL_ENTRIES * ENTRY_SIZE_BYTES));
        
        // Calculate compression ratio
        double compressionRatio = (double) totalFileSize / (TOTAL_ENTRIES * ENTRY_SIZE_BYTES);
        System.out.printf("  Storage efficiency: %.3f (1.0 = no overhead)\n", compressionRatio);
    }
}

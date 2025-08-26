package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test to verify that sequence range optimization works correctly for efficient file selection.
 */
class SequenceRangeOptimizationTest {
    
    @TempDir
    Path tempDir;
    
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
    void testSequenceRangeOptimizationWithMultipleFiles() throws WALException {
        // Create entries that will span multiple files
        // Each entry is about 150 bytes, so ~7 entries per 1KB file
        
        // File 0: sequences 0-6 (7 entries)
        for (int i = 0; i < 7; i++) {
            String data = "entry_" + i + "_" + "x".repeat(100);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        // File 1: sequences 7-13 (7 entries)  
        for (int i = 7; i < 14; i++) {
            String data = "entry_" + i + "_" + "x".repeat(100);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        // File 2: sequences 14-20 (7 entries)
        for (int i = 14; i < 21; i++) {
            String data = "entry_" + i + "_" + "x".repeat(100);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        // File 3: sequences 21-27 (7 entries)
        for (int i = 21; i < 28; i++) {
            String data = "entry_" + i + "_" + "x".repeat(100);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        // File 4: sequences 28-29 (2 entries)
        for (int i = 28; i < 30; i++) {
            String data = "entry_" + i + "_" + "x".repeat(100);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        wal.sync();
        
        // Verify we have 30 entries total
        assertEquals(30, wal.size());
        assertEquals(29, wal.getCurrentSequenceNumber());
        
        // Test 1: Read from sequence 23 to end (should only need files 3 and 4)
        List<WALEntry> entriesFromSeq23 = wal.readRange(23L, Long.MAX_VALUE);
        assertEquals(7, entriesFromSeq23.size()); // sequences 23-29
        assertEquals(23L, entriesFromSeq23.get(0).getSequenceNumber());
        assertEquals(29L, entriesFromSeq23.get(6).getSequenceNumber());
        
        // Test 2: Read middle range (should only need files 1 and 2)
        List<WALEntry> middleRange = wal.readRange(10L, 17L);
        assertEquals(8, middleRange.size()); // sequences 10-17
        assertEquals(10L, middleRange.get(0).getSequenceNumber());
        assertEquals(17L, middleRange.get(7).getSequenceNumber());
        
        // Test 3: Read single file range (should only need file 0)
        List<WALEntry> singleFileRange = wal.readRange(2L, 5L);
        assertEquals(4, singleFileRange.size()); // sequences 2-5
        assertEquals(2L, singleFileRange.get(0).getSequenceNumber());
        assertEquals(5L, singleFileRange.get(3).getSequenceNumber());
        
        // Test 4: Read across all files
        List<WALEntry> allEntries = wal.readRange(0L, 29L);
        assertEquals(30, allEntries.size());
        assertEquals(0L, allEntries.get(0).getSequenceNumber());
        assertEquals(29L, allEntries.get(29).getSequenceNumber());
        
        // Test 5: Read non-existent range (should return empty)
        List<WALEntry> nonExistent = wal.readRange(100L, 200L);
        assertEquals(0, nonExistent.size());
        
        // Test 6: Read range that spans file boundary
        List<WALEntry> spanBoundary = wal.readRange(5L, 9L);
        assertEquals(5, spanBoundary.size()); // sequences 5-9
        assertEquals(5L, spanBoundary.get(0).getSequenceNumber());
        assertEquals(9L, spanBoundary.get(4).getSequenceNumber());
    }
    
    @Test
    void testSequenceRangeWithFileRotation() throws WALException {
        // Test that sequence ranges are properly maintained during file rotation
        
        // Add entries to trigger multiple rotations
        for (int i = 0; i < 50; i++) {
            String data = "rotation_test_" + i + "_" + "x".repeat(80);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        wal.sync();
        
        // Verify total entries
        assertEquals(50, wal.size());
        assertEquals(49, wal.getCurrentSequenceNumber());
        
        // Test reading from various points to ensure optimization works
        
        // Read from beginning
        List<WALEntry> fromBeginning = wal.readRange(0L, 10L);
        assertEquals(11, fromBeginning.size());
        
        // Read from middle
        List<WALEntry> fromMiddle = wal.readRange(20L, 30L);
        assertEquals(11, fromMiddle.size());
        
        // Read from end
        List<WALEntry> fromEnd = wal.readRange(40L, 49L);
        assertEquals(10, fromEnd.size());
        
        // Verify sequence continuity
        for (int i = 0; i < fromEnd.size(); i++) {
            assertEquals(40L + i, fromEnd.get(i).getSequenceNumber());
        }
    }
    
    @Test
    void testSequenceRangeAfterRestart() throws Exception {
        // Test that sequence ranges are properly rebuilt after WAL restart
        
        // Create entries across multiple files
        for (int i = 0; i < 25; i++) {
            String data = "restart_test_" + i + "_" + "x".repeat(90);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        wal.sync();
        
        // Close and reopen WAL
        wal.close();
        wal = new FileBasedWAL(tempDir, 1024, true);
        
        // Verify sequence ranges are rebuilt correctly
        assertEquals(25, wal.size());
        assertEquals(24, wal.getCurrentSequenceNumber());
        
        // Test optimized reading after restart
        List<WALEntry> afterRestart = wal.readRange(15L, 20L);
        assertEquals(6, afterRestart.size());
        assertEquals(15L, afterRestart.get(0).getSequenceNumber());
        assertEquals(20L, afterRestart.get(5).getSequenceNumber());
        
        // Verify data integrity
        for (int i = 0; i < afterRestart.size(); i++) {
            WALEntry entry = afterRestart.get(i);
            String expectedPrefix = "restart_test_" + (15 + i) + "_";
            String actualData = new String(entry.getDataAsBytes());
            assertTrue(actualData.startsWith(expectedPrefix));
        }
    }
    
    @Test
    void testSequenceRangeEdgeCases() throws WALException {
        // Test edge cases for sequence range optimization
        
        // Single entry
        wal.createAndAppend(ByteBuffer.wrap("single_entry".getBytes()));
        
        List<WALEntry> singleEntry = wal.readRange(0L, 0L);
        assertEquals(1, singleEntry.size());
        assertEquals(0L, singleEntry.get(0).getSequenceNumber());
        
        // Add more entries
        for (int i = 1; i < 10; i++) {
            String data = "edge_case_" + i + "_" + "x".repeat(100);
            wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
        }
        
        // Test boundary conditions
        
        // Read exactly at file boundaries (assuming ~7 entries per file)
        List<WALEntry> atBoundary = wal.readRange(6L, 8L);
        assertEquals(3, atBoundary.size());
        assertEquals(6L, atBoundary.get(0).getSequenceNumber());
        assertEquals(8L, atBoundary.get(2).getSequenceNumber());
        
        // Read single sequence
        List<WALEntry> singleSeq = wal.readRange(5L, 5L);
        assertEquals(1, singleSeq.size());
        assertEquals(5L, singleSeq.get(0).getSequenceNumber());
        
        // Read beyond available range
        List<WALEntry> beyondRange = wal.readRange(50L, 100L);
        assertEquals(0, beyondRange.size());
        
        // Read with reversed range (should return empty)
        List<WALEntry> reversedRange = wal.readRange(8L, 5L);
        assertEquals(0, reversedRange.size());
    }

}

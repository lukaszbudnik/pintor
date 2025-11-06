package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

class BinarySearchOptimizationTest {

  @TempDir Path tempDir;
  private FileBasedWAL wal;

  private static final int SMALL_FILE_SIZE = 16384; // 16KB = 4 pages per file

  @BeforeEach
  void setUp() throws WALException {
    wal = new FileBasedWAL(tempDir, SMALL_FILE_SIZE, (byte) 4);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (wal != null) {
      wal.close();
    }
  }

  @Test
  void testBinarySearchFileScanning_PreciseCalculation() throws WALException {
    // Create exactly 8 files with known entry distribution
    // Each entry: 25 bytes header + 100 bytes data = 125 bytes
    // Entries per page: 4052 / 125 = 32 entries per page
    // Pages per file: 4 pages per file
    // Entries per file: 32 * 4 = 128 entries per file
    // Total: 8 files * 128 entries = 1024 entries

    List<WALEntry> entries = createPreciseFileStructure();
    assertEquals(1024, entries.size());

    WALMetrics metrics = wal.getMetrics();
    assertEquals(9, metrics.getFilesCreated()); // 8 data files + 1 final file

    // Query entries in files 3-5 (entries 384-639)
    // LINEAR SEARCH: Would scan ALL 9 files (read 9 file headers)
    // BINARY SEARCH: Scans ~2*log2(9) ≈ 7 files during binary search operations
    long startSeq = entries.get(384).getSequenceNumber(); // File 3
    long endSeq = entries.get(639).getSequenceNumber(); // File 5

    long filesScannedBefore = metrics.getFilesScanned();
    List<WALEntry> result = collectEntries(wal.readRange(startSeq, endSeq));
    long filesScannedAfter = metrics.getFilesScanned();

    assertEquals(256, result.size()); // 384-639 inclusive

    // Binary search should scan significantly fewer files than linear
    // LINEAR: 9 files scanned | BINARY: ≤7 files scanned (2*log2(9) ≈ 6.3)
    long filesScanned = filesScannedAfter - filesScannedBefore;
    assertTrue(
        filesScanned <= 7,
        "Binary search should scan ≤7 files, but scanned: "
            + filesScanned
            + " (linear would scan 9)");

    // Verify we found the correct range
    assertEquals(384, result.get(0).getSequenceNumber());
    assertEquals(639, result.get(result.size() - 1).getSequenceNumber());
  }

  @Test
  void testBinarySearchPageScanning_PreciseCalculation() throws WALException {
    // Create entries that span exactly 16 pages in single file
    // Each entry: 25 bytes header + 200 bytes data = 225 bytes
    // Entries per page: 4052 / 225 = 18 entries per page
    // Total: 16 pages * 18 entries = 288 entries

    List<WALEntry> entries = createPrecisePageStructure();
    assertEquals(288, entries.size());

    WALMetrics metrics = wal.getMetrics();
    assertTrue(metrics.getPagesWritten() >= 16, "Should have written at least 16 pages");

    // Query entries in pages 6-10 (entries 108-179)
    // LINEAR SEARCH: Would scan ALL pages sequentially
    // BINARY SEARCH: Scans ~2*log2(pages) page headers + reads only relevant pages
    long startSeq = entries.get(108).getSequenceNumber(); // Page 6
    long endSeq = entries.get(179).getSequenceNumber(); // Page 10

    long pagesReadBefore = metrics.getPagesRead();

    List<WALEntry> result = collectEntries(wal.readRange(startSeq, endSeq));

    long pagesReadAfter = metrics.getPagesRead();

    assertEquals(72, result.size()); // 108-179 inclusive

    // Should only read data from relevant pages
    long pagesRead = pagesReadAfter - pagesReadBefore;
    assertTrue(pagesRead <= 4, "Should read ≤4 pages of data, but read: " + pagesRead);

    // Verify we found the correct range
    assertEquals(108, result.get(0).getSequenceNumber());
    assertEquals(179, result.get(result.size() - 1).getSequenceNumber());
  }

  @Test
  void testSparseQueryEfficiency() throws WALException {
    // Create 16 files with known structure
    List<WALEntry> entries = createLargeFileStructure();
    assertEquals(2048, entries.size()); // 16 files * 128 entries

    WALMetrics metrics = wal.getMetrics();
    assertEquals(17, metrics.getFilesCreated()); // 16 data files + 1 final file

    // Query tiny range in file 14 (entries 1792-1796)
    // LINEAR SEARCH: Would scan ALL 17 files for just 5 entries
    // BINARY SEARCH: Scans ~2*log2(17) ≈ 8-9 files during binary search
    long startSeq = entries.get(1792).getSequenceNumber();
    long endSeq = entries.get(1796).getSequenceNumber();

    long filesScannedBefore = metrics.getFilesScanned();
    List<WALEntry> result = collectEntries(wal.readRange(startSeq, endSeq));
    long filesScannedAfter = metrics.getFilesScanned();

    assertEquals(5, result.size());

    // Sparse query efficiency: 5 entries from 2048 total
    // LINEAR: 17 files scanned | BINARY: ≤10 files scanned
    long filesScanned = filesScannedAfter - filesScannedBefore;
    assertTrue(
        filesScanned <= 9,
        "Sparse query should scan ≤9 files, but scanned: "
            + filesScanned
            + " (linear would scan 17)");
  }

  private List<WALEntry> createPreciseFileStructure() throws WALException {
    List<WALEntry> entries = new ArrayList<>();

    // Create exactly 1024 entries across 8 files
    // Each entry: 100 bytes data + 25 bytes header = 125 bytes total
    for (int i = 0; i < 1024; i++) {
      String data = String.format("entry_%04d_", i) + "x".repeat(85); // Exactly 100 bytes
      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
      entries.add(entry);

      // Force file rotation every 128 entries
      if ((i + 1) % 128 == 0) {
        wal.sync();
      }
    }

    wal.sync();
    return entries;
  }

  private List<WALEntry> createPrecisePageStructure() throws WALException {
    List<WALEntry> entries = new ArrayList<>();

    // Create exactly 288 entries across 16 pages in single file
    // Each entry: 200 bytes data + 25 bytes header = 225 bytes total
    for (int i = 0; i < 288; i++) {
      String data = String.format("page_entry_%03d_", i) + "x".repeat(184); // Exactly 200 bytes
      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
      entries.add(entry);
    }

    wal.sync();
    return entries;
  }

  private List<WALEntry> createLargeFileStructure() throws WALException {
    List<WALEntry> entries = new ArrayList<>();

    // Create exactly 2048 entries across 16 files
    for (int i = 0; i < 2048; i++) {
      String data = String.format("large_%04d_", i) + "x".repeat(85); // 100 bytes
      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
      entries.add(entry);

      // Force file rotation every 128 entries
      if ((i + 1) % 128 == 0) {
        wal.sync();
      }
    }

    wal.sync();
    return entries;
  }

  private List<WALEntry> createPreciseFileStructureWithUniqueTimestamps() throws WALException {
    List<WALEntry> entries = new ArrayList<>();

    // Create exactly 1024 entries across 8 files with unique timestamps
    // Each entry: 100 bytes data + 25 bytes header = 125 bytes total
    for (int i = 0; i < 1024; i++) {
      String data = String.format("entry_%04d_", i) + "x".repeat(85); // Exactly 100 bytes
      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
      entries.add(entry);

      // Sleep to ensure unique timestamps
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      // Force file rotation every 128 entries
      if ((i + 1) % 128 == 0) {
        wal.sync();
      }
    }

    wal.sync();
    return entries;
  }

  private List<WALEntry> createPrecisePageStructureWithUniqueTimestamps() throws WALException {
    List<WALEntry> entries = new ArrayList<>();

    // Create exactly 288 entries across 16 pages in single file with unique timestamps
    // Each entry: 200 bytes data + 25 bytes header = 225 bytes total
    for (int i = 0; i < 288; i++) {
      String data = String.format("page_entry_%03d_", i) + "x".repeat(184); // Exactly 200 bytes
      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(data.getBytes()));
      entries.add(entry);

      // Sleep to ensure unique timestamps
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    wal.sync();
    return entries;
  }

  @Test
  void testBinarySearchPageScanningByTimestamp_PreciseCalculation() throws WALException {
    // Create entries that span exactly 16 pages in single file with unique timestamps
    List<WALEntry> entries = createPrecisePageStructureWithUniqueTimestamps();
    assertEquals(288, entries.size());

    WALMetrics metrics = wal.getMetrics();
    assertTrue(metrics.getPagesWritten() >= 16, "Should have written at least 16 pages");

    // Query entries by timestamp range
    Instant startTime = entries.get(100).getTimestamp();
    Instant endTime = entries.get(200).getTimestamp();

    long pagesReadBefore = metrics.getPagesRead();
    List<WALEntry> result = collectEntries(wal.readRange(startTime, endTime));
    long pagesReadAfter = metrics.getPagesRead();

    // Should get entries in the expected range (at least 100 entries: 100-200 inclusive)
    assertTrue(result.size() >= 100, "Should get at least 100 entries, got: " + result.size());

    // Most importantly: verify binary search limited page reads
    long pagesRead = pagesReadAfter - pagesReadBefore;
    assertTrue(pagesRead <= 7, "Should read ≤7 pages of data, but read: " + pagesRead);

    System.out.println(
        "Timestamp page-level binary search: read "
            + pagesRead
            + " pages, got "
            + result.size()
            + " entries");
  }

  @Test
  void testSparseQueryEfficiencyByTimestamp() throws WALException {
    // Create entries with unique timestamps
    List<WALEntry> entries = createPreciseFileStructureWithUniqueTimestamps();
    assertEquals(1024, entries.size());

    WALMetrics metrics = wal.getMetrics();

    // Query a small range by timestamp
    Instant startTime = entries.get(500).getTimestamp();
    Instant endTime = entries.get(510).getTimestamp();

    long filesScannedBefore = metrics.getFilesScanned();
    List<WALEntry> result = collectEntries(wal.readRange(startTime, endTime));
    long filesScannedAfter = metrics.getFilesScanned();

    // Should get entries in the expected range (at least 10 entries: 500-510 inclusive)
    assertTrue(result.size() >= 10, "Should get at least 10 entries, got: " + result.size());

    // Most importantly: verify binary search limited file scans
    long filesScanned = filesScannedAfter - filesScannedBefore;
    assertTrue(filesScanned <= 7, "Should scan ≤7 files, but scanned: " + filesScanned);
  }

  private List<WALEntry> collectEntries(Publisher<WALEntry> publisher) {
    return Flux.from(publisher).collectList().block();
  }
}

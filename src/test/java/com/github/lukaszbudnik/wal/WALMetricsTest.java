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
import reactor.core.publisher.Flux;

class WALMetricsTest {

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
  void testInitialMetrics() throws WALException {
    WALMetrics metrics = wal.getMetrics();

    assertEquals(0, metrics.getEntriesWritten());
    assertEquals(0, metrics.getBytesWritten());
    assertEquals(0, metrics.getPagesWritten());
    assertEquals(0, metrics.getEntriesRead());
    assertEquals(0, metrics.getPagesRead());
    assertEquals(0, metrics.getFilesScanned());
    assertEquals(0, metrics.getPagesScanned());
    assertEquals(0, metrics.getRangeQueries());
    assertEquals(1, metrics.getFilesCreated()); // First file created during initialization
  }

  @Test
  void testSingleEntryWriteMetrics() throws WALException {
    WALMetrics metrics = wal.getMetrics();

    String data = "test entry";
    ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
    wal.createAndAppend(buffer);
    wal.sync(); // Force page flush

    assertEquals(1, metrics.getEntriesWritten());
    assertTrue(metrics.getBytesWritten() > 0);
    assertTrue(metrics.getPagesWritten() > 0);
    assertEquals(1, metrics.getFilesCreated()); // Only initial file, no rotation
  }

  @Test
  void testBatchWriteMetrics() throws WALException {
    WALMetrics metrics = wal.getMetrics();

    List<ByteBuffer> batch =
        Arrays.asList(
            ByteBuffer.wrap("entry1".getBytes()),
            ByteBuffer.wrap("entry2".getBytes()),
            ByteBuffer.wrap("entry3".getBytes()));

    wal.createAndAppendBatch(batch);

    assertEquals(3, metrics.getEntriesWritten());
    assertTrue(metrics.getBytesWritten() > 0);
  }

  @Test
  void testReadMetrics() throws WALException {
    // Write some entries first
    for (int i = 0; i < 10; i++) {
      wal.createAndAppend(ByteBuffer.wrap(("entry" + i).getBytes()));
    }

    WALMetrics metrics = wal.getMetrics();
    long entriesReadBefore = metrics.getEntriesRead();
    long pagesReadBefore = metrics.getPagesRead();
    long rangeQueriesBefore = metrics.getRangeQueries();

    // Read all entries
    List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();

    assertEquals(10, metrics.getEntriesRead() - entriesReadBefore);
    assertTrue(metrics.getPagesRead() > pagesReadBefore);
    assertEquals(1, metrics.getRangeQueries() - rangeQueriesBefore);
  }

  @Test
  void testRangeQueryMetrics() throws WALException {
    // Write entries
    for (int i = 0; i < 20; i++) {
      wal.createAndAppend(ByteBuffer.wrap(("entry" + i).getBytes()));
    }

    WALMetrics metrics = wal.getMetrics();
    long entriesReadBefore = metrics.getEntriesRead();
    long rangeQueriesBefore = metrics.getRangeQueries();

    // Read range 5-15
    List<WALEntry> entries = Flux.from(wal.readRange(5L, 15L)).collectList().block();

    assertEquals(11, metrics.getEntriesRead() - entriesReadBefore); // 5-15 inclusive
    assertEquals(1, metrics.getRangeQueries() - rangeQueriesBefore);
  }

  @Test
  void testFileCreationMetrics() throws Exception {
    // Use smaller file size to force rotation
    wal.close();
    wal = new FileBasedWAL(tempDir, 8192); // 8KB files

    WALMetrics metrics = wal.getMetrics();
    long initialFiles = metrics.getFilesCreated();

    // Create entries large enough to trigger file rotation
    for (int i = 0; i < 50; i++) {
      String largeData = "entry" + i + "_" + "x".repeat(200); // ~200 bytes per entry
      wal.createAndAppend(ByteBuffer.wrap(largeData.getBytes()));

      if (i % 10 == 0) {
        wal.sync(); // Force file rotation check
      }
    }

    assertTrue(metrics.getFilesCreated() > initialFiles, "Should create additional files");
    assertEquals(50, metrics.getEntriesWritten());
  }

  @Test
  void testMetricsAccumulation() throws WALException {
    WALMetrics metrics = wal.getMetrics();

    // Multiple write operations
    wal.createAndAppend(ByteBuffer.wrap("entry1".getBytes()));
    wal.createAndAppend(ByteBuffer.wrap("entry2".getBytes()));

    assertEquals(2, metrics.getEntriesWritten());

    // Multiple read operations
    Flux.from(wal.readFrom(0L)).collectList().block();
    Flux.from(wal.readRange(0L, 1L)).collectList().block();

    assertEquals(4, metrics.getEntriesRead()); // 2 + 2
    assertEquals(2, metrics.getRangeQueries());
  }

  @Test
  void testBytesWrittenAccuracy() throws WALException {
    WALMetrics metrics = wal.getMetrics();

    String data1 = "small";
    String data2 = "much_larger_entry_with_more_data";

    long bytesBefore = metrics.getBytesWritten();

    wal.createAndAppend(ByteBuffer.wrap(data1.getBytes()));
    long bytesAfterFirst = metrics.getBytesWritten();

    wal.createAndAppend(ByteBuffer.wrap(data2.getBytes()));
    long bytesAfterSecond = metrics.getBytesWritten();

    assertTrue(bytesAfterFirst > bytesBefore);
    assertTrue(bytesAfterSecond > bytesAfterFirst);
    assertTrue((bytesAfterSecond - bytesAfterFirst) > (bytesAfterFirst - bytesBefore));
  }

  @Test
  void testPageMetrics() throws WALException {
    WALMetrics metrics = wal.getMetrics();

    // Write entries to trigger page writes
    for (int i = 0; i < 5; i++) {
      wal.createAndAppend(ByteBuffer.wrap(("entry" + i).getBytes()));
    }
    wal.sync(); // Force page flush

    long pagesWritten = metrics.getPagesWritten();
    assertTrue(pagesWritten > 0, "Should have written pages");

    // Read entries to trigger page reads
    long pagesReadBefore = metrics.getPagesRead();
    Flux.from(wal.readFrom(0L)).collectList().block();

    long pagesReadAfter = metrics.getPagesRead();
    assertTrue(pagesReadAfter > pagesReadBefore, "Should have read pages");
  }
}

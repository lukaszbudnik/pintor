package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

class ConfigurablePageSizeTest {

  @TempDir Path tempDir;

  @Test
  void testPageSizeValidation() throws Exception {
    // Valid page sizes should work
    try (FileBasedWAL wal = new FileBasedWAL(tempDir.resolve("4k"), 1024 * 1024, (byte) 4)) {}
    try (FileBasedWAL wal = new FileBasedWAL(tempDir.resolve("8k"), 1024 * 1024, (byte) 8)) {}
    try (FileBasedWAL wal = new FileBasedWAL(tempDir.resolve("16k"), 1024 * 1024, (byte) 16)) {}
    try (FileBasedWAL wal = new FileBasedWAL(tempDir.resolve("32k"), 1024 * 1024, (byte) 32)) {}
    try (FileBasedWAL wal = new FileBasedWAL(tempDir.resolve("64k"), 1024 * 1024, (byte) 64)) {}

    // Invalid page sizes should throw exception
    assertThrows(
        WALException.class,
        () -> new FileBasedWAL(tempDir.resolve("invalid1"), 1024 * 1024, (byte) 3));

    assertThrows(
        WALException.class,
        () -> new FileBasedWAL(tempDir.resolve("invalid2"), 1024 * 1024, (byte) 128));
  }

  @Test
  void testPageSizeMismatchValidation() throws Exception {
    // Create WAL with 8KB pages and add enough data to fill at least one page
    try (FileBasedWAL wal8k =
        new FileBasedWAL(tempDir.resolve("mismatch"), 1024 * 1024, (byte) 8)) {
      // Add enough data to ensure we have a complete 8KB page
      byte[] data = new byte[1000];
      for (int i = 0; i < 10; i++) { // 10KB total data
        wal8k.createAndAppend(ByteBuffer.wrap(data));
      }
      wal8k.sync(); // Ensure data is written to disk
    }

    // Try to open same directory with different page size - should fail
    WALException ex =
        assertThrows(
            WALException.class,
            () -> new FileBasedWAL(tempDir.resolve("mismatch"), 1024 * 1024, (byte) 16));

    // The page size mismatch error gets wrapped in a generic error
    assertEquals("WAL file error: page header could not be read", ex.getMessage());

    // But the cause should be the page size mismatch
    assertNotNull(ex.getCause());
    assertTrue(
        ex.getCause()
            .getMessage()
            .contains(
                "Page size mismatch: WAL file has 8KB pages, but FileBasedWAL configured for 16KB pages"));
  }

  @Test
  void test8KBPages() throws Exception {
    try (FileBasedWAL wal = new FileBasedWAL(tempDir, 1024 * 1024, (byte) 8)) {
      // 8KB = 8192 bytes, header = 45 bytes, data section = 8147 bytes
      // Entry overhead = 25 bytes, so max data = 8147 - 25 = 8122 bytes
      byte[] largeData = new byte[8122];
      for (int i = 0; i < largeData.length; i++) {
        largeData[i] = (byte) (i % 256);
      }

      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(largeData));
      assertEquals(0, entry.getSequenceNumber());

      // Verify data integrity
      List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
      assertEquals(1, entries.size());
      assertArrayEquals(largeData, entries.get(0).getDataAsBytes());
    }
  }

  @Test
  void test16KBPages() throws Exception {
    try (FileBasedWAL wal = new FileBasedWAL(tempDir, 1024 * 1024, (byte) 16)) {
      // 16KB = 16384 bytes, header = 45 bytes, data section = 16339 bytes
      // Entry overhead = 25 bytes, so max data = 16339 - 25 = 16314 bytes
      byte[] largeData = new byte[16314];
      for (int i = 0; i < largeData.length; i++) {
        largeData[i] = (byte) (i % 256);
      }

      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(largeData));
      assertEquals(0, entry.getSequenceNumber());

      // Verify data integrity
      List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
      assertEquals(1, entries.size());
      assertArrayEquals(largeData, entries.get(0).getDataAsBytes());
    }
  }

  @Test
  void test32KBPages() throws Exception {
    try (FileBasedWAL wal = new FileBasedWAL(tempDir, 1024 * 1024, (byte) 32)) {
      // 32KB = 32768 bytes, header = 45 bytes, data section = 32723 bytes
      // Entry overhead = 25 bytes, so max data = 32723 - 25 = 32698 bytes
      byte[] largeData = new byte[32698];
      for (int i = 0; i < largeData.length; i++) {
        largeData[i] = (byte) (i % 256);
      }

      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(largeData));
      assertEquals(0, entry.getSequenceNumber());

      // Verify data integrity
      List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
      assertEquals(1, entries.size());
      assertArrayEquals(largeData, entries.get(0).getDataAsBytes());
    }
  }

  @Test
  void test64KBPages() throws Exception {
    try (FileBasedWAL wal = new FileBasedWAL(tempDir, 1024 * 1024, (byte) 64)) {
      // 64KB = 65536 bytes, header = 45 bytes, data section = 65491 bytes
      // Entry overhead = 25 bytes, so max data = 65491 - 25 = 65466 bytes
      byte[] largeData = new byte[65466];
      for (int i = 0; i < largeData.length; i++) {
        largeData[i] = (byte) (i % 256);
      }

      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(largeData));
      assertEquals(0, entry.getSequenceNumber());

      // Verify data integrity
      List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
      assertEquals(1, entries.size());
      assertArrayEquals(largeData, entries.get(0).getDataAsBytes());
    }
  }

  @Test
  void testPageSizeStoredInHeader() throws Exception {
    // Test that page size is correctly stored and retrieved from header
    try (FileBasedWAL wal16k =
        new FileBasedWAL(tempDir.resolve("header-test"), 1024 * 1024, (byte) 16)) {
      wal16k.createAndAppend(ByteBuffer.wrap("test".getBytes()));
    }

    // Reopen and verify page size is preserved
    try (FileBasedWAL wal16k =
        new FileBasedWAL(tempDir.resolve("header-test"), 1024 * 1024, (byte) 16)) {
      List<WALEntry> entries = Flux.from(wal16k.readFrom(0L)).collectList().block();
      assertEquals(1, entries.size());
      assertEquals("test", new String(entries.get(0).getDataAsBytes()));
    }
  }

  @Test
  void testSpanningEntriesWithLargerPages() throws Exception {
    // Test that spanning works correctly with larger page sizes
    try (FileBasedWAL wal =
        new FileBasedWAL(tempDir, 16384, (byte) 8)) { // 16KB file limit, 8KB pages
      // Create entry larger than single page to force spanning
      byte[] spanningData = new byte[10000]; // 10KB data
      for (int i = 0; i < spanningData.length; i++) {
        spanningData[i] = (byte) (i % 256);
      }

      WALEntry entry = wal.createAndAppend(ByteBuffer.wrap(spanningData));
      assertEquals(0, entry.getSequenceNumber());

      // Verify spanning entry can be read back correctly
      List<WALEntry> entries = Flux.from(wal.readFrom(0L)).collectList().block();
      assertEquals(1, entries.size());
      assertArrayEquals(spanningData, entries.get(0).getDataAsBytes());
    }
  }

  @Test
  void testDefaultPageSizeIs8KB() throws Exception {
    // Create WAL with default constructor and write data
    try (FileBasedWAL defaultWal = new FileBasedWAL(tempDir.resolve("default"))) {
      defaultWal.createAndAppend(ByteBuffer.wrap("test data".getBytes()));
    }

    // Open same directory with explicit 8KB - should work if default is 8KB
    try (FileBasedWAL explicitWal =
        new FileBasedWAL(tempDir.resolve("default"), 64 * 1024 * 1024, (byte) 8)) {
      List<WALEntry> entries = Flux.from(explicitWal.readFrom(0L)).collectList().block();
      assertEquals(1, entries.size());
      assertEquals("test data", new String(entries.get(0).getDataAsBytes()));
    }

    // Verify it fails with different page size (should throw page size mismatch)
    assertThrows(
        WALException.class,
        () -> new FileBasedWAL(tempDir.resolve("default"), 64 * 1024 * 1024, (byte) 16));
  }
}

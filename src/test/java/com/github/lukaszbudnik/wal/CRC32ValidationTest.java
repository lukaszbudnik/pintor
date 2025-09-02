package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test CRC32 validation for both page headers and WAL entries. Verifies that corruption is properly
 * detected during read operations.
 */
class CRC32ValidationTest {

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
  void testValidCRC32PassesValidation() throws WALException {
    // Write some entries
    wal.createAndAppend(ByteBuffer.wrap("test data 1".getBytes()));
    wal.createAndAppend(ByteBuffer.wrap("test data 2".getBytes()));
    wal.sync();

    // Reading should work fine with valid CRC32
    List<WALEntry> entries = wal.readFrom(0L);
    assertEquals(2, entries.size());
    assertEquals("test data 1", new String(entries.get(0).getDataAsBytes()));
    assertEquals("test data 2", new String(entries.get(1).getDataAsBytes()));
  }

  @Test
  void testCorruptedEntryCRC32DetectedDuringRead() throws Exception {
    // Write an entry and sync to disk
    wal.createAndAppend(ByteBuffer.wrap("test data".getBytes()));
    wal.sync();
    wal.close();

    // Corrupt the entry CRC32 in the file
    Path walFile = tempDir.resolve("wal-0.log");
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "rw")) {
      // Skip page header (44 bytes) and entry header (21 bytes) and data (9 bytes)
      // CRC32 is at the end of the entry
      long crcPosition = WALPageHeader.HEADER_SIZE + 21 + 9;
      file.seek(crcPosition);
      file.writeInt(0xBADC0DE); // Write invalid CRC32
    }

    // Reopen WAL and try to read - should detect corruption and throw exception
    wal = new FileBasedWAL(tempDir);

    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              wal.readFrom(0L);
            });

    assertTrue(exception.getMessage().contains("Corrupted page"));
    assertTrue(exception.getCause().getMessage().contains("Entry CRC32 validation failed"));
  }

  @Test
  void testCorruptedPageHeaderCRC32DetectedDuringRead() throws Exception {
    // Write an entry and sync to disk
    wal.createAndAppend(ByteBuffer.wrap("test data".getBytes()));
    wal.sync();
    wal.close();

    // Corrupt the page header CRC32 in the file
    Path walFile = tempDir.resolve("wal-0.log");
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "rw")) {
      // Page header CRC32 is at offset 40 (last 4 bytes of 44-byte header)
      file.seek(40);
      file.writeInt(0xBADC0DE); // Write invalid CRC32
    }

    // Try to reopen WAL - should detect corruption during initialization/recovery
    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              new FileBasedWAL(tempDir);
            });

    assertTrue(exception.getMessage().contains("page header could not be read"));
    assertTrue(exception.getCause().getMessage().contains("Header CRC mismatch"));
  }

  @Test
  void testCorruptedEntryDataDetectedDuringRead() throws Exception {
    // Write an entry and sync to disk
    wal.createAndAppend(ByteBuffer.wrap("test data".getBytes()));
    wal.sync();
    wal.close();

    // Corrupt the entry data in the file
    Path walFile = tempDir.resolve("wal-0.log");
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "rw")) {
      // Skip page header (44 bytes) and entry header (21 bytes)
      // Corrupt the first byte of data
      long dataPosition = WALPageHeader.HEADER_SIZE + 21;
      file.seek(dataPosition);
      file.writeByte(0xFF); // Corrupt first byte of "test data"
    }

    // Reopen WAL and try to read - should detect corruption and throw exception
    wal = new FileBasedWAL(tempDir);

    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              wal.readFrom(0L);
            });

    assertTrue(exception.getMessage().contains("Corrupted page"));
    assertTrue(exception.getCause().getMessage().contains("Entry CRC32 validation failed"));
  }

  @Test
  void testCorruptedEntryHeaderDetectedDuringRead() throws Exception {
    // Write an entry and sync to disk
    wal.createAndAppend(ByteBuffer.wrap("test data".getBytes()));
    wal.sync();
    wal.close();

    // Corrupt the entry sequence number in the file
    Path walFile = tempDir.resolve("wal-0.log");
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "rw")) {
      // Skip page header (44 bytes) and entry type (1 byte)
      // Corrupt the sequence number (8 bytes)
      long sequencePosition = WALPageHeader.HEADER_SIZE + 1;
      file.seek(sequencePosition);
      file.writeLong(999L); // Change sequence from 0 to 999
    }

    // Reopen WAL and try to read - should detect corruption and throw exception
    wal = new FileBasedWAL(tempDir);

    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              wal.readFrom(0L);
            });

    assertTrue(exception.getMessage().contains("Corrupted page"));
    assertTrue(exception.getCause().getMessage().contains("Entry CRC32 validation failed"));
  }

  @Test
  void testMultipleEntriesWithOneCorrupted() throws Exception {
    // Write multiple entries to ensure they span multiple pages or at least test partial corruption
    wal.createAndAppend(ByteBuffer.wrap("entry 1".getBytes()));
    wal.sync(); // Force first entry to disk
    wal.createAndAppend(ByteBuffer.wrap("entry 2".getBytes()));
    wal.createAndAppend(ByteBuffer.wrap("entry 3".getBytes()));
    wal.sync();
    wal.close();

    // Verify all entries are readable before corruption
    wal = new FileBasedWAL(tempDir);
    List<WALEntry> allEntries = wal.readFrom(0L);
    assertEquals(3, allEntries.size());
    wal.close();

    // Corrupt the first page by corrupting the first entry's data
    Path walFile = tempDir.resolve("wal-0.log");
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "rw")) {
      // Corrupt first entry's data
      long dataPosition = WALPageHeader.HEADER_SIZE + 21; // After first entry header
      file.seek(dataPosition);
      file.writeByte(0xFF); // Corrupt first byte
    }

    // Reopen WAL and try to read - should fail on corrupted page
    wal = new FileBasedWAL(tempDir);

    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              wal.readFrom(0L);
            });

    assertTrue(exception.getMessage().contains("Corrupted page"));
    assertTrue(exception.getCause().getMessage().contains("Entry CRC32 validation failed"));
  }
}

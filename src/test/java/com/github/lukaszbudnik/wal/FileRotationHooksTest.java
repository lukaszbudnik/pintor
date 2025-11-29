package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRotationHooksTest {

  @TempDir Path tempDir;

  @Test
  void shouldCallRotationCallbackWhenFileRotates() throws Exception {
    // Given
    AtomicInteger callbackCount = new AtomicInteger(0);
    AtomicReference<Long> capturedFirstSeq = new AtomicReference<>();
    AtomicReference<Instant> capturedFirstTimestamp = new AtomicReference<>();
    AtomicReference<Long> capturedLastSeq = new AtomicReference<>();
    AtomicReference<Instant> capturedLastTimestamp = new AtomicReference<>();
    AtomicReference<Path> capturedFilePath = new AtomicReference<>();

    FileRotationCallback callback =
        (firstSeq, firstTimestamp, lastSeq, lastTimestamp, filePath) -> {
          callbackCount.incrementAndGet();
          capturedFirstSeq.set(firstSeq);
          capturedFirstTimestamp.set(firstTimestamp);
          capturedLastSeq.set(lastSeq);
          capturedLastTimestamp.set(lastTimestamp);
          capturedFilePath.set(filePath);
        };

    // Small file size to force rotation
    int smallFileSize = 1024; // 1KB

    try (FileBasedWAL wal = new FileBasedWAL(tempDir, smallFileSize, (byte) 4, callback)) {
      // When - write enough data to trigger file rotation
      ByteBuffer largeData = ByteBuffer.wrap(new byte[800]); // Large enough to force rotation
      wal.createAndAppend(largeData);
      wal.createAndAppend(largeData); // This should trigger rotation
    }

    // Then - assertions after WAL is closed to ensure callback was called
    assertEquals(1, callbackCount.get(), "Callback should be called once");
    assertNotNull(capturedFirstSeq.get(), "First sequence should be captured");
    assertNotNull(capturedFirstTimestamp.get(), "First timestamp should be captured");
    assertNotNull(capturedLastSeq.get(), "Last sequence should be captured");
    assertNotNull(capturedLastTimestamp.get(), "Last timestamp should be captured");
    assertNotNull(capturedFilePath.get(), "File path should be captured");
    assertTrue(capturedFilePath.get().toString().contains("wal-0.log"), "Should be first WAL file");
  }

  @Test
  void shouldNotCallCallbackWhenNoRotationOccurs() throws Exception {
    // Given
    AtomicInteger callbackCount = new AtomicInteger(0);
    FileRotationCallback callback =
        (firstSeq, firstTimestamp, lastSeq, lastTimestamp, filePath) -> {
          callbackCount.incrementAndGet();
        };

    try (FileBasedWAL wal = new FileBasedWAL(tempDir, 64 * 1024 * 1024, (byte) 4, callback)) {
      // When - write small amount of data (no rotation)
      ByteBuffer smallData = ByteBuffer.wrap("test".getBytes());
      wal.createAndAppend(smallData);

      // Then
      assertEquals(0, callbackCount.get(), "Callback should not be called without rotation");
    }
  }

  @Test
  void shouldWorkWithoutCallback() throws Exception {
    // Given - no callback provided
    try (FileBasedWAL wal = new FileBasedWAL(tempDir, 1024, (byte) 4, null)) {
      // When - write data that would trigger rotation
      ByteBuffer largeData = ByteBuffer.wrap(new byte[800]);
      wal.createAndAppend(largeData);
      wal.createAndAppend(largeData);

      // Then - should not throw exception
      assertTrue(wal.size() >= 2, "Entries should be written successfully");
    }
  }

  @Test
  void shouldCallCallbackMultipleTimesForMultipleRotations() throws Exception {
    // Given
    AtomicInteger callbackCount = new AtomicInteger(0);
    FileRotationCallback callback =
        (firstSeq, firstTimestamp, lastSeq, lastTimestamp, filePath) -> {
          callbackCount.incrementAndGet();
        };

    try (FileBasedWAL wal = new FileBasedWAL(tempDir, 1024, (byte) 4, callback)) {
      // When - write enough data to trigger multiple rotations
      // Each entry is ~425 bytes, page is 4KB, file limit is 1KB
      // So we need to write enough to exceed 1KB multiple times
      ByteBuffer data = ByteBuffer.wrap(new byte[600]); // Larger entries
      for (int i = 0; i < 10; i++) {
        wal.createAndAppend(data);
        // Force sync occasionally to trigger rotations during writes
        if (i % 3 == 0) {
          wal.sync();
        }
      }
    }

    // Then - assertion after WAL is closed
    assertTrue(
        callbackCount.get() >= 2,
        "Multiple rotations should trigger multiple callbacks, got: " + callbackCount.get());
  }
}

package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class WALPageHeaderTest {

  @Test
  void testPageHeaderCreationAndSerialization() throws WALException {
    Instant now = Instant.now();
    Instant later = now.plusSeconds(10).plusMillis(123);

    WALPageHeader header =
        new WALPageHeader(100L, 200L, now, later, (short) 5, WALPageHeader.NO_CONTINUATION);

    // Test getters
    assertEquals(100L, header.getFirstSequence());
    assertEquals(200L, header.getLastSequence());
    assertEquals(now.toEpochMilli(), header.getFirstTimestamp().toEpochMilli());
    assertEquals(later.toEpochMilli(), header.getLastTimestamp().toEpochMilli());
    assertEquals(5, header.getEntryCount());
    assertEquals(WALPageHeader.NO_CONTINUATION, header.getContinuationFlags());

    // Test serialization/deserialization
    byte[] serialized = header.serialize();
    assertEquals(WALPageHeader.HEADER_SIZE, serialized.length);
    assertEquals(44, serialized.length); // Verify correct header size

    WALPageHeader deserialized = WALPageHeader.deserialize(serialized);
    assertEquals(header.getFirstSequence(), deserialized.getFirstSequence());
    assertEquals(header.getLastSequence(), deserialized.getLastSequence());
    assertEquals(
        header.getFirstTimestamp().toEpochMilli(), deserialized.getFirstTimestamp().toEpochMilli());
    assertEquals(
        header.getLastTimestamp().toEpochMilli(), deserialized.getLastTimestamp().toEpochMilli());
    assertEquals(header.getEntryCount(), deserialized.getEntryCount());
    assertEquals(header.getContinuationFlags(), deserialized.getContinuationFlags());
  }

  @Test
  void testMillisecondPrecision() throws WALException {
    // Test that millisecond precision is preserved
    Instant precise = Instant.ofEpochMilli(1693123456987L);

    WALPageHeader header =
        new WALPageHeader(1L, 1L, precise, precise, (short) 1, WALPageHeader.NO_CONTINUATION);

    byte[] serialized = header.serialize();
    WALPageHeader deserialized = WALPageHeader.deserialize(serialized);

    assertEquals(precise.toEpochMilli(), deserialized.getFirstTimestamp().toEpochMilli());
    assertEquals(precise.toEpochMilli(), deserialized.getLastTimestamp().toEpochMilli());
    assertEquals(1693123456987L, deserialized.getFirstTimestamp().toEpochMilli());
  }

  @Test
  void testContinuationFlags() throws WALException {
    Instant now = Instant.now();

    // Test NO_CONTINUATION
    WALPageHeader noSpan =
        new WALPageHeader(1L, 1L, now, now, (short) 1, WALPageHeader.NO_CONTINUATION);
    assertFalse(noSpan.isSpanningRecord());
    assertFalse(noSpan.isFirstPart());
    assertFalse(noSpan.isMiddlePart());
    assertFalse(noSpan.isLastPart());

    // Test FIRST_PART
    WALPageHeader firstPart =
        new WALPageHeader(1L, 1L, now, now, (short) 1, WALPageHeader.FIRST_PART);
    assertTrue(firstPart.isSpanningRecord());
    assertTrue(firstPart.isFirstPart());
    assertFalse(firstPart.isMiddlePart());
    assertFalse(firstPart.isLastPart());

    // Test MIDDLE_PART
    WALPageHeader middlePart =
        new WALPageHeader(1L, 1L, now, now, (short) 0, WALPageHeader.MIDDLE_PART);
    assertTrue(middlePart.isSpanningRecord());
    assertFalse(middlePart.isFirstPart());
    assertTrue(middlePart.isMiddlePart());
    assertFalse(middlePart.isLastPart());

    // Test LAST_PART
    WALPageHeader lastPart =
        new WALPageHeader(1L, 1L, now, now, (short) 0, WALPageHeader.LAST_PART);
    assertTrue(lastPart.isSpanningRecord());
    assertFalse(lastPart.isFirstPart());
    assertFalse(lastPart.isMiddlePart());
    assertTrue(lastPart.isLastPart());
  }

  @Test
  void testInvalidMagicNumber() {
    byte[] invalidData = new byte[WALPageHeader.HEADER_SIZE];
    // Fill with invalid magic number (not 0xDEADBEEF)

    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              WALPageHeader.deserialize(invalidData);
            });
    assertTrue(exception.getMessage().contains("Invalid magic number"));
  }

  @Test
  void testInvalidHeaderSize() {
    byte[] invalidData = new byte[10]; // Too small

    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              WALPageHeader.deserialize(invalidData);
            });
    assertTrue(exception.getMessage().contains("Invalid header size"));
  }

  @Test
  void testHeaderCRCValidation() throws WALException {
    Instant now = Instant.now();
    WALPageHeader header =
        new WALPageHeader(1L, 1L, now, now, (short) 1, WALPageHeader.NO_CONTINUATION);

    byte[] serialized = header.serialize();

    // Corrupt the CRC (last 4 bytes)
    serialized[serialized.length - 1] = (byte) 0xFF;
    serialized[serialized.length - 2] = (byte) 0xFF;

    WALException exception =
        assertThrows(
            WALException.class,
            () -> {
              WALPageHeader.deserialize(serialized);
            });
    assertTrue(exception.getMessage().contains("Header CRC mismatch"));
  }

  @Test
  void testMagicNumberConstant() {
    assertEquals(0xDEADBEEF, WALPageHeader.MAGIC_NUMBER);
  }

  @Test
  void testHeaderSizeConstant() {
    assertEquals(44, WALPageHeader.HEADER_SIZE);
  }
}

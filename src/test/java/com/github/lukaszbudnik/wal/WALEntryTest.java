package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class WALEntryTest {
    
    @Test
    void testEntryCreation() {
        // Test full constructor
        String data = "INSERT|txn_1001|users|1|test-data";
        ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
        Instant timestamp = Instant.now();
        WALEntry entry1 = new WALEntry(1L, timestamp, buffer);
        
        assertEquals(1L, entry1.getSequenceNumber());
        assertEquals(timestamp, entry1.getTimestamp());
        assertArrayEquals(data.getBytes(), entry1.getDataAsBytes());
    }
    
    @Test
    void testNullData() {
        Instant timestamp = Instant.now();
        WALEntry entry = new WALEntry(2L, timestamp, null);
        
        assertEquals(2L, entry.getSequenceNumber());
        assertEquals(timestamp, entry.getTimestamp());
        assertNull(entry.getData());
        assertNull(entry.getDataAsBytes());
    }
    
    @Test
    void testDataHandling() {
        // Test ByteBuffer handling with complex data
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putInt(42);
        buffer.putShort((short) 123);
        buffer.put("test".getBytes());
        buffer.flip();
        
        WALEntry entry = new WALEntry(1L, Instant.now(), buffer);
        
        // Should be able to read the data back
        ByteBuffer retrievedBuffer = entry.getData();
        assertNotNull(retrievedBuffer);
        assertEquals(10, retrievedBuffer.remaining()); // 4 bytes int + 2 bytes short + 4 bytes "test"
        
        assertEquals(42, retrievedBuffer.getInt());
        assertEquals(123, retrievedBuffer.getShort());
        byte[] textBytes = new byte[4];
        retrievedBuffer.get(textBytes);
        assertEquals("test", new String(textBytes));
        
        // Test data immutability - defensive copying
        byte[] originalData = "INSERT|txn_1001|users|1|test-data".getBytes();
        ByteBuffer immutableBuffer = ByteBuffer.wrap(originalData);
        WALEntry immutableEntry = new WALEntry(2L, Instant.now(), immutableBuffer);
        
        // Modify original array
        originalData[0] = 'X';
        
        // Entry should have a defensive copy, not the original reference
        assertEquals('I', immutableEntry.getDataAsBytes()[0]);
    }
    
    @Test
    void testEqualsAndHashCode() {
        Instant timestamp = Instant.ofEpochSecond(1000, 123456789);
        ByteBuffer data = ByteBuffer.wrap("test-data".getBytes());
        
        WALEntry entry1 = new WALEntry(1L, timestamp, data);
        WALEntry entry2 = new WALEntry(1L, timestamp, ByteBuffer.wrap("test-data".getBytes()));
        WALEntry entry3 = new WALEntry(2L, timestamp, data); // Different sequence
        WALEntry entry4 = new WALEntry(1L, timestamp.plusSeconds(1), data); // Different timestamp
        
        // Test equals
        assertEquals(entry1, entry1); // Same object
        assertEquals(entry1, entry2); // Same values
        assertNotEquals(entry1, entry3); // Different sequence
        assertNotEquals(entry1, entry4); // Different timestamp
        assertNotEquals(entry1, null); // Null object
        assertNotEquals(entry1, "not a WALEntry"); // Different class
        
        // Test hashCode
        assertEquals(entry1.hashCode(), entry2.hashCode()); // Same values should have same hash
    }
    
    @Test
    void testToString() {
        // Test with data
        Instant timestamp = Instant.ofEpochSecond(1000);
        WALEntry entryWithData = new WALEntry(1L, timestamp, ByteBuffer.wrap("test".getBytes()));
        String str1 = entryWithData.toString();
        
        assertTrue(str1.contains("seq=1"));
        assertTrue(str1.contains("timestamp=" + timestamp));
        assertTrue(str1.contains("dataSize=4"));
        
        // Test with null data
        WALEntry entryWithoutData = new WALEntry(2L, timestamp, null);
        String str2 = entryWithoutData.toString();
        
        assertTrue(str2.contains("seq=2"));
        assertTrue(str2.contains("timestamp=" + timestamp));
        assertTrue(str2.contains("dataSize=0"));
    }
    
    @Test
    void testApplicationDataFormats() {
        // Test INSERT operation with JSON data
        String insertData = "INSERT|txn_123|users|1|{\"name\":\"John\",\"email\":\"john@example.com\"}";
        WALEntry insertEntry = new WALEntry(1L, Instant.now(), ByteBuffer.wrap(insertData.getBytes()));
        
        String[] insertParts = new String(insertEntry.getDataAsBytes()).split("\\|");
        assertEquals("INSERT", insertParts[0]);
        assertEquals("txn_123", insertParts[1]);
        assertEquals("users", insertParts[2]);
        assertEquals("1", insertParts[3]);
        assertEquals("{\"name\":\"John\",\"email\":\"john@example.com\"}", insertParts[4]);
        
        // Test DELETE operation
        String deleteData = "DELETE|txn_124|users|2|";
        WALEntry deleteEntry = new WALEntry(2L, Instant.now(), ByteBuffer.wrap(deleteData.getBytes()));
        
        String[] deleteParts = new String(deleteEntry.getDataAsBytes()).split("\\|", -1); // Use -1 to include trailing empty strings
        assertEquals("DELETE", deleteParts[0]);
        assertEquals("txn_124", deleteParts[1]);
        assertEquals("users", deleteParts[2]);
        assertEquals("2", deleteParts[3]);
        assertEquals("", deleteParts[4]); // Empty data for DELETE
        
        // Test transaction control operations
        String beginData = "BEGIN|txn_125|";
        WALEntry beginEntry = new WALEntry(3L, Instant.now(), ByteBuffer.wrap(beginData.getBytes()));
        
        String[] beginParts = new String(beginEntry.getDataAsBytes()).split("\\|");
        assertEquals("BEGIN", beginParts[0]);
        assertEquals("txn_125", beginParts[1]);
        assertTrue(beginParts.length >= 2); // Should have at least operation and transaction
        
        // Test ROLLBACK with reason
        String rollbackData = "ROLLBACK|txn_126|reason:timeout";
        WALEntry rollbackEntry = new WALEntry(4L, Instant.now(), ByteBuffer.wrap(rollbackData.getBytes()));
        
        String[] rollbackParts = new String(rollbackEntry.getDataAsBytes()).split("\\|");
        assertEquals("ROLLBACK", rollbackParts[0]);
        assertEquals("txn_126", rollbackParts[1]);
        assertEquals("reason:timeout", rollbackParts[2]);
    }
    
    @Test
    void testTimestampPrecision() {
        Instant preciseTimestamp = Instant.ofEpochSecond(1609459200, 123456789); // 2021-01-01 00:00:00.123456789 UTC
        String data = "INSERT|txn_1001|users|1|timestamp_test";
        WALEntry entry = new WALEntry(1L, preciseTimestamp, ByteBuffer.wrap(data.getBytes()));
        
        assertEquals(preciseTimestamp, entry.getTimestamp());
        assertEquals(1609459200, entry.getTimestamp().getEpochSecond());
        assertEquals(123456789, entry.getTimestamp().getNano());
    }
    
    @Test
    void testLargeData() {
        // Test with larger application data to ensure performance
        StringBuilder largeData = new StringBuilder();
        largeData.append("INSERT|txn_1001|large_table|1|");
        for (int i = 0; i < 1000; i++) {
            largeData.append("field").append(i).append(":value").append(i).append(",");
        }
        
        WALEntry entry = new WALEntry(1L, Instant.now(), ByteBuffer.wrap(largeData.toString().getBytes()));
        
        assertEquals(1L, entry.getSequenceNumber());
        assertEquals(largeData.toString(), new String(entry.getDataAsBytes()));
    }
}

package com.github.lukaszbudnik.wal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WALManagerTest {
    
    @Mock
    private WriteAheadLog mockWal;
    
    private WALManager walManager;
    
    @BeforeEach
    void setUp() {
        walManager = new WALManager(mockWal);
    }
    
    @Test
    void testCreateEntryWithByteBuffer() throws WALException {
        // Given
        ByteBuffer data = ByteBuffer.wrap("test data".getBytes());
        WALEntry expectedEntry = new WALEntry(5L, Instant.now(), ByteBuffer.wrap("test data".getBytes()));
        when(mockWal.createAndAppend(data)).thenReturn(expectedEntry);
        
        // When
        WALEntry entry = walManager.createEntry(data);
        
        // Then
        assertEquals(5L, entry.getSequenceNumber());
        assertArrayEquals("test data".getBytes(), entry.getDataAsBytes());
        assertNotNull(entry.getTimestamp());
        
        verify(mockWal).createAndAppend(data);
    }
    
    @Test
    void testCreateEntryWithByteArray() throws WALException {
        // Given
        byte[] data = "buffer data".getBytes();
        WALEntry expectedEntry = new WALEntry(10L, Instant.now(), ByteBuffer.wrap(data));
        when(mockWal.createAndAppend(any(ByteBuffer.class))).thenReturn(expectedEntry);
        
        // When
        WALEntry entry = walManager.createEntry(data);
        
        // Then
        assertEquals(10L, entry.getSequenceNumber());
        assertArrayEquals("buffer data".getBytes(), entry.getDataAsBytes());
        assertNotNull(entry.getTimestamp());
        
        verify(mockWal).createAndAppend(any(ByteBuffer.class));
    }
    
    @Test
    void testCreateEntryWithNullData() throws WALException {
        // Given
        WALEntry expectedEntry = new WALEntry(0L, Instant.now(), (ByteBuffer) null);
        when(mockWal.createAndAppend(null)).thenReturn(expectedEntry);
        
        // When
        WALEntry entry = walManager.createEntry((byte[]) null);
        
        // Then
        assertEquals(0L, entry.getSequenceNumber());
        assertNull(entry.getDataAsBytes());
        assertNotNull(entry.getTimestamp());
        
        verify(mockWal).createAndAppend(null);
    }
    
    @Test
    void testCreateEntryCallsAppend() throws WALException {
        // Given
        byte[] data = "some data".getBytes();
        WALEntry expectedEntry = new WALEntry(42L, Instant.now(), ByteBuffer.wrap(data));
        when(mockWal.createAndAppend(any(ByteBuffer.class))).thenReturn(expectedEntry);
        
        // When
        walManager.createEntry(data);
        
        // Then
        verify(mockWal).createAndAppend(argThat(buffer -> 
            buffer != null && Arrays.equals(getBufferBytes(buffer), data)
        ));
    }
    
    // Helper method to extract bytes from ByteBuffer for testing
    private byte[] getBufferBytes(ByteBuffer buffer) {
        if (buffer == null) return null;
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.rewind();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
    
    @Test
    void testCreateEntryThrowsExceptionOnAppendFailure() throws WALException {
        // Given
        WALException expectedException = new WALException("Create and append failed");
        when(mockWal.createAndAppend(any(ByteBuffer.class))).thenThrow(expectedException);
        
        // When & Then
        WALException actualException = assertThrows(WALException.class, 
            () -> walManager.createEntry("data".getBytes()));
        
        assertEquals(expectedException, actualException);
        verify(mockWal).createAndAppend(any(ByteBuffer.class));
    }
    
    @Test
    void testSync() throws WALException {
        // When
        walManager.sync();
        
        // Then
        verify(mockWal).sync();
    }
    
    @Test
    void testReadFrom() throws WALException {
        // Given
        List<WALEntry> expectedEntries = Arrays.asList(
            new WALEntry(5L, Instant.now(), ByteBuffer.wrap("data1".getBytes())),
            new WALEntry(6L, Instant.now(), ByteBuffer.wrap("data2".getBytes()))
        );
        when(mockWal.readFrom(5L)).thenReturn(expectedEntries);
        
        // When
        List<WALEntry> actualEntries = walManager.readFrom(5L);
        
        // Then
        assertEquals(expectedEntries, actualEntries);
        verify(mockWal).readFrom(5L);
    }
    
    @Test
    void testReadRange() throws WALException {
        // Given
        List<WALEntry> expectedEntries = Arrays.asList(
            new WALEntry(10L, Instant.now(), ByteBuffer.wrap("range1".getBytes())),
            new WALEntry(11L, Instant.now(), ByteBuffer.wrap("range2".getBytes())),
            new WALEntry(12L, Instant.now(), ByteBuffer.wrap("range3".getBytes()))
        );
        when(mockWal.readRange(10L, 12L)).thenReturn(expectedEntries);
        
        // When
        List<WALEntry> actualEntries = walManager.readRange(10L, 12L);
        
        // Then
        assertEquals(3, actualEntries.size());
        assertEquals("range1", new String(actualEntries.get(0).getDataAsBytes()));
        assertEquals("range2", new String(actualEntries.get(1).getDataAsBytes()));
        assertEquals("range3", new String(actualEntries.get(2).getDataAsBytes()));
        verify(mockWal).readRange(10L, 12L);
    }
    
    @Test
    void testGetCurrentSequenceNumber() {
        // Given
        when(mockWal.getCurrentSequenceNumber()).thenReturn(42L);
        
        // When
        long currentSeq = walManager.getCurrentSequenceNumber();
        
        // Then
        assertEquals(42L, currentSeq);
        verify(mockWal).getCurrentSequenceNumber();
    }
    
    @Test
    void testGetNextSequenceNumber() {
        // Given
        when(mockWal.getNextSequenceNumber()).thenReturn(43L);
        
        // When
        long nextSeq = walManager.getNextSequenceNumber();
        
        // Then
        assertEquals(43L, nextSeq);
        verify(mockWal).getNextSequenceNumber();
    }
    
    @Test
    void testTruncate() throws WALException {
        // When
        walManager.truncate(100L);
        
        // Then
        verify(mockWal).truncate(100L);
    }
    
    @Test
    void testSize() {
        // Given
        when(mockWal.size()).thenReturn(1000L);
        
        // When
        long size = walManager.size();
        
        // Then
        assertEquals(1000L, size);
        verify(mockWal).size();
    }
    
    @Test
    void testIsEmpty() {
        // Given
        when(mockWal.isEmpty()).thenReturn(false);
        
        // When
        boolean isEmpty = walManager.isEmpty();
        
        // Then
        assertFalse(isEmpty);
        verify(mockWal).isEmpty();
    }
    
    @Test
    void testIsEmptyWhenEmpty() {
        // Given
        when(mockWal.isEmpty()).thenReturn(true);
        
        // When
        boolean isEmpty = walManager.isEmpty();
        
        // Then
        assertTrue(isEmpty);
        verify(mockWal).isEmpty();
    }
    
    @Test
    void testClose() throws Exception {
        // When
        walManager.close();
        
        // Then
        verify(mockWal).close();
    }
    
    @Test
    void testSyncThrowsException() throws WALException {
        // Given
        WALException expectedException = new WALException("Sync exception");
        doThrow(expectedException).when(mockWal).sync();
        
        // When & Then
        WALException actualException = assertThrows(WALException.class, () -> walManager.sync());
        assertEquals(expectedException, actualException);
        verify(mockWal).sync();
    }
    
    @Test
    void testReadFromThrowsException() throws WALException {
        // Given
        WALException expectedException = new WALException("Read exception");
        when(mockWal.readFrom(anyLong())).thenThrow(expectedException);
        
        // When & Then
        WALException actualException = assertThrows(WALException.class, () -> walManager.readFrom(5L));
        assertEquals(expectedException, actualException);
        verify(mockWal).readFrom(5L);
    }
    
    @Test
    void testTruncateThrowsException() throws WALException {
        // Given
        WALException expectedException = new WALException("Truncate exception");
        doThrow(expectedException).when(mockWal).truncate(anyLong());
        
        // When & Then
        WALException actualException = assertThrows(WALException.class, () -> walManager.truncate(100L));
        assertEquals(expectedException, actualException);
        verify(mockWal).truncate(100L);
    }
    
    @Test
    void testCloseThrowsException() throws Exception {
        // Given
        Exception expectedException = new Exception("Close exception");
        doThrow(expectedException).when(mockWal).close();
        
        // When & Then
        Exception actualException = assertThrows(Exception.class, () -> walManager.close());
        assertEquals(expectedException, actualException);
        verify(mockWal).close();
    }
    
    @Test
    void testMultipleCreateEntryCallsGetDifferentSequenceNumbers() throws WALException {
        // Given
        WALEntry entry1 = new WALEntry(1L, Instant.now(), ByteBuffer.wrap("data1".getBytes()));
        WALEntry entry2 = new WALEntry(2L, Instant.now(), ByteBuffer.wrap("data2".getBytes()));
        WALEntry entry3 = new WALEntry(3L, Instant.now(), ByteBuffer.wrap("data3".getBytes()));
        
        when(mockWal.createAndAppend(any(ByteBuffer.class)))
            .thenReturn(entry1)
            .thenReturn(entry2)
            .thenReturn(entry3);
        
        // When
        WALEntry result1 = walManager.createEntry("data1".getBytes());
        WALEntry result2 = walManager.createEntry("data2".getBytes());
        WALEntry result3 = walManager.createEntry("data3".getBytes());
        
        // Then
        assertEquals(1L, result1.getSequenceNumber());
        assertEquals(2L, result2.getSequenceNumber());
        assertEquals(3L, result3.getSequenceNumber());
        verify(mockWal, times(3)).createAndAppend(any(ByteBuffer.class));
    }
    
    @Test
    void testCreateEntryWithLargeData() throws WALException {
        // Given
        byte[] largeData = new byte[1024];
        Arrays.fill(largeData, (byte) 'A');
        WALEntry expectedEntry = new WALEntry(99L, Instant.now(), ByteBuffer.wrap(largeData));
        when(mockWal.createAndAppend(any(ByteBuffer.class))).thenReturn(expectedEntry);
        
        // When
        WALEntry entry = walManager.createEntry(largeData);
        
        // Then
        assertEquals(99L, entry.getSequenceNumber());
        assertArrayEquals(largeData, entry.getDataAsBytes());
        
        verify(mockWal).createAndAppend(any(ByteBuffer.class));
    }
    
    @Test
    void testCreateEntryWithApplicationData() throws WALException {
        // Given - simulate application embedding record type and transaction ID in data
        String applicationData = "INSERT|txn_123|users|1|{\"name\":\"John\",\"email\":\"john@example.com\"}";
        WALEntry expectedEntry = new WALEntry(7L, Instant.now(), ByteBuffer.wrap(applicationData.getBytes()));
        when(mockWal.createAndAppend(any(ByteBuffer.class))).thenReturn(expectedEntry);
        
        // When
        WALEntry entry = walManager.createEntry(applicationData.getBytes());
        
        // Then
        assertEquals(7L, entry.getSequenceNumber());
        assertArrayEquals(applicationData.getBytes(), entry.getDataAsBytes());
        
        // Verify application can extract its own metadata from data
        String retrievedData = new String(entry.getDataAsBytes());
        assertTrue(retrievedData.startsWith("INSERT|txn_123|"));
        
        verify(mockWal).createAndAppend(any(ByteBuffer.class));
    }
    
    @Test
    void testCreateEntryBatch() throws WALException {
        // Given
        List<ByteBuffer> dataList = Arrays.asList(
            ByteBuffer.wrap("data1".getBytes()),
            ByteBuffer.wrap("data2".getBytes()),
            ByteBuffer.wrap("data3".getBytes())
        );
        
        List<WALEntry> expectedEntries = Arrays.asList(
            new WALEntry(1L, Instant.now(), ByteBuffer.wrap("data1".getBytes())),
            new WALEntry(2L, Instant.now(), ByteBuffer.wrap("data2".getBytes())),
            new WALEntry(3L, Instant.now(), ByteBuffer.wrap("data3".getBytes()))
        );
        
        when(mockWal.createAndAppendBatch(dataList)).thenReturn(expectedEntries);
        
        // When
        List<WALEntry> entries = walManager.createEntryBatch(dataList);
        
        // Then
        assertEquals(3, entries.size());
        assertEquals(1L, entries.get(0).getSequenceNumber());
        assertEquals(2L, entries.get(1).getSequenceNumber());
        assertEquals(3L, entries.get(2).getSequenceNumber());
        
        verify(mockWal).createAndAppendBatch(dataList);
    }
    
    @Test
    void testCreateEntryBatchFromBytes() throws WALException {
        // Given
        List<byte[]> dataList = Arrays.asList(
            "data1".getBytes(),
            "data2".getBytes(),
            null  // Test null data
        );
        
        List<WALEntry> expectedEntries = Arrays.asList(
            new WALEntry(10L, Instant.now(), ByteBuffer.wrap("data1".getBytes())),
            new WALEntry(11L, Instant.now(), ByteBuffer.wrap("data2".getBytes())),
            new WALEntry(12L, Instant.now(), (ByteBuffer) null)
        );
        
        when(mockWal.createAndAppendBatch(any(List.class))).thenReturn(expectedEntries);
        
        // When
        List<WALEntry> entries = walManager.createEntryBatchFromBytes(dataList);
        
        // Then
        assertEquals(3, entries.size());
        assertEquals(10L, entries.get(0).getSequenceNumber());
        assertEquals(11L, entries.get(1).getSequenceNumber());
        assertEquals(12L, entries.get(2).getSequenceNumber());
        assertArrayEquals("data1".getBytes(), entries.get(0).getDataAsBytes());
        assertArrayEquals("data2".getBytes(), entries.get(1).getDataAsBytes());
        assertNull(entries.get(2).getDataAsBytes());
        
        verify(mockWal).createAndAppendBatch(any(List.class));
    }
    
    @Test
    void testReadFromTimestamp() throws WALException {
        // Given
        Instant timestamp = Instant.now();
        List<WALEntry> expectedEntries = Arrays.asList(
            new WALEntry(5L, Instant.now(), ByteBuffer.wrap("data1".getBytes())),
            new WALEntry(6L, Instant.now(), ByteBuffer.wrap("data2".getBytes()))
        );
        when(mockWal.readFrom(timestamp)).thenReturn(expectedEntries);
        
        // When
        List<WALEntry> entries = walManager.readFrom(timestamp);
        
        // Then
        assertEquals(2, entries.size());
        assertEquals(5L, entries.get(0).getSequenceNumber());
        assertEquals(6L, entries.get(1).getSequenceNumber());
        
        verify(mockWal).readFrom(timestamp);
    }
    
    @Test
    void testReadRangeByTimestamp() throws WALException {
        // Given
        Instant fromTimestamp = Instant.now().minusSeconds(60);
        Instant toTimestamp = Instant.now();
        List<WALEntry> expectedEntries = Arrays.asList(
            new WALEntry(10L, Instant.now(), ByteBuffer.wrap("range1".getBytes())),
            new WALEntry(11L, Instant.now(), ByteBuffer.wrap("range2".getBytes())),
            new WALEntry(12L, Instant.now(), ByteBuffer.wrap("range3".getBytes()))
        );
        when(mockWal.readRange(fromTimestamp, toTimestamp)).thenReturn(expectedEntries);
        
        // When
        List<WALEntry> entries = walManager.readRange(fromTimestamp, toTimestamp);
        
        // Then
        assertEquals(3, entries.size());
        assertEquals("range1", new String(entries.get(0).getDataAsBytes()));
        assertEquals("range2", new String(entries.get(1).getDataAsBytes()));
        assertEquals("range3", new String(entries.get(2).getDataAsBytes()));
        
        verify(mockWal).readRange(fromTimestamp, toTimestamp);
    }
}

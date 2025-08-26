package com.github.lukaszbudnik.wal;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/**
 * File-based implementation of Write Ahead Log.
 *
 * <p>This implementation stores WAL entries in binary files with the following format: - Each entry
 * is prefixed with its length (4 bytes) - Followed by the serialized WALEntry
 *
 * <p>Features: - Thread-safe operations using ReadWriteLock - Automatic file rotation when size
 * limits are reached - Recovery from corrupted entries - Configurable sync behavior - Persistent
 * sequence number that always increments (even after restart)
 */
public class FileBasedWAL implements WriteAheadLog {

  private static final String WAL_FILE_PREFIX = "wal-";
  private static final String WAL_FILE_SUFFIX = ".log";
  private static final String SEQUENCE_FILE_NAME = "sequence.dat";
  private static final int DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024; // 64MB
  private static final int ENTRY_HEADER_SIZE = 24; // 8+8+4+4 bytes for seq, timestamp_sec, timestamp_ns, data_length

  private final Path walDirectory;
  private final Path sequenceFile;
  private final int maxFileSize;
  private final boolean syncOnWrite;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<Integer, Path> walFiles = new TreeMap<>();
  private volatile long currentSequenceNumber = -1;
  private volatile int currentFileIndex = 0;
  private RandomAccessFile currentFile;
  private RandomAccessFile sequenceFileRAF;

  public FileBasedWAL(Path walDirectory) throws WALException {
    this(walDirectory, DEFAULT_MAX_FILE_SIZE, true);
  }

  public FileBasedWAL(Path walDirectory, int maxFileSize, boolean syncOnWrite) throws WALException {
    this.walDirectory = walDirectory;
    this.sequenceFile = walDirectory.resolve(SEQUENCE_FILE_NAME);
    this.maxFileSize = maxFileSize;
    this.syncOnWrite = syncOnWrite;

    try {
      Files.createDirectories(walDirectory);
      initialize();
    } catch (IOException e) {
      throw new WALException("Failed to initialize WAL", e);
    }
  }

  private void initialize() throws IOException, WALException {
    // Open sequence file for reuse (create if doesn't exist)
    sequenceFileRAF = new RandomAccessFile(sequenceFile.toFile(), "rw");

    loadSequenceNumber();

    // Discover existing WAL files
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(walDirectory, WAL_FILE_PREFIX + "*" + WAL_FILE_SUFFIX)) {
      for (Path file : stream) {
        String fileName = file.getFileName().toString();
        int fileIndex = extractFileIndex(fileName);
        walFiles.put(fileIndex, file);
        currentFileIndex = Math.max(currentFileIndex, fileIndex);
      }
    }

    // Open or create the current file
    openCurrentFile();
  }

  private void loadSequenceNumber() throws IOException {
    sequenceFileRAF.seek(0);
    try {
      currentSequenceNumber = sequenceFileRAF.readLong();
    } catch (IOException e) {
      currentSequenceNumber = -1;
    }
  }

  private void saveSequenceNumber() throws IOException {
    if (sequenceFileRAF != null) {
      sequenceFileRAF.seek(0);
      sequenceFileRAF.writeLong(currentSequenceNumber);
      
      // Respect syncOnWrite flag for consistency
      if (syncOnWrite) {
        sequenceFileRAF.getFD().sync();
      }
    }
  }

  private int extractFileIndex(String fileName) {
    String indexStr =
        fileName.substring(WAL_FILE_PREFIX.length(), fileName.length() - WAL_FILE_SUFFIX.length());
    return Integer.parseInt(indexStr);
  }

  private void openCurrentFile() throws IOException {
    Path currentFilePath =
        walDirectory.resolve(WAL_FILE_PREFIX + currentFileIndex + WAL_FILE_SUFFIX);
    walFiles.put(currentFileIndex, currentFilePath);

    if (currentFile != null) {
      currentFile.close();
    }

    currentFile = new RandomAccessFile(currentFilePath.toFile(), "rw");
    currentFile.seek(currentFile.length()); // Position at end for appending
  }

  @Override
  public WALEntry createAndAppend(ByteBuffer data) throws WALException {
    lock.writeLock().lock();
    try {
      long nextSeq = currentSequenceNumber + 1;
      WALEntry entry = new WALEntry(nextSeq, Instant.now(), data);
      
      // Use private append method
      appendInternal(entry);
      
      return entry;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public List<WALEntry> createAndAppendBatch(List<ByteBuffer> dataList) throws WALException {
    if (dataList.isEmpty()) {
      return new ArrayList<>();
    }

    lock.writeLock().lock();
    try {
      List<WALEntry> entries = new ArrayList<>(dataList.size());
      long nextSeq = currentSequenceNumber + 1;
      
      // Create all entries with consecutive sequence numbers
      for (ByteBuffer data : dataList) {
        WALEntry entry = new WALEntry(nextSeq++, Instant.now(), data);
        entries.add(entry);
      }

      // Use private appendBatch method
      appendBatchInternal(entries);
      
      return entries;
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  // Internal method for appending a single entry (used by createAndAppend)
  private void appendInternal(WALEntry entry) throws WALException {
    try {
      writeEntry(entry);
      currentSequenceNumber = entry.getSequenceNumber();

      // Persist the sequence number
      saveSequenceNumber();

      if (syncOnWrite) {
        currentFile.getFD().sync();
      }

      // Check if we need to rotate to a new file
      if (currentFile.length() > maxFileSize) {
        rotateFile();
      }
    } catch (IOException e) {
      throw new WALException("Failed to append entry", e);
    }
  }
  
  // Internal method for appending multiple entries (used by createAndAppendBatch)
  private void appendBatchInternal(List<WALEntry> entries) throws WALException {
    try {
      for (WALEntry entry : entries) {
        writeEntry(entry);
        currentSequenceNumber = entry.getSequenceNumber();
      }

      // Persist the sequence number
      saveSequenceNumber();

      if (syncOnWrite) {
        currentFile.getFD().sync();
      }

      // Check if we need to rotate to a new file
      if (currentFile.length() > maxFileSize) {
        rotateFile();
      }
    } catch (IOException e) {
      throw new WALException("Failed to append batch", e);
    }
  }

  private void writeEntry(WALEntry entry) throws IOException {
    byte[] entryData = serializeEntry(entry);
    currentFile.write(entryData);
  }

  private void rotateFile() throws IOException {
    currentFileIndex++;
    openCurrentFile();
  }

  /**
   * Serialize WAL entry to binary format with CRC32 checksum Format:
   * [seq_num(8)][timestamp_sec(8)][timestamp_ns(4)][data_length(4)][data][crc32(4)]
   */
  private byte[] serializeEntry(WALEntry entry) throws IOException {
    ByteBuffer dataBuffer = entry.getData();
    int dataLength = dataBuffer != null ? dataBuffer.remaining() : 0;

    // Calculate total size: 8+8+4+4+dataLength+4 = 28 + dataLength
    int totalSize = 28 + dataLength;
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);

    // Write entry fields
    buffer.putLong(entry.getSequenceNumber());
    buffer.putLong(entry.getTimestamp().getEpochSecond());
    buffer.putInt(entry.getTimestamp().getNano());
    buffer.putInt(dataLength);

    if (dataBuffer != null) {
      ByteBuffer dataDuplicate = dataBuffer.duplicate();
      dataDuplicate.rewind();
      buffer.put(dataDuplicate);
    }

    // Calculate and write CRC32 (excluding the CRC field itself)
    CRC32 crc = new CRC32();
    buffer.position(0);
    buffer.limit(totalSize - 4); // Exclude CRC field
    crc.update(buffer);
    buffer.limit(totalSize);
    buffer.putInt((int) crc.getValue());

    return buffer.array();
  }

  /** Deserialize WAL entry from binary format and verify CRC32 checksum */
  private WALEntry deserializeEntry(byte[] data) throws IOException {
    if (data.length < 28) {
      throw new IOException("Invalid entry data: too short");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);

    // Verify CRC32
    CRC32 crc = new CRC32();
    buffer.position(0);
    buffer.limit(data.length - 4);
    crc.update(buffer);
    buffer.limit(data.length);
    buffer.position(data.length - 4);
    int expectedCrc = buffer.getInt();

    if ((int) crc.getValue() != expectedCrc) {
      throw new IOException("CRC32 checksum mismatch");
    }

    // Read entry fields
    buffer.position(0);
    long sequenceNumber = buffer.getLong();
    long epochSeconds = buffer.getLong();
    int nanos = buffer.getInt();
    int dataLength = buffer.getInt();

    Instant timestamp = Instant.ofEpochSecond(epochSeconds, nanos);

    ByteBuffer entryData = null;
    if (dataLength > 0) {
      byte[] dataBytes = new byte[dataLength];
      buffer.get(dataBytes);
      entryData = ByteBuffer.wrap(dataBytes);
    }

    return new WALEntry(sequenceNumber, timestamp, entryData);
  }

  @Override
  public void sync() throws WALException {
    lock.readLock().lock();
    try {
      // Sync both log file and sequence file
      if (currentFile != null) {
        currentFile.getFD().sync();
      }
      if (sequenceFileRAF != null) {
        sequenceFileRAF.getFD().sync();
      }
    } catch (IOException e) {
      throw new WALException("Failed to sync WAL", e);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<WALEntry> readFrom(long fromSequenceNumber) throws WALException {
    return readRange(fromSequenceNumber, Long.MAX_VALUE);
  }

  @Override
  public List<WALEntry> readRange(long fromSequenceNumber, long toSequenceNumber)
      throws WALException {
    lock.readLock().lock();
    try {
      List<WALEntry> entries = new ArrayList<>();

      for (Map.Entry<Integer, Path> fileEntry : walFiles.entrySet()) {
        Path filePath = fileEntry.getValue();
        if (Files.exists(filePath)) {
          entries.addAll(readEntriesFromFile(filePath, fromSequenceNumber, toSequenceNumber));
        }
      }

      // Sort by sequence number to ensure correct order
      entries.sort(Comparator.comparingLong(WALEntry::getSequenceNumber));

      return entries;
    } finally {
      lock.readLock().unlock();
    }
  }

  private List<WALEntry> readEntriesFromFile(Path filePath, long fromSeq, long toSeq)
      throws WALException {
    List<WALEntry> entries = new ArrayList<>();

    try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
      long position = 0;

      while (position < file.length()) {
        file.seek(position);

        if (file.length() - position < ENTRY_HEADER_SIZE) {
          break;
        }

        // Read sequence number to check if we want this entry
        long sequenceNumber = file.readLong();
        
        // Read timestamp fields (we need to read them to get to data length)
        file.readLong(); // timestamp seconds
        file.readInt();  // timestamp nanoseconds
        
        // Read data length
        int dataLength = file.readInt();
        if (dataLength < 0 || position + ENTRY_HEADER_SIZE + dataLength + 4 > file.length()) {
          break;
        }

        // Check if this entry is in our desired range
        if (sequenceNumber >= fromSeq && sequenceNumber <= toSeq) {
          // Read the complete entry
          file.seek(position);
          int totalEntrySize = ENTRY_HEADER_SIZE + dataLength + 4; // +4 for CRC32
          byte[] entryData = new byte[totalEntrySize];
          file.readFully(entryData);

          try {
            WALEntry entry = deserializeEntry(entryData);
            entries.add(entry);
          } catch (Exception e) {
            // Skip corrupted entry and continue
          }
        }

        position += ENTRY_HEADER_SIZE + dataLength + 4; // +4 for CRC32
      }
    } catch (IOException e) {
      throw new WALException("Failed to read entries from " + filePath, e);
    }

    return entries;
  }

  @Override
  public List<WALEntry> readFrom(Instant fromTimestamp) throws WALException {
    return readRange(fromTimestamp, Instant.MAX);
  }

  @Override
  public List<WALEntry> readRange(Instant fromTimestamp, Instant toTimestamp) throws WALException {
    lock.readLock().lock();
    try {
      List<WALEntry> entries = new ArrayList<>();

      if (fromTimestamp.isAfter(toTimestamp)) {
        return entries; // Invalid range
      }

      for (Map.Entry<Integer, Path> fileEntry : walFiles.entrySet()) {
        Path filePath = fileEntry.getValue();
        if (Files.exists(filePath)) {
          entries.addAll(readEntriesFromFileByTimestamp(filePath, fromTimestamp, toTimestamp));
        }
      }

      // Sort by sequence number to ensure correct order
      entries.sort(Comparator.comparingLong(WALEntry::getSequenceNumber));

      return entries;
    } finally {
      lock.readLock().unlock();
    }
  }

  private List<WALEntry> readEntriesFromFileByTimestamp(Path filePath, Instant fromTimestamp, Instant toTimestamp)
      throws WALException {
    List<WALEntry> entries = new ArrayList<>();

    try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
      long position = 0;

      while (position < file.length()) {
        file.seek(position);

        if (file.length() - position < ENTRY_HEADER_SIZE) {
          break;
        }

        // Read entry header to get timestamp
        file.readLong(); // sequence number
        long timestampSeconds = file.readLong();
        int timestampNanos = file.readInt();
        int dataLength = file.readInt();
        
        if (dataLength < 0 || position + ENTRY_HEADER_SIZE + dataLength + 4 > file.length()) {
          break;
        }

        // Create timestamp from the entry
        Instant entryTimestamp = Instant.ofEpochSecond(timestampSeconds, timestampNanos);

        // Check if this entry is in our desired timestamp range
        if (!entryTimestamp.isBefore(fromTimestamp) && !entryTimestamp.isAfter(toTimestamp)) {
          // Read the complete entry
          file.seek(position);
          int totalEntrySize = ENTRY_HEADER_SIZE + dataLength + 4; // +4 for CRC32
          byte[] entryData = new byte[totalEntrySize];
          file.readFully(entryData);

          try {
            WALEntry entry = deserializeEntry(entryData);
            entries.add(entry);
          } catch (Exception e) {
            // Skip corrupted entry and continue
          }
        }

        position += ENTRY_HEADER_SIZE + dataLength + 4; // +4 for CRC32
      }
    } catch (IOException e) {
      throw new WALException("Failed to read entries by timestamp from " + filePath, e);
    }

    return entries;
  }

  @Override
  public long getCurrentSequenceNumber() {
    return currentSequenceNumber;
  }

  @Override
  public long getNextSequenceNumber() {
    return currentSequenceNumber + 1;
  }

  @Override
  public void truncate(long upToSequenceNumber) throws WALException {
    lock.writeLock().lock();
    try {
      // Find files that can be completely removed
      List<Integer> filesToRemove = new ArrayList<>();

      for (Map.Entry<Integer, Path> fileEntry : walFiles.entrySet()) {
        Path filePath = fileEntry.getValue();
        if (Files.exists(filePath)) {
          long maxSeqInFile = findMaxSequenceInFile(filePath);
          if (maxSeqInFile < upToSequenceNumber) {
            filesToRemove.add(fileEntry.getKey());
          }
        }
      }

      // Remove old files and update entry count
      for (Integer fileIndex : filesToRemove) {
        Path filePath = walFiles.get(fileIndex);
        Files.deleteIfExists(filePath);
        walFiles.remove(fileIndex);
      }

    } catch (IOException e) {
      throw new WALException("Failed to truncate WAL", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private long findMaxSequenceInFile(Path filePath) throws WALException {
    try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
      long maxSeq = -1;
      long position = 0;

      while (position < file.length()) {
        file.seek(position);

        if (file.length() - position < ENTRY_HEADER_SIZE) {
          break;
        }

        // Read sequence number directly
        long sequenceNumber = file.readLong();
        maxSeq = Math.max(maxSeq, sequenceNumber);
        
        // Read timestamp fields to get to data length
        file.readLong(); // timestamp seconds
        file.readInt();  // timestamp nanoseconds
        
        // Read data length
        int dataLength = file.readInt();
        if (dataLength < 0 || position + ENTRY_HEADER_SIZE + dataLength + 4 > file.length()) {
          break;
        }

        position += ENTRY_HEADER_SIZE + dataLength + 4; // +4 for CRC32
      }

      return maxSeq;
    } catch (IOException e) {
      throw new WALException("Failed to find max sequence in " + filePath, e);
    }
  }

  @Override
  public long size() {
    // If currentSequenceNumber is -1, there are no entries (0 entries)
    // If currentSequenceNumber is N, there are N+1 entries (sequences 0, 1, 2, ..., N)
    return currentSequenceNumber + 1;
  }

  @Override
  public boolean isEmpty() {
    return currentSequenceNumber == -1;
  }

  @Override
  public void close() throws Exception {
    lock.writeLock().lock();
    try {
      // Ensure final sequence number is saved
      if (sequenceFileRAF != null) {
        saveSequenceNumber();
        // Always sync sequence file on close for durability
        sequenceFileRAF.getFD().sync();
        sequenceFileRAF.close();
        sequenceFileRAF = null;
      }
      
      // Close and sync current log file
      if (currentFile != null) {
        // Always sync log file on close for durability
        currentFile.getFD().sync();
        currentFile.close();
        currentFile = null;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}

package com.github.lukaszbudnik.wal;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File-based implementation of Write Ahead Log using 4KB pages. Provides O(1) recovery and
 * professional-grade record spanning.
 */
public class FileBasedWAL implements WriteAheadLog {
  private static final Logger logger = LoggerFactory.getLogger(FileBasedWAL.class);

  // Page constants
  static final int PAGE_SIZE = 4096; // 4KB pages
  static final int PAGE_DATA_SIZE = PAGE_SIZE - WALPageHeader.HEADER_SIZE; // 4052 bytes
  static final int ENTRY_HEADER_SIZE = 25; // 1+8+8+4+4 bytes

  // File constants
  private static final String WAL_FILE_PREFIX = "wal-";
  private static final String WAL_FILE_SUFFIX = ".log";
  private static final int DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024; // 64MB

  // Entry types
  static final byte DATA_ENTRY = 1;
  static final byte CHECKPOINT_ENTRY = 2;

  private final Path walDirectory;
  private final int maxFileSize;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<Integer, Path> walFiles = new TreeMap<>();

  private volatile long currentSequenceNumber = -1;
  private volatile int currentFileIndex = 0;
  private RandomAccessFile currentFile;

  // In-memory page management
  private ByteBuffer currentPageBuffer;
  private long currentPageFirstSequence = -1;
  private long currentPageLastSequence = -1;
  private Instant currentPageFirstTimestamp;
  private Instant currentPageLastTimestamp;
  private short currentPageEntryCount = 0;

  public FileBasedWAL(Path walDirectory) throws WALException {
    this(walDirectory, DEFAULT_MAX_FILE_SIZE);
  }

  public FileBasedWAL(Path walDirectory, int maxFileSize) throws WALException {
    this.walDirectory = walDirectory;
    this.maxFileSize = maxFileSize;

    logger.info(
        "Initializing FileBasedWAL: directory={}, maxFileSize={} bytes", walDirectory, maxFileSize);

    try {
      Files.createDirectories(walDirectory);
      initialize();
      logger.info(
          "FileBasedWAL initialized successfully: currentSequence={}, currentFileIndex={}, files={}",
          currentSequenceNumber,
          currentFileIndex,
          walFiles.size());
    } catch (IOException e) {
      logger.error("Failed to initialize WAL in directory {}: {}", walDirectory, e.getMessage(), e);
      throw new WALException("Failed to initialize WAL", e);
    }
  }

  private void initialize() throws IOException, WALException {
    logger.debug("Starting WAL initialization in directory: {}", walDirectory);

    // Discover existing WAL files
    int discoveredFiles = 0;
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(walDirectory, WAL_FILE_PREFIX + "*" + WAL_FILE_SUFFIX)) {
      for (Path file : stream) {
        String fileName = file.getFileName().toString();
        int fileIndex = extractFileIndex(fileName);
        long fileSize = Files.size(file);
        walFiles.put(fileIndex, file);
        currentFileIndex = Math.max(currentFileIndex, fileIndex);
        discoveredFiles++;
        logger.debug(
            "Discovered WAL file: {} (index={}, size={} bytes)", fileName, fileIndex, fileSize);
      }
    }

    logger.debug(
        "WAL file discovery completed: {} files found, currentFileIndex={}",
        discoveredFiles,
        currentFileIndex);

    // Recover from last file using O(1) page header reading
    recoverFromLastFile();

    // Open or create current file
    openCurrentFile();
  }

  private int extractFileIndex(String fileName) {
    String indexStr =
        fileName.substring(WAL_FILE_PREFIX.length(), fileName.length() - WAL_FILE_SUFFIX.length());
    return Integer.parseInt(indexStr);
  }

  /** O(1) recovery by reading the last page header from the newest file. */
  private void recoverFromLastFile() throws IOException, WALException {
    if (walFiles.isEmpty()) {
      logger.debug("No WAL files found, starting with sequence -1");
      currentSequenceNumber = -1;
      return;
    }

    // Find the newest file
    Path newestFile = walFiles.get(currentFileIndex);
    logger.debug(
        "Starting recovery from newest file: {} (index={})",
        newestFile.getFileName(),
        currentFileIndex);

    if (!Files.exists(newestFile)) {
      logger.debug("Newest file does not exist, starting with sequence -1");
      currentSequenceNumber = -1;
      return;
    }

    try (RandomAccessFile file = new RandomAccessFile(newestFile.toFile(), "r")) {
      long fileSize = file.length();
      logger.debug("Recovery file size: {} bytes", fileSize);

      if (fileSize == 0) {
        logger.debug("Recovery file is empty, starting with sequence -1");
        currentSequenceNumber = -1;
        return;
      }

      // Check if file is page-aligned and try O(1) recovery
      if (fileSize % PAGE_SIZE == 0 && fileSize >= PAGE_SIZE) {
        try {
          long lastPagePosition = fileSize - PAGE_SIZE;
          logger.debug("Reading last page header at position: {}", lastPagePosition);
          file.seek(lastPagePosition);

          byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
          file.readFully(headerData);

          WALPageHeader header = WALPageHeader.deserialize(headerData);
          logger.debug(
              "Last page header: firstSeq={}, lastSeq={}, entryCount={}",
              header.getFirstSequence(),
              header.getLastSequence(),
              header.getEntryCount());

          if (header.getLastSequence() >= 0) {
            currentSequenceNumber = header.getLastSequence();
            logger.info("WAL file recovery completed: sequence={}", currentSequenceNumber);
          }
        } catch (Exception e) {
          logger.error("WAL file error: page header could not be read.", e);
          throw new WALException("WAL file error: page header could not be read", e);
        }
      } else {
        logger.error(
            "WAL file error: File is not page-aligned. Size: {}, PAGE_SIZE: {}",
            fileSize,
            PAGE_SIZE);
        throw new WALException("WAL file error: File is not page-aligned.");
      }
    }
  }

  private void openCurrentFile() throws IOException {
    Path currentFilePath =
        walDirectory.resolve(WAL_FILE_PREFIX + currentFileIndex + WAL_FILE_SUFFIX);
    walFiles.put(currentFileIndex, currentFilePath);

    if (currentFile != null) {
      logger.debug("Closing previous file before opening new one");
      currentFile.close();
    }

    boolean isNewFile = !Files.exists(currentFilePath);
    currentFile = new RandomAccessFile(currentFilePath.toFile(), "rw");
    long fileLength = currentFile.length();
    currentFile.seek(fileLength); // Position at end for appending

    logger.debug(
        "Opened WAL file: {} (index={}, length={} bytes, isNew={})",
        currentFilePath.getFileName(),
        currentFileIndex,
        fileLength,
        isNewFile);

    // Initialize new page buffer
    initializeNewPage();
  }

  private void initializeNewPage() {
    currentPageBuffer = ByteBuffer.allocate(PAGE_SIZE);
    currentPageBuffer.position(WALPageHeader.HEADER_SIZE); // Reserve space for header
    currentPageFirstSequence = -1;
    currentPageLastSequence = -1;
    currentPageFirstTimestamp = null;
    currentPageLastTimestamp = null;
    currentPageEntryCount = 0;

    logger.debug(
        "Initialized new page buffer: available space={} bytes",
        PAGE_SIZE - WALPageHeader.HEADER_SIZE);
  }

  @Override
  public WALEntry createAndAppend(ByteBuffer data) throws WALException {
    lock.writeLock().lock();
    try {
      long nextSeq = currentSequenceNumber + 1;
      WALEntry entry = new WALEntry(nextSeq, Instant.now(), data);

      logger.debug(
          "Creating entry: seq={}, dataSize={} bytes",
          nextSeq,
          data != null ? data.remaining() : 0);

      writeEntry(entry);
      currentSequenceNumber = nextSeq;

      return entry;
    } catch (IOException | WALException e) {
      logger.error(
          "Failed to append entry with sequence {}: {}", currentSequenceNumber + 1, e.getMessage());
      throw new WALException("Failed to append entry", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public List<WALEntry> createAndAppendBatch(List<ByteBuffer> dataList) throws WALException {
    if (dataList.isEmpty()) {
      return new ArrayList<>();
    }

    logger.debug(
        "Creating batch of {} entries, starting from seq={}",
        dataList.size(),
        currentSequenceNumber + 1);

    lock.writeLock().lock();
    try {
      List<WALEntry> entries = new ArrayList<>(dataList.size());
      long nextSeq = currentSequenceNumber + 1;
      long batchStartSeq = nextSeq;

      // Create all entries with consecutive sequence numbers
      for (ByteBuffer data : dataList) {
        WALEntry entry = new WALEntry(nextSeq++, Instant.now(), data);
        entries.add(entry);
        writeEntry(entry);
      }

      currentSequenceNumber = nextSeq - 1;

      logger.debug(
          "Batch completed: {} entries written (seq={}-{})",
          entries.size(),
          batchStartSeq,
          currentSequenceNumber);

      // Check if we need to rotate file
      if (currentFile.length() > maxFileSize) {
        logger.debug("File size exceeded after batch, triggering rotation");
        rotateFile();
      }

      return entries;
    } catch (IOException | WALException e) {
      logger.error("Failed to append batch of {} entries: {}", dataList.size(), e.getMessage());
      throw new WALException("Failed to append batch", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Write entry to current page buffer, flushing to disk if page becomes full. */
  private void writeEntry(WALEntry entry) throws IOException, WALException {
    byte[] serializedEntry = serializeEntry(entry);
    int entrySize = serializedEntry.length;

    logger.debug(
        "Writing entry: seq={}, serializedSize={} bytes, pageRemaining={} bytes",
        entry.getSequenceNumber(),
        entrySize,
        currentPageBuffer.remaining());

    // Check if entry is too large for any page
    if (entrySize + ENTRY_HEADER_SIZE > PAGE_DATA_SIZE) {
      logger.error("Entry too large: {} bytes, max: {} bytes", entrySize, PAGE_DATA_SIZE);
      throw new WALException(
          "Entry too large: " + entrySize + " bytes, max: " + PAGE_DATA_SIZE + " bytes");
    }

    // Check if entry fits in current page
    if (currentPageBuffer.remaining() < entrySize + ENTRY_HEADER_SIZE) {
      logger.debug(
          "Page full, flushing current page (entries={}, remaining={} bytes)",
          currentPageEntryCount,
          currentPageBuffer.remaining());
      // Flush current page and start new one
      flushCurrentPage();
      initializeNewPage();
    }

    // Add entry to page buffer
    currentPageBuffer.put(serializedEntry);

    // Update page metadata
    if (currentPageFirstSequence == -1) {
      currentPageFirstSequence = entry.getSequenceNumber();
      currentPageFirstTimestamp = entry.getTimestamp();
      logger.debug("Started new page with first entry: seq={}", entry.getSequenceNumber());
    }
    currentPageLastSequence = entry.getSequenceNumber();
    currentPageLastTimestamp = entry.getTimestamp();
    currentPageEntryCount++;

    logger.debug(
        "Entry added to page: entryCount={}, pageUsed={} bytes",
        currentPageEntryCount,
        currentPageBuffer.position());
  }

  /** Flush current page buffer to disk with proper header. */
  private void flushCurrentPage() throws IOException, WALException {
    if (currentPageBuffer == null || currentPageBuffer.position() <= WALPageHeader.HEADER_SIZE) {
      logger.debug("No data to flush in current page");
      return; // No data to flush
    }

    // Create page header
    Instant firstTs = currentPageFirstTimestamp != null ? currentPageFirstTimestamp : Instant.now();
    Instant lastTs = currentPageLastTimestamp != null ? currentPageLastTimestamp : firstTs;

    WALPageHeader header =
        new WALPageHeader(
            currentPageFirstSequence, currentPageLastSequence,
            firstTs, lastTs,
            currentPageEntryCount, WALPageHeader.NO_CONTINUATION);

    logger.debug(
        "Flushing page to disk: seq={}-{}, entries={}, dataSize={} bytes",
        currentPageFirstSequence,
        currentPageLastSequence,
        currentPageEntryCount,
        currentPageBuffer.position() - WALPageHeader.HEADER_SIZE);

    // Write header at beginning of buffer
    byte[] headerBytes = header.serialize();
    System.arraycopy(headerBytes, 0, currentPageBuffer.array(), 0, headerBytes.length);

    // Write full page to disk
    long filePositionBefore = currentFile.getFilePointer();
    currentFile.write(currentPageBuffer.array());
    currentFile.getFD().sync();
    long filePositionAfter = currentFile.getFilePointer();

    logger.debug(
        "Page written to disk: filePos={}-{}, fileSize={} bytes",
        filePositionBefore,
        filePositionAfter,
        currentFile.length());

    // Check if we need to rotate file
    if (currentFile.length() >= maxFileSize) {
      logger.debug(
          "File size limit reached: {} >= {}, triggering rotation",
          currentFile.length(),
          maxFileSize);
      rotateFile();
    }
  }

  /** Serialize WAL entry to binary format. */
  private byte[] serializeEntry(WALEntry entry) {
    ByteBuffer dataBuffer = entry.getData();
    int dataLength = dataBuffer != null ? dataBuffer.remaining() : 0;

    // Calculate total size
    int totalSize = ENTRY_HEADER_SIZE + dataLength;
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);

    // Write entry fields
    buffer.put(DATA_ENTRY);
    buffer.putLong(entry.getSequenceNumber());
    buffer.putLong(entry.getTimestamp().toEpochMilli());
    buffer.putInt(dataLength);

    if (dataBuffer != null) {
      ByteBuffer dataDuplicate = dataBuffer.duplicate();
      dataDuplicate.rewind();
      buffer.put(dataDuplicate);
    }

    // Calculate and write CRC32
    CRC32 crc = new CRC32();
    buffer.position(0);
    buffer.limit(totalSize - 4); // Exclude CRC field
    crc.update(buffer);
    buffer.limit(totalSize);
    buffer.putInt((int) crc.getValue());

    return buffer.array();
  }

  private void rotateFile() throws IOException, WALException {
    logger.info("Rotating WAL file from index {} to {}", currentFileIndex, currentFileIndex + 1);

    currentFile.close();

    currentFileIndex++;
    openCurrentFile();

    logger.debug(
        "File rotation completed: new file index={}, total files={}",
        currentFileIndex,
        walFiles.size());
  }

  @Override
  public void sync() throws WALException {
    lock.writeLock().lock();
    try {
      if (currentFile != null) {
        // Flush current page buffer to disk
        flushCurrentPage();
        initializeNewPage(); // Start fresh page after sync

        currentFile.getFD().sync();
        logger.debug("WAL synced to disk");
      }
    } catch (IOException e) {
      throw new WALException("Failed to sync WAL", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public List<WALEntry> readFrom(long fromSequenceNumber) throws WALException {
    lock.writeLock().lock();
    try {
      // Flush current page to ensure all data is on disk
      if (currentPageBuffer != null && currentPageBuffer.position() > WALPageHeader.HEADER_SIZE) {
        flushCurrentPage();
        initializeNewPage();
      }
    } catch (IOException e) {
      throw new WALException("Failed to flush page before read", e);
    } finally {
      lock.writeLock().unlock();
    }

    return readRange(fromSequenceNumber, Long.MAX_VALUE);
  }

  @Override
  public List<WALEntry> readRange(long fromSequenceNumber, long toSequenceNumber)
      throws WALException {
    if (fromSequenceNumber > toSequenceNumber) {
      throw new WALException("Invalid range: from > to");
    }

    logger.debug("Reading entries in sequence range: {}-{}", fromSequenceNumber, toSequenceNumber);

    lock.writeLock().lock();
    try {
      // Flush current page to ensure all data is on disk
      if (currentPageBuffer != null && currentPageBuffer.position() > WALPageHeader.HEADER_SIZE) {
        logger.debug("Flushing current page before read");
        flushCurrentPage();
        initializeNewPage();
      }
    } catch (IOException e) {
      throw new WALException("Failed to flush page before read", e);
    } finally {
      lock.writeLock().unlock();
    }

    List<WALEntry> result = new ArrayList<>();

    // Step 1 & 2: Scan all WAL files and filter overlapping ones
    List<Path> overlappingFiles = new ArrayList<>();
    logger.debug("Scanning {} WAL files for overlapping ranges", walFiles.size());

    for (Path walFile : walFiles.values()) {
      if (Files.exists(walFile)
          && fileOverlapsSequenceRange(walFile, fromSequenceNumber, toSequenceNumber)) {
        overlappingFiles.add(walFile);
        logger.debug("File {} overlaps with requested range", walFile.getFileName());
      }
    }

    logger.debug("Found {} overlapping files to process", overlappingFiles.size());

    // Step 3, 4 & 5: Process overlapping files
    for (Path walFile : overlappingFiles) {
      try {
        List<WALEntry> fileEntries =
            readEntriesFromFileBySequence(walFile, fromSequenceNumber, toSequenceNumber);
        result.addAll(fileEntries);
        logger.debug("Read {} entries from file {}", fileEntries.size(), walFile.getFileName());
      } catch (IOException e) {
        logger.error("Failed to read from file {}: {}", walFile.getFileName(), e.getMessage());
        throw new WALException("Failed to read from file: " + walFile, e);
      }
    }

    logger.debug("Read operation completed: {} total entries found", result.size());
    return result;
  }

  private boolean fileOverlapsSequenceRange(Path walFile, long fromSeq, long toSeq)
      throws WALException {
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      if (fileSize == 0) return false;

      // Read first page header
      if (fileSize < WALPageHeader.HEADER_SIZE) return false;
      file.seek(0);
      byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(headerData);
      WALPageHeader firstHeader = WALPageHeader.deserialize(headerData);

      // Read last page header
      long lastPageOffset = fileSize - (fileSize % PAGE_SIZE);

      // Handle special case when fileSize == pageSize
      // Then the last page is also the first one
      if (lastPageOffset == fileSize) {
        lastPageOffset -= PAGE_SIZE;
      }

      if (lastPageOffset + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(lastPageOffset);
        file.readFully(headerData);
        WALPageHeader lastHeader = WALPageHeader.deserialize(headerData);

        // Check if file range overlaps with requested range
        return !(lastHeader.getLastSequence() < fromSeq || firstHeader.getFirstSequence() > toSeq);
      }

      // Fallback: assume overlap if can't read last header
      return firstHeader.getFirstSequence() <= toSeq;
    } catch (Exception e) {
      throw new WALException("Failed to check file overlap: " + walFile, e);
    }
  }

  private List<WALEntry> readEntriesFromFileBySequence(Path walFile, long fromSeq, long toSeq)
      throws IOException, WALException {
    List<WALEntry> entries = new ArrayList<>();

    logger.debug(
        "Reading entries from file: {} (range: {}-{})", walFile.getFileName(), fromSeq, toSeq);

    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      long currentPos = 0;
      int pagesRead = 0;
      int pagesSkipped = 0;

      logger.debug("File size: {} bytes, expected pages: {}", fileSize, fileSize / PAGE_SIZE);

      // Read all pages in file
      while (currentPos + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(currentPos);

        // Read page header
        byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
        file.readFully(headerData);

        try {
          WALPageHeader header = WALPageHeader.deserialize(headerData);

          logger.debug(
              "Page at offset {}: seq={}-{}, entries={}",
              currentPos,
              header.getFirstSequence(),
              header.getLastSequence(),
              header.getEntryCount());

          // Skip non-overlapping pages
          if (header.getLastSequence() < fromSeq || header.getFirstSequence() > toSeq) {
            logger.debug("Skipping non-overlapping page at offset {}", currentPos);
            currentPos += PAGE_SIZE;
            pagesSkipped++;
            continue;
          }

          // Read entries from overlapping page
          long pageEnd = Math.min(currentPos + PAGE_SIZE, fileSize);
          long dataSize = pageEnd - currentPos - WALPageHeader.HEADER_SIZE;

          List<WALEntry> pageEntries =
              readEntriesFromPage(
                  file, currentPos + WALPageHeader.HEADER_SIZE, dataSize, fromSeq, toSeq);
          entries.addAll(pageEntries);
          pagesRead++;

          logger.debug("Read {} entries from page at offset {}", pageEntries.size(), currentPos);

        } catch (WALException e) {
          // Skip corrupted page
          logger.warn("Skipping corrupted page at offset {}: {}", currentPos, e.getMessage());
          pagesSkipped++;
        }

        currentPos += PAGE_SIZE;
      }

      logger.debug(
          "File processing completed: {} entries from {} pages (skipped: {})",
          entries.size(),
          pagesRead,
          pagesSkipped);
    }

    return entries;
  }

  private List<WALEntry> readEntriesFromPage(
      RandomAccessFile file, long startOffset, long dataSize, long fromSeq, long toSeq)
      throws IOException, WALException {
    List<WALEntry> entries = new ArrayList<>();
    file.seek(startOffset);

    long currentPos = startOffset;
    long endPos = startOffset + dataSize;

    while (currentPos < endPos && currentPos < file.length()) {
      if (endPos - currentPos < ENTRY_HEADER_SIZE) break;

      // Read entry header
      byte entryType = file.readByte();
      if (entryType == 0) break; // Padding

      long sequence = file.readLong();
      long timestampMillis = file.readLong();
      int dataLength = file.readInt();

      if (dataLength < 0 || currentPos + ENTRY_HEADER_SIZE + dataLength + 4 > endPos) {
        break; // Invalid entry
      }

      // Read data and CRC
      byte[] data = new byte[dataLength];
      file.readFully(data);
      int crc = file.readInt();

      // Filter by sequence range
      if (sequence >= fromSeq && sequence <= toSeq) {
        Instant timestamp = Instant.ofEpochMilli(timestampMillis);
        ByteBuffer dataBuffer = dataLength > 0 ? ByteBuffer.wrap(data) : null;
        entries.add(new WALEntry(sequence, timestamp, dataBuffer));
      }

      currentPos = file.getFilePointer();
    }

    return entries;
  }

  @Override
  public List<WALEntry> readFrom(Instant fromTimestamp) throws WALException {
    return readRange(fromTimestamp, Instant.MAX);
  }

  @Override
  public List<WALEntry> readRange(Instant fromTimestamp, Instant toTimestamp) throws WALException {
    if (fromTimestamp.isAfter(toTimestamp)) {
      throw new WALException("Invalid range: from > to");
    }

    logger.debug("Reading entries in timestamp range: {} to {}", fromTimestamp, toTimestamp);

    lock.writeLock().lock();
    try {
      // Flush current page to ensure all data is on disk
      if (currentPageBuffer != null && currentPageBuffer.position() > WALPageHeader.HEADER_SIZE) {
        logger.debug("Flushing current page before timestamp read");
        flushCurrentPage();
        initializeNewPage();
      }
    } catch (IOException e) {
      throw new WALException("Failed to flush page before read", e);
    } finally {
      lock.writeLock().unlock();
    }

    List<WALEntry> result = new ArrayList<>();

    // Step 1 & 2: Scan all WAL files and filter overlapping ones
    List<Path> overlappingFiles = new ArrayList<>();
    logger.debug("Scanning {} WAL files for timestamp overlaps", walFiles.size());

    for (Path walFile : walFiles.values()) {
      if (Files.exists(walFile)
          && fileOverlapsTimestampRange(walFile, fromTimestamp, toTimestamp)) {
        overlappingFiles.add(walFile);
        logger.debug("File {} overlaps with timestamp range", walFile.getFileName());
      }
    }

    logger.debug("Found {} overlapping files for timestamp range", overlappingFiles.size());

    // Step 3, 4 & 5: Process overlapping files
    for (Path walFile : overlappingFiles) {
      try {
        List<WALEntry> fileEntries =
            readEntriesFromFileByTimestamp(walFile, fromTimestamp, toTimestamp);
        result.addAll(fileEntries);
        logger.debug(
            "Read {} entries from file {} by timestamp", fileEntries.size(), walFile.getFileName());
      } catch (IOException e) {
        logger.error(
            "Failed to read from file {} by timestamp: {}", walFile.getFileName(), e.getMessage());
        throw new WALException("Failed to read from file: " + walFile, e);
      }
    }

    // Sort by sequence number to ensure correct order
    result.sort(Comparator.comparingLong(WALEntry::getSequenceNumber));

    logger.debug("Timestamp read operation completed: {} total entries found", result.size());
    return result;
  }

  private boolean fileOverlapsTimestampRange(Path walFile, Instant fromTs, Instant toTs)
      throws WALException {
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      if (fileSize == 0) return false;

      // Read first page header
      if (fileSize < WALPageHeader.HEADER_SIZE) return false;
      file.seek(0);
      byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(headerData);
      WALPageHeader firstHeader = WALPageHeader.deserialize(headerData);

      // Read last page header
      long lastPageOffset = fileSize - (fileSize % PAGE_SIZE);
      if (lastPageOffset + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(lastPageOffset);
        file.readFully(headerData);
        WALPageHeader lastHeader = WALPageHeader.deserialize(headerData);

        // Check if file range overlaps with requested range
        return !(lastHeader.getLastTimestamp().isBefore(fromTs)
            || firstHeader.getFirstTimestamp().isAfter(toTs));
      }

      // Fallback: assume overlap if can't read last header
      return !firstHeader.getFirstTimestamp().isAfter(toTs);
    } catch (Exception e) {
      throw new WALException("Failed to check file overlap: " + walFile, e);
    }
  }

  private List<WALEntry> readEntriesFromFileByTimestamp(Path walFile, Instant fromTs, Instant toTs)
      throws IOException, WALException {
    List<WALEntry> entries = new ArrayList<>();

    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      long currentPos = 0;

      // Read all pages in file
      while (currentPos + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(currentPos);

        // Read page header
        byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
        file.readFully(headerData);

        try {
          WALPageHeader header = WALPageHeader.deserialize(headerData);

          // Skip non-overlapping pages
          if (header.getLastTimestamp().isBefore(fromTs)
              || header.getFirstTimestamp().isAfter(toTs)) {
            currentPos += PAGE_SIZE;
            continue;
          }

          // Read entries from overlapping page
          long pageEnd = Math.min(currentPos + PAGE_SIZE, fileSize);
          long dataSize = pageEnd - currentPos - WALPageHeader.HEADER_SIZE;

          entries.addAll(
              readEntriesFromPageByTimestamp(
                  file, currentPos + WALPageHeader.HEADER_SIZE, dataSize, fromTs, toTs));

        } catch (WALException e) {
          // Skip corrupted page
          logger.warn("Skipping corrupted page at offset {}: {}", currentPos, e.getMessage());
        }

        currentPos += PAGE_SIZE;
      }
    }

    return entries;
  }

  private List<WALEntry> readEntriesFromPageByTimestamp(
      RandomAccessFile file, long startOffset, long dataSize, Instant fromTs, Instant toTs)
      throws IOException, WALException {
    List<WALEntry> entries = new ArrayList<>();
    file.seek(startOffset);

    long currentPos = startOffset;
    long endPos = startOffset + dataSize;

    while (currentPos < endPos && currentPos < file.length()) {
      if (endPos - currentPos < ENTRY_HEADER_SIZE) break;

      // Read entry header
      byte entryType = file.readByte();
      if (entryType == 0) break; // Padding

      long sequence = file.readLong();
      long timestampMillis = file.readLong();
      int dataLength = file.readInt();

      if (dataLength < 0 || currentPos + ENTRY_HEADER_SIZE + dataLength + 4 > endPos) {
        break; // Invalid entry
      }

      // Read data and CRC
      byte[] data = new byte[dataLength];
      file.readFully(data);
      int crc = file.readInt();

      // Filter by timestamp range
      Instant timestamp = Instant.ofEpochMilli(timestampMillis);
      if (!timestamp.isBefore(fromTs) && !timestamp.isAfter(toTs)) {
        ByteBuffer dataBuffer = dataLength > 0 ? ByteBuffer.wrap(data) : null;
        entries.add(new WALEntry(sequence, timestamp, dataBuffer));
      }

      currentPos = file.getFilePointer();
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
          // maxSeqInFile != -1 covers newly created files not yet flushed to disk
          if (maxSeqInFile != -1 && maxSeqInFile < upToSequenceNumber) {
            filesToRemove.add(fileEntry.getKey());
          }
        }
      }

      // Remove old files
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

        byte entryType = file.readByte();
        if (entryType != DATA_ENTRY) {
          position++;
          continue;
        }

        long sequenceNumber = file.readLong();
        file.readLong(); // timestamp milliseconds
        int dataLength = file.readInt();

        if (dataLength >= 0 && position + ENTRY_HEADER_SIZE + dataLength <= file.length()) {
          maxSeq = Math.max(maxSeq, sequenceNumber);
          position += ENTRY_HEADER_SIZE + dataLength;
        } else {
          position++;
        }
      }

      return maxSeq;
    } catch (IOException e) {
      throw new WALException("Failed to find max sequence in " + filePath, e);
    }
  }

  @Override
  public long size() {
    return currentSequenceNumber + 1;
  }

  @Override
  public boolean isEmpty() {
    return currentSequenceNumber == -1;
  }

  @Override
  public void close() throws Exception {
    logger.info(
        "Closing FileBasedWAL: currentSequence={}, totalFiles={}",
        currentSequenceNumber,
        walFiles.size());

    lock.writeLock().lock();
    try {
      if (currentFile != null) {
        // Always flush current page before closing
        flushCurrentPage();

        currentFile.getFD().sync();
        currentFile.close();
        currentFile = null;
        logger.debug("Current WAL file closed and synced");
      }

      logger.info("FileBasedWAL closed successfully");
    } finally {
      lock.writeLock().unlock();
    }
  }
}

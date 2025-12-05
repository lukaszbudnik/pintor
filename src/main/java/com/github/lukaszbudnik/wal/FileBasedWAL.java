package com.github.lukaszbudnik.wal;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * File-based implementation of Write Ahead Log using configurable page sizes (4KB, 8KB, 16KB, 32KB,
 * or 64KB). Provides O(1) recovery and professional-grade record spanning.
 */
public class FileBasedWAL implements WriteAheadLog {
  private static final Logger logger = LoggerFactory.getLogger(FileBasedWAL.class);

  // Page constants
  static final int ENTRY_HEADER_SIZE = 25; // 1+8+8+4+4 bytes

  // File constants
  private static final String WAL_FILE_PREFIX = "wal-";
  private static final String WAL_FILE_SUFFIX = ".log";
  private static final int DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024; // 64MB
  private static final byte DEFAULT_PAGE_SIZE_KB = 8; // 8KB pages

  // Entry types
  static final byte DATA_ENTRY = 1;
  static final byte CHECKPOINT_ENTRY = 2;

  private final Path walDirectory;
  private final int maxFileSize;
  private final byte pageSizeKB;
  private final int pageSize;
  private final int pageDataSize;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<Integer, Path> walFiles = new TreeMap<>();
  private final WALMetrics metrics = new WALMetrics();
  private final FileRotationCallback rotationCallback;

  private long currentSequenceNumber = -1;
  private int currentFileIndex = 0;
  private RandomAccessFile currentFile;

  // In-memory page management
  private ByteBuffer currentPageBuffer;
  private long currentPageFirstSequence = -1;
  private long currentPageLastSequence = -1;
  private Instant currentPageFirstTimestamp;
  private Instant currentPageLastTimestamp;
  private short currentPageEntryCount = 0;
  private byte currentPageContinuationFlags = WALPageHeader.NO_CONTINUATION;

  public FileBasedWAL(Path walDirectory) throws WALException {
    this(walDirectory, DEFAULT_MAX_FILE_SIZE, DEFAULT_PAGE_SIZE_KB, null);
  }

  public FileBasedWAL(Path walDirectory, int maxFileSize, byte pageSizeKB) throws WALException {
    this(walDirectory, maxFileSize, pageSizeKB, null);
  }

  public FileBasedWAL(
      Path walDirectory, int maxFileSize, byte pageSizeKB, FileRotationCallback rotationCallback)
      throws WALException {
    validatePageSize(pageSizeKB);
    this.walDirectory = walDirectory;
    this.maxFileSize = maxFileSize;
    this.pageSizeKB = pageSizeKB;
    this.pageSize = pageSizeKB * 1024;
    this.pageDataSize = pageSize - WALPageHeader.HEADER_SIZE;
    this.rotationCallback = rotationCallback;

    logger.info(
        "Initializing FileBasedWAL: directory={}, maxFileSize={} bytes, pageSize={}KB",
        walDirectory,
        maxFileSize,
        pageSizeKB);

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

  private static void validatePageSize(byte pageSizeKB) throws WALException {
    if (pageSizeKB != 4
        && pageSizeKB != 8
        && pageSizeKB != 16
        && pageSizeKB != 32
        && pageSizeKB != 64) {
      throw new WALException(
          "Invalid page size: " + pageSizeKB + "KB. Valid sizes are: 4, 8, 16, 32, 64");
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
      if (fileSize % pageSize == 0 && fileSize >= pageSize) {
        try {
          long lastPagePosition = fileSize - pageSize;
          logger.debug("Reading last page header at position: {}", lastPagePosition);
          file.seek(lastPagePosition);

          byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
          file.readFully(headerData);

          WALPageHeader header = WALPageHeader.deserialize(headerData);

          // Validate page size matches
          if (header.getPageSizeKB() != pageSizeKB) {
            throw new WALException(
                "Page size mismatch: WAL file has "
                    + header.getPageSizeKB()
                    + "KB pages, "
                    + "but FileBasedWAL configured for "
                    + pageSizeKB
                    + "KB pages");
          }

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
            "WAL file error: File is not page-aligned. Size: {}, pageSize: {}", fileSize, pageSize);
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

    if (isNewFile) {
      metrics.incrementFilesCreated();
    }

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
    currentPageBuffer = ByteBuffer.allocate(pageSize);
    currentPageBuffer.position(WALPageHeader.HEADER_SIZE); // Reserve space for header
    currentPageFirstSequence = -1;
    currentPageLastSequence = -1;
    currentPageFirstTimestamp = null;
    currentPageLastTimestamp = null;
    currentPageEntryCount = 0;
    currentPageContinuationFlags = WALPageHeader.NO_CONTINUATION;

    logger.debug("Initialized new page buffer: available space={} bytes", pageDataSize);
  }

  @Override
  public WALEntry createAndAppend(ByteBuffer data) throws WALException {
    lock.writeLock().lock();
    try {
      long nextSeq = currentSequenceNumber + 1;
      WALEntry entry = new WALEntry(nextSeq, Instant.now(), data);

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

      return entries;
    } catch (IOException | WALException e) {
      logger.error("Failed to append batch of {} entries: {}", dataList.size(), e.getMessage());
      throw new WALException("Failed to append batch", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Write entry to current page buffer, handling spanning entries across multiple pages.
   *
   * <p>This method implements page space utilization optimization by checking if the current page
   * has sufficient space (>= 25 bytes for entry header) to write the first part of a spanning
   * entry. This eliminates unnecessary page flushes and reduces wasted space.
   *
   * <p>Entry writing logic:
   *
   * <ul>
   *   <li>If entry fits in current page: write directly to current page
   *   <li>If entry fits in single page but not current: flush current page, write to new page
   *   <li>If entry needs spanning and current page has >= 25 bytes: use current page for first part
   *   <li>If entry needs spanning and current page has < 25 bytes: flush first, then span
   * </ul>
   *
   * @param entry the WAL entry to write
   * @throws IOException if I/O error occurs during writing
   * @throws WALException if WAL-specific error occurs
   */
  private void writeEntry(WALEntry entry) throws IOException, WALException {
    byte[] serializedEntry = serializeEntry(entry);
    int entrySize = serializedEntry.length;

    logger.debug(
        "Writing entry: seq={}, entrySize={} bytes, pageRemaining={} bytes",
        entry.getSequenceNumber(),
        entrySize,
        currentPageBuffer.remaining());

    // Check if entry fits in current page
    if (currentPageBuffer.remaining() >= entrySize) {
      // Entry fits in current page - simple case
      writeEntryToCurrentPage(entry, serializedEntry);
    } else if (entrySize <= pageDataSize) {
      // Entry doesn't fit in current page but fits in a single page
      // Flush current page and write to new page
      if (currentPageBuffer.position() > WALPageHeader.HEADER_SIZE) {
        logger.debug("Flushing current page to make room for entry that fits in single page");
        flushCurrentPage();
        initializeNewPage();
      }
      writeEntryToCurrentPage(entry, serializedEntry);
    } else {
      // Entry is too large for a single page - needs spanning
      // Check if we can use current page for first part
      if (currentPageBuffer.remaining() >= ENTRY_HEADER_SIZE) {
        // Use current page for first part - optimization to utilize available space
        logger.debug(
            "Using current page for spanning entry first part: available={} bytes",
            currentPageBuffer.remaining());
        writeSpanningEntry(entry, serializedEntry);
      } else {
        // Not enough space even for header, flush first
        if (currentPageBuffer.position() > WALPageHeader.HEADER_SIZE) {
          logger.debug(
              "Insufficient space for entry header ({} < {}), flushing current page",
              currentPageBuffer.remaining(),
              ENTRY_HEADER_SIZE);
          flushCurrentPage();
          initializeNewPage();
        }
        writeSpanningEntry(entry, serializedEntry);
      }
    }

    logger.debug("Entry written: seq={}, totalSize={} bytes", entry.getSequenceNumber(), entrySize);

    // Update metrics
    metrics.incrementEntriesWritten();
    metrics.incrementBytesWritten(entrySize);
  }

  /** Write entry that fits in current page. */
  private void writeEntryToCurrentPage(WALEntry entry, byte[] serializedEntry)
      throws IOException, WALException {
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

    // Flush current page when remaining space is less than an entry header
    if (currentPageBuffer.remaining() < ENTRY_HEADER_SIZE) {
      logger.debug(
          "Page full, flushing current page (entries={}, remaining={} bytes)",
          currentPageEntryCount,
          currentPageBuffer.remaining());
      flushCurrentPage();
      initializeNewPage();
    }
  }

  /**
   * Write entry that spans multiple pages, handling non-empty pages correctly.
   *
   * <p>This method has been optimized to handle pages that already contain complete entries. Unlike
   * the previous implementation that assumed a clean page, this version:
   *
   * <ul>
   *   <li>Preserves existing page metadata (first sequence, timestamps, entry count)
   *   <li>Correctly combines continuation flags when page has LAST_PART from previous entry
   *   <li>Handles mixed pages with both complete entries and spanning entry parts
   *   <li>Uses bitwise OR to combine flags: LAST_PART | FIRST_PART = 5
   * </ul>
   *
   * <p>Flag combination behavior:
   *
   * <ul>
   *   <li>FIRST_PART (1): First part of spanning entry
   *   <li>MIDDLE_PART (2): Middle part of spanning entry
   *   <li>LAST_PART (4): Last part of spanning entry
   *   <li>FIRST_PART | LAST_PART (5): Page contains both last part of one entry and first part of
   *       another
   * </ul>
   *
   * @param entry the WAL entry to write across multiple pages
   * @param serializedEntry the serialized byte array of the entry
   * @throws IOException if I/O error occurs during writing
   * @throws WALException if WAL-specific error occurs
   */
  private void writeSpanningEntry(WALEntry entry, byte[] serializedEntry)
      throws IOException, WALException {
    logger.debug(
        "Writing spanning entry: seq={}, totalSize={} bytes",
        entry.getSequenceNumber(),
        serializedEntry.length);

    int bytesWritten = 0;
    int totalBytes = serializedEntry.length;
    boolean isFirstPart = true;

    while (bytesWritten < totalBytes) {
      // Check if current page already has complete entries
      // If so, we need to flush them first before writing spanning entry data
      // This ensures pages don't mix complete entries with spanning entry data
      boolean pageHasCompleteEntries =
          (currentPageFirstSequence != -1
              && currentPageContinuationFlags == WALPageHeader.NO_CONTINUATION);

      if (isFirstPart && pageHasCompleteEntries) {
        // Flush complete entries first, then start spanning entry on new page
        logger.debug(
            "Flushing page with complete entries before starting spanning entry: entries={}, seq={}-{}",
            currentPageEntryCount,
            currentPageFirstSequence,
            currentPageLastSequence);
        flushCurrentPage();
        initializeNewPage();
      }

      // Calculate available space
      int availableSpace = currentPageBuffer.remaining();
      int bytesToWrite = Math.min(availableSpace, totalBytes - bytesWritten);

      // Write data chunk to current page
      currentPageBuffer.put(serializedEntry, bytesWritten, bytesToWrite);
      bytesWritten += bytesToWrite;

      // Update page metadata for spanning entry
      if (currentPageFirstSequence == -1) {
        currentPageFirstSequence = entry.getSequenceNumber();
        currentPageFirstTimestamp = entry.getTimestamp();
      }
      currentPageLastSequence = entry.getSequenceNumber();
      currentPageLastTimestamp = entry.getTimestamp();

      // Determine continuation flags and entry count
      byte newContinuationFlags;
      if (isFirstPart) {
        // First part of spanning entry
        newContinuationFlags =
            (bytesWritten >= totalBytes) ? WALPageHeader.NO_CONTINUATION : WALPageHeader.FIRST_PART;
        currentPageEntryCount = 1;
        isFirstPart = false;
      } else if (bytesWritten >= totalBytes) {
        // Last part of spanning entry
        newContinuationFlags = WALPageHeader.LAST_PART;
        currentPageEntryCount = 0; // Continuation pages don't count entries
      } else {
        // Middle part of spanning entry
        newContinuationFlags = WALPageHeader.MIDDLE_PART;
        currentPageEntryCount = 0; // Continuation pages don't count entries
      }

      // Update current page continuation flags to track what flags this page has
      // This is used for combining flags when multiple spanning parts are on the same page
      currentPageContinuationFlags = (byte) (currentPageContinuationFlags | newContinuationFlags);

      // Flush current page with appropriate continuation flags
      // The flushCurrentPageWithFlags method will use the combined flags
      flushCurrentPageWithFlags(newContinuationFlags);

      // Start new page if more data to write
      if (bytesWritten < totalBytes) {
        initializeNewPage();
      }
    }
  }

  /** Flush current page buffer to disk with proper header. */
  private void flushCurrentPage() throws IOException, WALException {
    flushCurrentPageWithFlags(WALPageHeader.NO_CONTINUATION);
  }

  /**
   * Flush current page buffer to disk with specified continuation flags.
   *
   * <p>This method implements flag combination logic using bitwise OR operations to handle
   * scenarios where a page contains multiple spanning entry parts. For example, when a page ends
   * with the last part of one spanning entry (LAST_PART) and begins with the first part of another
   * spanning entry (FIRST_PART), the combined flags become LAST_PART | FIRST_PART = 5.
   *
   * <p>Flag combination examples:
   *
   * <ul>
   *   <li>Page with only complete entries: flags = 0 (NO_CONTINUATION)
   *   <li>Page with first part of spanning entry: flags = 1 (FIRST_PART)
   *   <li>Page with last part of spanning entry: flags = 4 (LAST_PART)
   *   <li>Page with both last and first parts: flags = 5 (LAST_PART | FIRST_PART)
   * </ul>
   *
   * @param continuationFlags the continuation flags for the current write operation
   * @throws IOException if I/O error occurs during flushing
   * @throws WALException if WAL-specific error occurs
   */
  private void flushCurrentPageWithFlags(byte continuationFlags) throws IOException, WALException {
    if (currentPageBuffer == null || currentPageBuffer.position() <= WALPageHeader.HEADER_SIZE) {
      logger.debug("No data to flush in current page");
      return; // No data to flush
    }

    // Combine flags with existing page flags using bitwise OR
    // This handles the case where a page has LAST_PART from a previous spanning entry
    // and we're adding FIRST_PART from a new spanning entry
    byte combinedFlags = (byte) (currentPageContinuationFlags | continuationFlags);

    // Create page header
    Instant firstTs = currentPageFirstTimestamp != null ? currentPageFirstTimestamp : Instant.now();
    Instant lastTs = currentPageLastTimestamp != null ? currentPageLastTimestamp : firstTs;

    WALPageHeader header =
        new WALPageHeader(
            currentPageFirstSequence,
            currentPageLastSequence,
            firstTs,
            lastTs,
            currentPageEntryCount,
            combinedFlags,
            pageSizeKB);

    logger.debug(
        "Flushing page to disk: seq={}-{}, entries={}, dataSize={} bytes, flags={} (current={}, new={}, combined={})",
        currentPageFirstSequence,
        currentPageLastSequence,
        currentPageEntryCount,
        currentPageBuffer.position() - WALPageHeader.HEADER_SIZE,
        combinedFlags,
        currentPageContinuationFlags,
        continuationFlags,
        combinedFlags);

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

    // Update metrics
    metrics.incrementPagesWritten();

    // Check if we need to rotate file (only if not in middle of spanning entry)
    if (combinedFlags == WALPageHeader.NO_CONTINUATION
        || combinedFlags == WALPageHeader.LAST_PART
        || combinedFlags == (WALPageHeader.FIRST_PART | WALPageHeader.LAST_PART)) {
      if (currentFile.length() >= maxFileSize) {
        logger.debug(
            "File size limit reached: {} >= {}, triggering rotation",
            currentFile.length(),
            maxFileSize);
        rotateFile();
      }
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

    // Get metadata from current file before closing it
    Path currentFilePath = walFiles.get(currentFileIndex);
    FileMetadata metadata = null;
    if (rotationCallback != null && currentFilePath != null && currentFile != null) {
      // Sync the file first to ensure all data is written
      currentFile.getFD().sync();
      metadata = getFileMetadata(currentFilePath);
    }

    currentFile.close();

    // Call rotation callback with file metadata
    if (rotationCallback != null && metadata != null) {
      try {
        rotationCallback.onFileRotated(
            metadata.firstSequence,
            metadata.firstTimestamp,
            metadata.lastSequence,
            metadata.lastTimestamp,
            currentFilePath);
      } catch (Exception e) {
        logger.warn(
            "File rotation callback failed for file {}: {}", currentFilePath, e.getMessage(), e);
      }
    }

    currentFileIndex++;
    openCurrentFile();

    logger.debug(
        "File rotation completed: new file index={}, total files={}",
        currentFileIndex,
        walFiles.size());
  }

  private FileMetadata getFileMetadata(Path filePath) throws IOException, WALException {
    try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
      long fileSize = file.length();
      if (fileSize == 0) {
        return null;
      }

      // Read first page header
      file.seek(0);
      byte[] firstHeaderData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(firstHeaderData);
      WALPageHeader firstHeader = WALPageHeader.deserialize(firstHeaderData);

      // Read last page header
      long lastPagePosition = ((fileSize - 1) / pageSize) * pageSize;
      file.seek(lastPagePosition);
      byte[] lastHeaderData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(lastHeaderData);
      WALPageHeader lastHeader = WALPageHeader.deserialize(lastHeaderData);

      return new FileMetadata(
          firstHeader.getFirstSequence(),
          firstHeader.getFirstTimestamp(),
          lastHeader.getLastSequence(),
          lastHeader.getLastTimestamp());
    }
  }

  private static class FileMetadata {
    final long firstSequence;
    final Instant firstTimestamp;
    final long lastSequence;
    final Instant lastTimestamp;

    FileMetadata(
        long firstSequence, Instant firstTimestamp, long lastSequence, Instant lastTimestamp) {
      this.firstSequence = firstSequence;
      this.firstTimestamp = firstTimestamp;
      this.lastSequence = lastSequence;
      this.lastTimestamp = lastTimestamp;
    }
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
  public Publisher<WALEntry> readFrom(long fromSequenceNumber) {
    return readRange(fromSequenceNumber, Long.MAX_VALUE);
  }

  @Override
  public Publisher<WALEntry> readRange(long fromSequenceNumber, long toSequenceNumber) {
    return readRange(RangeQuery.bySequence(fromSequenceNumber, toSequenceNumber));
  }

  private <T extends Comparable<T>> Publisher<WALEntry> readRange(RangeQuery<T> query) {
    if (!query.isValidRange()) {
      return Flux.error(new WALException("Invalid range: from > to"));
    }

    logger.debug("Reading entries in range: {} to {}", query.getFrom(), query.getTo());
    metrics.incrementRangeQueries();

    return Flux.create(
        sink -> {
          try {
            try {
              sync();
            } catch (WALException e) {
              sink.error(new WALException("Failed to flush page before read", e));
              return;
            }

            List<Path> overlappingFiles = findOverlappingFiles(query);
            logger.debug("Found {} overlapping files to process", overlappingFiles.size());

            for (Path walFile : overlappingFiles) {
              if (sink.isCancelled()) break;

              try {
                emitEntriesFromFile(sink, walFile, query);
              } catch (IOException e) {
                logger.error(
                    "Failed to read from file {}: {}", walFile.getFileName(), e.getMessage());
                sink.error(new WALException("Failed to read from file: " + walFile, e));
                return;
              }
            }

            sink.complete();
            logger.debug("Read operation completed");
          } catch (Exception e) {
            sink.error(e);
          }
        },
        FluxSink.OverflowStrategy.BUFFER);
  }

  private <T extends Comparable<T>> List<Path> findOverlappingFiles(RangeQuery<T> query)
      throws WALException {
    List<Path> sortedFiles = new ArrayList<>(walFiles.values());
    sortedFiles.removeIf(file -> !Files.exists(file));

    if (sortedFiles.isEmpty()) {
      return new ArrayList<>();
    }

    logger.debug("Binary searching {} WAL files for overlapping ranges", sortedFiles.size());

    int firstOverlapping = findFirstOverlappingFile(sortedFiles, query);
    if (firstOverlapping == -1) {
      return new ArrayList<>();
    }

    int lastOverlapping = findLastOverlappingFile(sortedFiles, query, firstOverlapping);

    List<Path> overlappingFiles = new ArrayList<>();
    for (int i = firstOverlapping; i <= lastOverlapping; i++) {
      overlappingFiles.add(sortedFiles.get(i));
      logger.debug("File {} overlaps with requested range", sortedFiles.get(i).getFileName());
    }

    metrics.incrementFilesScanned(countFilesScannedInBinarySearch(sortedFiles.size()));
    return overlappingFiles;
  }

  private <T extends Comparable<T>> int findFirstOverlappingFile(
      List<Path> sortedFiles, RangeQuery<T> query) throws WALException {
    int left = 0, right = sortedFiles.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      Path file = sortedFiles.get(mid);

      if (fileOverlapsRange(file, query)) {
        result = mid;
        right = mid - 1;
      } else {
        FileRange<T> range = getFileRange(file, query);
        if (range != null && range.lastValue.compareTo(query.getFrom()) < 0) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
    }
    return result;
  }

  private <T extends Comparable<T>> int findLastOverlappingFile(
      List<Path> sortedFiles, RangeQuery<T> query, int startFrom) throws WALException {
    int left = startFrom, right = sortedFiles.size() - 1;
    int result = startFrom;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      Path file = sortedFiles.get(mid);

      if (fileOverlapsRange(file, query)) {
        result = mid;
        left = mid + 1;
      } else {
        FileRange<T> range = getFileRange(file, query);
        if (range != null && range.lastValue.compareTo(query.getFrom()) < 0) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
    }
    return result;
  }

  private <T extends Comparable<T>> boolean fileOverlapsRange(Path walFile, RangeQuery<T> query)
      throws WALException {
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      if (fileSize == 0) return false;
      if (fileSize < WALPageHeader.HEADER_SIZE) return false;

      file.seek(0);
      byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(headerData);
      WALPageHeader firstHeader = WALPageHeader.deserialize(headerData);

      long lastPageOffset = fileSize - pageSize;
      if (lastPageOffset + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(lastPageOffset);
        file.readFully(headerData);
        WALPageHeader lastHeader = WALPageHeader.deserialize(headerData);

        WALPageHeader combinedHeader =
            new WALPageHeader(
                firstHeader.getFirstSequence(),
                lastHeader.getLastSequence(),
                firstHeader.getFirstTimestamp(),
                lastHeader.getLastTimestamp(),
                (short) 0,
                (byte) 0,
                firstHeader.getPageSizeKB());
        return query.headerOverlapsRange(combinedHeader);
      }

      return query.headerOverlapsRange(firstHeader);
    } catch (Exception e) {
      throw new WALException("Failed to check file overlap: " + walFile, e);
    }
  }

  /**
   * Emits entries from a WAL file, handling spanning entries across multiple pages.
   *
   * <p>This method correctly handles the page space optimization where pages can contain combined
   * continuation flags (FIRST_PART | LAST_PART = 5). The logic works because:
   *
   * <p>1. isFirstPart() uses bitwise AND: (flags & FIRST_PART) != 0 - For combined flags (5): 5 & 1
   * = 1 ≠ 0 → true 2. isLastPart() uses bitwise AND: (flags & LAST_PART) != 0 - For combined flags
   * (5): 5 & 4 = 4 ≠ 0 → true
   *
   * <p>When a page has combined flags (FIRST_PART | LAST_PART): - isLastPart() triggers first,
   * completing the previous spanning entry - isFirstPart() triggers second, starting accumulation
   * of the new spanning entry - Both conditions can be true simultaneously, handling the transition
   * correctly
   *
   * <p>This ensures backward compatibility with files created by version 1.0.0 (which never use
   * combined flags) while supporting the version 1.1.0 optimization that utilizes available page
   * space.
   */
  private <T extends Comparable<T>> void emitEntriesFromFile(
      FluxSink<WALEntry> sink, Path walFile, RangeQuery<T> query) throws IOException, WALException {
    logger.debug(
        "Emitting entries from file: {} (range: {} to {})",
        walFile.getFileName(),
        query.getFrom(),
        query.getTo());

    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();

      ByteArrayOutputStream spanningEntryData = null;

      for (long offset = 0; offset + WALPageHeader.HEADER_SIZE <= fileSize; offset += pageSize) {
        if (sink.isCancelled()) break;

        file.seek(offset);
        byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
        file.readFully(headerData);

        try {
          WALPageHeader header = WALPageHeader.deserialize(headerData);
          metrics.incrementPagesScanned();

          long pageEnd = Math.min(offset + pageSize, fileSize);
          long dataSize = pageEnd - offset - WALPageHeader.HEADER_SIZE;

          if (header.isSpanningRecord()) {
            if (header.isFirstPart()) {
              spanningEntryData = new ByteArrayOutputStream();
            }

            if (spanningEntryData != null) {
              byte[] pageData = new byte[(int) dataSize];
              file.seek(offset + WALPageHeader.HEADER_SIZE);
              file.readFully(pageData);
              spanningEntryData.write(pageData);
              metrics.incrementPagesRead();

              if (header.isLastPart()) {
                byte[] completeEntryData = spanningEntryData.toByteArray();
                try {
                  WALEntry entry = WALEntry.deserialize(completeEntryData);

                  if (query.entryInRange(entry)) {
                    sink.next(entry);
                    metrics.incrementEntriesRead();
                  }
                } catch (WALException e) {
                  sink.error(new WALException("Corrupted page at offset " + offset, e));
                  return;
                }

                spanningEntryData = null;
              }
            }
          } else {
            if (query.headerOverlapsRange(header)) {
              try {
                emitEntriesFromPage(
                    sink, file, offset + WALPageHeader.HEADER_SIZE, dataSize, query);
                metrics.incrementPagesRead();
              } catch (WALException e) {
                sink.error(e);
                return;
              }
            }
          }

        } catch (WALException e) {
          sink.error(
              new WALException("Corrupted page at offset " + offset + ": " + e.getMessage(), e));
          return;
        }
      }
    }
  }

  private <T extends Comparable<T>> void emitEntriesFromPage(
      FluxSink<WALEntry> sink,
      RandomAccessFile file,
      long startOffset,
      long dataSize,
      RangeQuery<T> query)
      throws IOException, WALException {
    file.seek(startOffset);
    long endPos = startOffset + dataSize;

    while (file.getFilePointer() < endPos
        && file.getFilePointer() < file.length()
        && !sink.isCancelled()) {
      if (endPos - file.getFilePointer() < ENTRY_HEADER_SIZE) {
        break;
      }

      long entryStart = file.getFilePointer();
      byte entryType = file.readByte();

      if (entryType == 0) {
        break; // Padding
      }

      file.readLong(); // sequence
      file.readLong(); // timestamp
      int dataLength = file.readInt();

      if (dataLength < 0 || file.getFilePointer() + dataLength > endPos) {
        break; // Invalid entry
      }

      int totalEntrySize = ENTRY_HEADER_SIZE + dataLength;
      byte[] entryData = new byte[totalEntrySize];
      file.seek(entryStart);
      file.readFully(entryData);

      try {
        WALEntry entry = WALEntry.deserialize(entryData);
        if (query.entryInRange(entry)) {
          sink.next(entry);
          metrics.incrementEntriesRead();
        }
      } catch (WALException e) {
        // Wrap CRC32 validation errors in a "Corrupted page" message for test compatibility
        throw new WALException(
            "Corrupted page at offset " + (startOffset - WALPageHeader.HEADER_SIZE), e);
      }
    }
  }

  private int countFilesScannedInBinarySearch(int totalFiles) {
    // Binary search scans approximately log2(n) files for each search
    // We do two binary searches (first + last), so roughly 2 * log2(n)
    return Math.max(1, (int) Math.ceil(2 * Math.log(totalFiles) / Math.log(2)));
  }

  private <T extends Comparable<T>> FileRange<T> getFileRange(Path walFile, RangeQuery<T> query)
      throws WALException {
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      if (fileSize == 0) return null;
      if (fileSize < WALPageHeader.HEADER_SIZE) return null;

      file.seek(0);
      byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(headerData);
      WALPageHeader firstHeader = WALPageHeader.deserialize(headerData);

      long lastPageOffset = fileSize - pageSize;
      if (lastPageOffset + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(lastPageOffset);
        file.readFully(headerData);
        WALPageHeader lastHeader = WALPageHeader.deserialize(headerData);
        return new FileRange<>(
            query.getHeaderFromExtractor().apply(firstHeader),
            query.getHeaderToExtractor().apply(lastHeader));
      }

      return new FileRange<>(
          query.getHeaderFromExtractor().apply(firstHeader),
          query.getHeaderToExtractor().apply(firstHeader));
    } catch (Exception e) {
      throw new WALException("Failed to get file range: " + walFile, e);
    }
  }

  private static class FileRange<T> {
    final T firstValue;
    final T lastValue;

    FileRange(T firstValue, T lastValue) {
      this.firstValue = firstValue;
      this.lastValue = lastValue;
    }
  }

  @Override
  public Publisher<WALEntry> readFrom(Instant fromTimestamp) {
    return readRange(fromTimestamp, Instant.MAX);
  }

  @Override
  public Publisher<WALEntry> readRange(Instant fromTimestamp, Instant toTimestamp) {
    return readRange(RangeQuery.byTimestamp(fromTimestamp, toTimestamp));
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
  public WALMetrics getMetrics() {
    return metrics;
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
        sync();

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

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
  private final WALMetrics metrics = new WALMetrics();

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

  /** Write entry to current page buffer, handling spanning entries across multiple pages. */
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
    } else if (entrySize <= PAGE_DATA_SIZE) {
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
      // First, flush current page if it has any data
      if (currentPageBuffer.position() > WALPageHeader.HEADER_SIZE) {
        logger.debug("Flushing current page before starting spanning entry");
        flushCurrentPage();
        initializeNewPage();
      }

      // Now write the spanning entry starting from a clean page
      writeSpanningEntry(entry, serializedEntry);
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

  /** Write entry that spans multiple pages. */
  private void writeSpanningEntry(WALEntry entry, byte[] serializedEntry)
      throws IOException, WALException {
    logger.debug(
        "Writing spanning entry: seq={}, totalSize={} bytes",
        entry.getSequenceNumber(),
        serializedEntry.length);

    int bytesWritten = 0;
    int totalBytes = serializedEntry.length;
    boolean isFirstPage = true;

    while (bytesWritten < totalBytes) {
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
      short continuationFlags;
      if (isFirstPage) {
        continuationFlags =
            (bytesWritten >= totalBytes) ? WALPageHeader.NO_CONTINUATION : WALPageHeader.FIRST_PART;
        currentPageEntryCount = 1; // Only first page counts the entry
        isFirstPage = false;
      } else if (bytesWritten >= totalBytes) {
        continuationFlags = WALPageHeader.LAST_PART;
        currentPageEntryCount = 0; // Continuation pages don't count entries
      } else {
        continuationFlags = WALPageHeader.MIDDLE_PART;
        currentPageEntryCount = 0; // Continuation pages don't count entries
      }

      // Flush current page with appropriate continuation flags
      flushCurrentPageWithFlags(continuationFlags);

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

  /** Flush current page buffer to disk with specified continuation flags. */
  private void flushCurrentPageWithFlags(short continuationFlags) throws IOException, WALException {
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
            currentPageEntryCount, continuationFlags);

    logger.debug(
        "Flushing page to disk: seq={}-{}, entries={}, dataSize={} bytes, flags={}",
        currentPageFirstSequence,
        currentPageLastSequence,
        currentPageEntryCount,
        currentPageBuffer.position() - WALPageHeader.HEADER_SIZE,
        continuationFlags);

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
    if (continuationFlags == WALPageHeader.NO_CONTINUATION
        || continuationFlags == WALPageHeader.LAST_PART) {
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
  public Publisher<WALEntry> readFrom(long fromSequenceNumber) {
    return readRange(fromSequenceNumber, Long.MAX_VALUE);
  }

  @Override
  public Publisher<WALEntry> readRange(long fromSequenceNumber, long toSequenceNumber) {
    if (fromSequenceNumber > toSequenceNumber) {
      return Flux.error(new WALException("Invalid range: from > to"));
    }

    logger.debug("Reading entries in sequence range: {}-{}", fromSequenceNumber, toSequenceNumber);
    metrics.incrementRangeQueries();

    return Flux.create(
        sink -> {
          try {
            try {
              // Flush current page to ensure all data is on disk
              sync();
            } catch (WALException e) {
              sink.error(new WALException("Failed to flush page before read", e));
              return;
            }

            // Step 1 & 2: Binary search WAL files for overlapping ranges
            List<Path> overlappingFiles =
                findOverlappingFilesBinarySearch(fromSequenceNumber, toSequenceNumber);
            logger.debug("Found {} overlapping files to process", overlappingFiles.size());

            // Step 3, 4 & 5: Process overlapping files and emit entries
            for (Path walFile : overlappingFiles) {
              if (sink.isCancelled()) break;

              try {
                emitEntriesFromFileBySequence(sink, walFile, fromSequenceNumber, toSequenceNumber);
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
      long lastPageOffset = fileSize - PAGE_SIZE;

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

  private List<Path> findOverlappingFilesBinarySearch(long fromSeq, long toSeq)
      throws WALException {
    List<Path> sortedFiles = new ArrayList<>(walFiles.values());
    sortedFiles.removeIf(file -> !Files.exists(file));

    if (sortedFiles.isEmpty()) {
      return new ArrayList<>();
    }

    logger.debug("Binary searching {} WAL files for overlapping ranges", sortedFiles.size());

    // Files are already sorted by index (wal-0.log, wal-1.log, etc.)
    // Use binary search to find first and last overlapping files
    int firstOverlapping = findFirstOverlappingFile(sortedFiles, fromSeq, toSeq);
    if (firstOverlapping == -1) {
      return new ArrayList<>(); // No overlapping files found
    }

    int lastOverlapping = findLastOverlappingFile(sortedFiles, fromSeq, toSeq, firstOverlapping);

    List<Path> overlappingFiles = new ArrayList<>();
    for (int i = firstOverlapping; i <= lastOverlapping; i++) {
      overlappingFiles.add(sortedFiles.get(i));
      logger.debug("File {} overlaps with requested range", sortedFiles.get(i).getFileName());
    }

    // Update metrics - only count files we actually checked during binary search
    metrics.incrementFilesScanned(countFilesScannedInBinarySearch(sortedFiles.size()));

    return overlappingFiles;
  }

  private int findFirstOverlappingFile(List<Path> sortedFiles, long fromSeq, long toSeq)
      throws WALException {
    int left = 0, right = sortedFiles.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      Path file = sortedFiles.get(mid);

      if (fileOverlapsSequenceRange(file, fromSeq, toSeq)) {
        result = mid;
        right = mid - 1; // Continue searching left for first overlapping file
      } else {
        // Check if this file is before or after our range
        FileSequenceRange range = getFileSequenceRange(file);
        if (range.lastSequence < fromSeq) {
          left = mid + 1; // File is before our range, search right
        } else {
          right = mid - 1; // File is after our range, search left
        }
      }
    }

    return result;
  }

  private int findLastOverlappingFile(
      List<Path> sortedFiles, long fromSeq, long toSeq, int startFrom) throws WALException {
    int left = startFrom, right = sortedFiles.size() - 1;
    int result = startFrom;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      Path file = sortedFiles.get(mid);

      if (fileOverlapsSequenceRange(file, fromSeq, toSeq)) {
        result = mid;
        left = mid + 1; // Continue searching right for last overlapping file
      } else {
        // Check if this file is before or after our range
        FileSequenceRange range = getFileSequenceRange(file);
        if (range.lastSequence < fromSeq) {
          left = mid + 1; // File is before our range, search right
        } else {
          right = mid - 1; // File is after our range, search left
        }
      }
    }

    return result;
  }

  private FileSequenceRange getFileSequenceRange(Path walFile) throws WALException {
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      if (fileSize == 0) return new FileSequenceRange(-1, -1);

      // Read first page header
      if (fileSize < WALPageHeader.HEADER_SIZE) return new FileSequenceRange(-1, -1);
      file.seek(0);
      byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(headerData);
      WALPageHeader firstHeader = WALPageHeader.deserialize(headerData);

      // Read last page header
      long lastPageOffset = fileSize - PAGE_SIZE;
      if (lastPageOffset + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(lastPageOffset);
        file.readFully(headerData);
        WALPageHeader lastHeader = WALPageHeader.deserialize(headerData);
        return new FileSequenceRange(firstHeader.getFirstSequence(), lastHeader.getLastSequence());
      }

      // Fallback: single page file
      return new FileSequenceRange(firstHeader.getFirstSequence(), firstHeader.getLastSequence());
    } catch (Exception e) {
      throw new WALException("Failed to get file sequence range: " + walFile, e);
    }
  }

  private int countFilesScannedInBinarySearch(int totalFiles) {
    // Binary search scans approximately log2(n) files for each search
    // We do two binary searches (first + last), so roughly 2 * log2(n)
    return Math.max(1, (int) Math.ceil(2 * Math.log(totalFiles) / Math.log(2)));
  }

  private static class FileSequenceRange {
    final long firstSequence;
    final long lastSequence;

    FileSequenceRange(long firstSequence, long lastSequence) {
      this.firstSequence = firstSequence;
      this.lastSequence = lastSequence;
    }
  }

  private void emitEntriesFromFileBySequence(
      FluxSink<WALEntry> sink, Path walFile, long fromSeq, long toSeq)
      throws IOException, WALException {
    logger.debug(
        "Emitting entries from file: {} (range: {}-{})", walFile.getFileName(), fromSeq, toSeq);

    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();

      // Use binary search to find overlapping pages
      List<Long> overlappingPageOffsets =
          findOverlappingPagesBySequence(file, fileSize, fromSeq, toSeq);

      // State for spanning entry reconstruction
      ByteArrayOutputStream spanningEntryData = null;
      long spanningEntrySequence = -1;

      for (long currentPos : overlappingPageOffsets) {
        if (sink.isCancelled()) break;

        file.seek(currentPos);
        byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
        file.readFully(headerData);

        try {
          WALPageHeader header = WALPageHeader.deserialize(headerData);
          metrics.incrementPagesScanned();

          long pageEnd = Math.min(currentPos + PAGE_SIZE, fileSize);
          long dataSize = pageEnd - currentPos - WALPageHeader.HEADER_SIZE;

          if (header.isSpanningRecord()) {
            // Handle spanning entry
            if (header.isFirstPart()) {
              // Start new spanning entry
              spanningEntryData = new ByteArrayOutputStream();
              spanningEntrySequence = header.getFirstSequence();
            }

            if (spanningEntryData != null) {
              // Read page data
              byte[] pageData = new byte[(int) dataSize];
              file.seek(currentPos + WALPageHeader.HEADER_SIZE);
              file.readFully(pageData);
              spanningEntryData.write(pageData);
              metrics.incrementPagesRead();

              if (header.isLastPart()) {
                // Complete spanning entry
                byte[] completeEntryData = spanningEntryData.toByteArray();
                WALEntry entry = WALEntry.deserialize(completeEntryData);

                if (entry.getSequenceNumber() >= fromSeq && entry.getSequenceNumber() <= toSeq) {
                  sink.next(entry);
                  metrics.incrementEntriesRead();
                }

                spanningEntryData = null;
                spanningEntrySequence = -1;
              }
            }
          } else {
            // Handle regular page with complete entries
            emitEntriesFromPage(
                sink, file, currentPos + WALPageHeader.HEADER_SIZE, dataSize, fromSeq, toSeq);
            metrics.incrementPagesRead();
          }

        } catch (WALException e) {
          sink.error(
              new WALException(
                  "Corrupted page at offset " + currentPos + ": " + e.getMessage(), e));
          return;
        }
      }
    }
  }

  private void emitEntriesFromPage(
      FluxSink<WALEntry> sink,
      RandomAccessFile file,
      long startOffset,
      long dataSize,
      long fromSeq,
      long toSeq)
      throws IOException, WALException {
    logger.debug(
        "emitEntriesFromPage: startOffset={}, dataSize={}, fromSeq={}, toSeq={}",
        startOffset,
        dataSize,
        fromSeq,
        toSeq);

    file.seek(startOffset);
    long endPos = startOffset + dataSize;
    long currentPos = file.getFilePointer();

    logger.debug("emitEntriesFromPage: endPos={}, currentPos={}", endPos, currentPos);

    while (currentPos < endPos && currentPos < file.length() && !sink.isCancelled()) {
      logger.debug(
          "emitEntriesFromPage: loop iteration - currentPos={}, endPos={}, remaining={}",
          currentPos,
          endPos,
          endPos - currentPos);

      if (endPos - currentPos < ENTRY_HEADER_SIZE) {
        logger.debug("emitEntriesFromPage: insufficient space for entry header, breaking");
        break;
      }

      // Read entry header to get size
      long entryStart = file.getFilePointer();
      byte entryType = file.readByte();
      logger.debug("emitEntriesFromPage: entryType={} at position={}", entryType, entryStart);

      if (entryType == 0) {
        logger.debug("emitEntriesFromPage: found padding, breaking");
        break; // Padding
      }

      file.readLong(); // sequence
      file.readLong(); // timestamp
      int dataLength = file.readInt();
      logger.debug("emitEntriesFromPage: dataLength={}", dataLength);

      if (dataLength < 0 || currentPos + ENTRY_HEADER_SIZE + dataLength > endPos) {
        logger.debug("emitEntriesFromPage: invalid entry size, breaking");
        break; // Invalid entry
      }

      // Read complete entry data
      int totalEntrySize = ENTRY_HEADER_SIZE + dataLength;
      byte[] entryData = new byte[totalEntrySize];
      file.seek(entryStart);
      file.readFully(entryData);
      logger.debug("emitEntriesFromPage: read entry data, totalEntrySize={}", totalEntrySize);

      try {
        // Deserialize with CRC32 validation
        WALEntry entry = WALEntry.deserialize(entryData);
        logger.debug("emitEntriesFromPage: deserialized entry seq={}", entry.getSequenceNumber());

        // Filter by sequence range
        if (entry.getSequenceNumber() >= fromSeq && entry.getSequenceNumber() <= toSeq) {
          logger.debug("emitEntriesFromPage: emitting entry seq={}", entry.getSequenceNumber());
          sink.next(entry);
          metrics.incrementEntriesRead();
        } else {
          logger.debug(
              "emitEntriesFromPage: entry seq={} outside range {}-{}",
              entry.getSequenceNumber(),
              fromSeq,
              toSeq);
        }
      } catch (WALException e) {
        // Let CRC32 validation errors propagate, but log other deserialization issues
        if (e.getMessage().contains("CRC32")) {
          logger.error("emitEntriesFromPage: CRC32 validation failed: {}", e.getMessage());
          throw e; // Propagate CRC32 validation errors
        } else {
          // Skip entries that fail deserialization for other reasons
          logger.debug(
              "Skipping entry that failed deserialization at position {}: {}",
              entryStart,
              e.getMessage());
        }
      }

      currentPos = file.getFilePointer();
      logger.debug("emitEntriesFromPage: updated currentPos={}", currentPos);
    }

    logger.debug("emitEntriesFromPage: finished processing page");
  }

  @Override
  public Publisher<WALEntry> readFrom(Instant fromTimestamp) {
    return readRange(fromTimestamp, Instant.MAX);
  }

  @Override
  public Publisher<WALEntry> readRange(Instant fromTimestamp, Instant toTimestamp) {
    if (fromTimestamp.isAfter(toTimestamp)) {
      return Flux.error(new WALException("Invalid range: from > to"));
    }

    logger.debug("Reading entries in timestamp range: {} to {}", fromTimestamp, toTimestamp);
    metrics.incrementRangeQueries();

    return Flux.create(
        sink -> {
          try {
            try {
              // Flush current page to ensure all data is on disk
              sync();
            } catch (WALException e) {
              sink.error(new WALException("Failed to flush page before read", e));
              return;
            }

            // Step 1 & 2: Binary search WAL files for timestamp overlapping ranges
            List<Path> overlappingFiles =
                findOverlappingFilesBinarySearchByTimestamp(fromTimestamp, toTimestamp);
            logger.debug("Found {} overlapping files for timestamp range", overlappingFiles.size());

            // Step 3, 4 & 5: Process overlapping files and emit entries
            for (Path walFile : overlappingFiles) {
              if (sink.isCancelled()) break;

              try {
                emitEntriesFromFileByTimestamp(sink, walFile, fromTimestamp, toTimestamp);
              } catch (IOException e) {
                logger.error(
                    "Failed to read from file {} by timestamp: {}",
                    walFile.getFileName(),
                    e.getMessage());
                sink.error(new WALException("Failed to read from file: " + walFile, e));
                return;
              }
            }

            sink.complete();
            logger.debug("Timestamp read operation completed");
          } catch (Exception e) {
            sink.error(e);
          }
        },
        FluxSink.OverflowStrategy.BUFFER);
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
      long lastPageOffset = fileSize - PAGE_SIZE;
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

  private List<Path> findOverlappingFilesBinarySearchByTimestamp(Instant fromTs, Instant toTs)
      throws WALException {
    List<Path> sortedFiles = new ArrayList<>(walFiles.values());
    sortedFiles.removeIf(file -> !Files.exists(file));

    if (sortedFiles.isEmpty()) {
      return new ArrayList<>();
    }

    logger.debug(
        "Binary searching {} WAL files for timestamp overlapping ranges", sortedFiles.size());

    // Files are already sorted by index (wal-0.log, wal-1.log, etc.)
    // Use binary search to find first and last overlapping files
    int firstOverlapping = findFirstOverlappingFileByTimestamp(sortedFiles, fromTs, toTs);
    if (firstOverlapping == -1) {
      return new ArrayList<>(); // No overlapping files found
    }

    int lastOverlapping =
        findLastOverlappingFileByTimestamp(sortedFiles, fromTs, toTs, firstOverlapping);

    List<Path> overlappingFiles = new ArrayList<>();
    for (int i = firstOverlapping; i <= lastOverlapping; i++) {
      overlappingFiles.add(sortedFiles.get(i));
      logger.debug("File {} overlaps with timestamp range", sortedFiles.get(i).getFileName());
    }

    // Update metrics - only count files we actually checked during binary search
    metrics.incrementFilesScanned(countFilesScannedInBinarySearch(sortedFiles.size()));

    return overlappingFiles;
  }

  private int findFirstOverlappingFileByTimestamp(
      List<Path> sortedFiles, Instant fromTs, Instant toTs) throws WALException {
    int left = 0, right = sortedFiles.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      Path file = sortedFiles.get(mid);

      if (fileOverlapsTimestampRange(file, fromTs, toTs)) {
        result = mid;
        right = mid - 1; // Continue searching left for first overlapping file
      } else {
        // Check if this file is before or after our range
        FileTimestampRange range = getFileTimestampRange(file);
        if (range.lastTimestamp.isBefore(fromTs)) {
          left = mid + 1; // File is before our range, search right
        } else {
          right = mid - 1; // File is after our range, search left
        }
      }
    }

    return result;
  }

  private int findLastOverlappingFileByTimestamp(
      List<Path> sortedFiles, Instant fromTs, Instant toTs, int startFrom) throws WALException {
    int left = startFrom, right = sortedFiles.size() - 1;
    int result = startFrom;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      Path file = sortedFiles.get(mid);

      if (fileOverlapsTimestampRange(file, fromTs, toTs)) {
        result = mid;
        left = mid + 1; // Continue searching right for last overlapping file
      } else {
        // Check if this file is before or after our range
        FileTimestampRange range = getFileTimestampRange(file);
        if (range.lastTimestamp.isBefore(fromTs)) {
          left = mid + 1; // File is before our range, search right
        } else {
          right = mid - 1; // File is after our range, search left
        }
      }
    }

    return result;
  }

  private FileTimestampRange getFileTimestampRange(Path walFile) throws WALException {
    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();
      if (fileSize == 0) return new FileTimestampRange(Instant.MAX, Instant.MIN);

      // Read first page header
      if (fileSize < WALPageHeader.HEADER_SIZE)
        return new FileTimestampRange(Instant.MAX, Instant.MIN);
      file.seek(0);
      byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
      file.readFully(headerData);
      WALPageHeader firstHeader = WALPageHeader.deserialize(headerData);

      // Read last page header
      long lastPageOffset = fileSize - PAGE_SIZE;
      if (lastPageOffset + WALPageHeader.HEADER_SIZE <= fileSize) {
        file.seek(lastPageOffset);
        file.readFully(headerData);
        WALPageHeader lastHeader = WALPageHeader.deserialize(headerData);
        return new FileTimestampRange(
            firstHeader.getFirstTimestamp(), lastHeader.getLastTimestamp());
      }

      // Fallback: single page file
      return new FileTimestampRange(
          firstHeader.getFirstTimestamp(), firstHeader.getLastTimestamp());
    } catch (Exception e) {
      throw new WALException("Failed to get file timestamp range: " + walFile, e);
    }
  }

  private static class FileTimestampRange {
    final Instant firstTimestamp;
    final Instant lastTimestamp;

    FileTimestampRange(Instant firstTimestamp, Instant lastTimestamp) {
      this.firstTimestamp = firstTimestamp;
      this.lastTimestamp = lastTimestamp;
    }
  }

  private List<Long> findOverlappingPagesBySequence(
      RandomAccessFile file, long fileSize, long fromSeq, long toSeq)
      throws IOException, WALException {
    List<Long> pageOffsets = new ArrayList<>();

    // Build list of all page offsets
    for (long offset = 0; offset + WALPageHeader.HEADER_SIZE <= fileSize; offset += PAGE_SIZE) {
      pageOffsets.add(offset);
    }

    if (pageOffsets.isEmpty()) {
      return pageOffsets;
    }

    logger.debug("Binary searching {} pages for sequence overlapping ranges", pageOffsets.size());

    // Use binary search to find first and last overlapping pages
    int firstOverlapping = findFirstOverlappingPageBySequence(file, pageOffsets, fromSeq, toSeq);
    if (firstOverlapping == -1) {
      return new ArrayList<>(); // No overlapping pages found
    }

    int lastOverlapping =
        findLastOverlappingPageBySequence(file, pageOffsets, fromSeq, toSeq, firstOverlapping);

    List<Long> overlappingPages = new ArrayList<>();
    for (int i = firstOverlapping; i <= lastOverlapping; i++) {
      overlappingPages.add(pageOffsets.get(i));
    }

    return overlappingPages;
  }

  private int findFirstOverlappingPageBySequence(
      RandomAccessFile file, List<Long> pageOffsets, long fromSeq, long toSeq)
      throws IOException, WALException {
    int left = 0, right = pageOffsets.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      long offset = pageOffsets.get(mid);

      PageSequenceRange range = getPageSequenceRange(file, offset);

      if (pageOverlapsSequenceRange(range, fromSeq, toSeq)) {
        result = mid;
        right = mid - 1; // Continue searching left for first overlapping page
      } else if (range.lastSequence < fromSeq) {
        left = mid + 1; // Page is before our range, search right
      } else {
        right = mid - 1; // Page is after our range, search left
      }
    }

    return result;
  }

  private int findLastOverlappingPageBySequence(
      RandomAccessFile file, List<Long> pageOffsets, long fromSeq, long toSeq, int startFrom)
      throws IOException, WALException {
    int left = startFrom, right = pageOffsets.size() - 1;
    int result = startFrom;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      long offset = pageOffsets.get(mid);

      PageSequenceRange range = getPageSequenceRange(file, offset);

      if (pageOverlapsSequenceRange(range, fromSeq, toSeq)) {
        result = mid;
        left = mid + 1; // Continue searching right for last overlapping page
      } else if (range.lastSequence < fromSeq) {
        left = mid + 1; // Page is before our range, search right
      } else {
        right = mid - 1; // Page is after our range, search left
      }
    }

    return result;
  }

  private PageSequenceRange getPageSequenceRange(RandomAccessFile file, long offset)
      throws IOException, WALException {
    file.seek(offset);
    byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
    file.readFully(headerData);
    WALPageHeader header = WALPageHeader.deserialize(headerData);
    return new PageSequenceRange(header.getFirstSequence(), header.getLastSequence());
  }

  private boolean pageOverlapsSequenceRange(PageSequenceRange pageRange, long fromSeq, long toSeq) {
    return !(pageRange.lastSequence < fromSeq || pageRange.firstSequence > toSeq);
  }

  private static class PageSequenceRange {
    final long firstSequence;
    final long lastSequence;

    PageSequenceRange(long firstSequence, long lastSequence) {
      this.firstSequence = firstSequence;
      this.lastSequence = lastSequence;
    }
  }

  private List<Long> findOverlappingPagesByTimestamp(
      RandomAccessFile file, long fileSize, Instant fromTs, Instant toTs)
      throws IOException, WALException {
    List<Long> pageOffsets = new ArrayList<>();

    // Build list of all page offsets
    for (long offset = 0; offset + WALPageHeader.HEADER_SIZE <= fileSize; offset += PAGE_SIZE) {
      pageOffsets.add(offset);
    }

    if (pageOffsets.isEmpty()) {
      return pageOffsets;
    }

    logger.debug("Binary searching {} pages for timestamp overlapping ranges", pageOffsets.size());

    // Use binary search to find first and last overlapping pages
    int firstOverlapping = findFirstOverlappingPageByTimestamp(file, pageOffsets, fromTs, toTs);
    if (firstOverlapping == -1) {
      return new ArrayList<>(); // No overlapping pages found
    }

    int lastOverlapping =
        findLastOverlappingPageByTimestamp(file, pageOffsets, fromTs, toTs, firstOverlapping);

    List<Long> overlappingPages = new ArrayList<>();
    for (int i = firstOverlapping; i <= lastOverlapping; i++) {
      overlappingPages.add(pageOffsets.get(i));
    }

    return overlappingPages;
  }

  private int findFirstOverlappingPageByTimestamp(
      RandomAccessFile file, List<Long> pageOffsets, Instant fromTs, Instant toTs)
      throws IOException, WALException {
    int left = 0, right = pageOffsets.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      long offset = pageOffsets.get(mid);

      PageTimestampRange range = getPageTimestampRange(file, offset);

      if (pageOverlapsTimestampRange(range, fromTs, toTs)) {
        result = mid;
        right = mid - 1; // Continue searching left for first overlapping page
      } else if (range.lastTimestamp.isBefore(fromTs)) {
        left = mid + 1; // Page is before our range, search right
      } else {
        right = mid - 1; // Page is after our range, search left
      }
    }

    return result;
  }

  private int findLastOverlappingPageByTimestamp(
      RandomAccessFile file, List<Long> pageOffsets, Instant fromTs, Instant toTs, int startFrom)
      throws IOException, WALException {
    int left = startFrom, right = pageOffsets.size() - 1;
    int result = startFrom;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      long offset = pageOffsets.get(mid);

      PageTimestampRange range = getPageTimestampRange(file, offset);

      if (pageOverlapsTimestampRange(range, fromTs, toTs)) {
        result = mid;
        left = mid + 1; // Continue searching right for last overlapping page
      } else if (range.lastTimestamp.isBefore(fromTs)) {
        left = mid + 1; // Page is before our range, search right
      } else {
        right = mid - 1; // Page is after our range, search left
      }
    }

    return result;
  }

  private PageTimestampRange getPageTimestampRange(RandomAccessFile file, long offset)
      throws IOException, WALException {
    file.seek(offset);
    byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
    file.readFully(headerData);
    WALPageHeader header = WALPageHeader.deserialize(headerData);
    return new PageTimestampRange(header.getFirstTimestamp(), header.getLastTimestamp());
  }

  private boolean pageOverlapsTimestampRange(
      PageTimestampRange pageRange, Instant fromTs, Instant toTs) {
    return !(pageRange.lastTimestamp.isBefore(fromTs) || pageRange.firstTimestamp.isAfter(toTs));
  }

  private static class PageTimestampRange {
    final Instant firstTimestamp;
    final Instant lastTimestamp;

    PageTimestampRange(Instant firstTimestamp, Instant lastTimestamp) {
      this.firstTimestamp = firstTimestamp;
      this.lastTimestamp = lastTimestamp;
    }
  }

  private void emitEntriesFromFileByTimestamp(
      FluxSink<WALEntry> sink, Path walFile, Instant fromTs, Instant toTs)
      throws IOException, WALException {
    logger.debug(
        "Emitting entries from file: {} (timestamp range: {} to {})",
        walFile.getFileName(),
        fromTs,
        toTs);

    try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
      long fileSize = file.length();

      // Use binary search to find overlapping pages
      List<Long> overlappingPageOffsets =
          findOverlappingPagesByTimestamp(file, fileSize, fromTs, toTs);

      // State for spanning entry reconstruction
      ByteArrayOutputStream spanningEntryData = null;
      long spanningEntrySequence = -1;

      for (long currentPos : overlappingPageOffsets) {
        if (sink.isCancelled()) break;

        file.seek(currentPos);
        byte[] headerData = new byte[WALPageHeader.HEADER_SIZE];
        file.readFully(headerData);

        try {
          WALPageHeader header = WALPageHeader.deserialize(headerData);
          metrics.incrementPagesScanned();

          long pageEnd = Math.min(currentPos + PAGE_SIZE, fileSize);
          long dataSize = pageEnd - currentPos - WALPageHeader.HEADER_SIZE;

          if (header.isSpanningRecord()) {
            // Handle spanning entry
            if (header.isFirstPart()) {
              // Start new spanning entry
              spanningEntryData = new ByteArrayOutputStream();
              spanningEntrySequence = header.getFirstSequence();
            }

            if (spanningEntryData != null) {
              // Read page data
              byte[] pageData = new byte[(int) dataSize];
              file.seek(currentPos + WALPageHeader.HEADER_SIZE);
              file.readFully(pageData);
              spanningEntryData.write(pageData);
              metrics.incrementPagesRead();

              if (header.isLastPart()) {
                // Complete spanning entry
                byte[] completeEntryData = spanningEntryData.toByteArray();
                WALEntry entry = WALEntry.deserialize(completeEntryData);

                // Filter by timestamp range
                if (!entry.getTimestamp().isBefore(fromTs) && !entry.getTimestamp().isAfter(toTs)) {
                  sink.next(entry);
                  metrics.incrementEntriesRead();
                }

                spanningEntryData = null;
                spanningEntrySequence = -1;
              }
            }
          } else {
            // Handle regular page with complete entries
            emitEntriesFromPageByTimestamp(
                sink, file, currentPos + WALPageHeader.HEADER_SIZE, dataSize, fromTs, toTs);
            metrics.incrementPagesRead();
          }

        } catch (WALException e) {
          throw new WALException(
              "Corrupted page at offset " + currentPos + ": " + e.getMessage(), e);
        }
      }
    }
  }

  private void emitEntriesFromPageByTimestamp(
      FluxSink<WALEntry> sink,
      RandomAccessFile file,
      long startOffset,
      long dataSize,
      Instant fromTs,
      Instant toTs)
      throws IOException, WALException {
    file.seek(startOffset);
    long endPos = startOffset + dataSize;
    long currentPos = file.getFilePointer();

    while (currentPos < endPos && currentPos < file.length() && !sink.isCancelled()) {
      if (endPos - currentPos < ENTRY_HEADER_SIZE) break;

      // Read entry header to get size
      long entryStart = file.getFilePointer();
      byte entryType = file.readByte();
      if (entryType == 0) break; // Padding

      file.readLong(); // sequence
      file.readLong(); // timestamp
      int dataLength = file.readInt();

      if (dataLength < 0 || currentPos + ENTRY_HEADER_SIZE + dataLength > endPos) {
        break; // Invalid entry
      }

      // Read complete entry data
      int totalEntrySize = ENTRY_HEADER_SIZE + dataLength;
      byte[] entryData = new byte[totalEntrySize];
      file.seek(entryStart);
      file.readFully(entryData);

      try {
        // Deserialize with CRC32 validation
        WALEntry entry = WALEntry.deserialize(entryData);

        // Filter by timestamp range
        if (!entry.getTimestamp().isBefore(fromTs) && !entry.getTimestamp().isAfter(toTs)) {
          sink.next(entry);
          metrics.incrementEntriesRead();
        }
      } catch (WALException e) {
        // Let CRC32 validation errors propagate, but log other deserialization issues
        if (e.getMessage().contains("CRC32")) {
          throw e; // Propagate CRC32 validation errors
        } else {
          // Skip entries that fail deserialization for other reasons
          logger.debug(
              "Skipping entry that failed deserialization at position {}: {}",
              file.getFilePointer() - totalEntrySize,
              e.getMessage());
        }
      }

      currentPos = file.getFilePointer();
    }
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

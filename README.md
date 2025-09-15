# Pintor - Write Ahead Log Implementation

Pintor is a robust, thread-safe Java implementation of a Write Ahead Log (WAL) system for ensuring data durability and consistency in databases. The name "pintor" means "painter" in Spanish - a playful nod to "painting" walls (WALs) with persistent data.

## Key Features

- 🔒 **Thread-safe operations**: Atomic entry creation and batching, eliminating race conditions.
- 📁 **File-based persistence**: Automatic rotation, binary serialization with CRC32 for integrity.
- 🔄 **Recovery capabilities**: Point-in-time recovery by sequence or timestamp; persists across restarts.
- ⚡ **Performance optimizations**: Batch APIs, configurable sync, efficient seeking and truncation.
- 🧩 **Flexible design**: Generic record types, clean encapsulation.
- 🤖 **AI-assisted development**: Generated codebase for production readiness.

## Getting Started

### Prerequisites

- Java 17+
- Gradle for building and testing

### Setup

Clone the repository:

```bash
git clone https://github.com/lukaszbudnik/pintor.git
cd pintor
```

Build and test:

```bash
./gradlew build
```

## Usage

### Basic Entry Creation

```java
try (FileBasedWAL wal = new FileBasedWAL(Paths.get("/path/to/wal"))) {
    ByteBuffer data = ByteBuffer.wrap("INSERT|txn_1001|users|1|{\"name\":\"John\"}".getBytes());
    WALEntry entry = wal.createAndAppend(data);
}
```

### Batch Operations

```java
try (FileBasedWAL wal = new FileBasedWAL(Paths.get("/path/to/wal"))) {
    List<ByteBuffer> batch = Arrays.asList(
        ByteBuffer.wrap("BEGIN|txn_2001|".getBytes()),
        ByteBuffer.wrap("INSERT|txn_2001|products|1|{\"name\":\"Laptop\"}".getBytes()),
        ByteBuffer.wrap("COMMIT|txn_2001|".getBytes())
    );
    wal.createAndAppendBatch(batch);
}
```
### Reading

```java
try (FileBasedWAL wal = new FileBasedWAL(Paths.get("/path/to/wal"))) {
    // Read from sequence
    List<WALEntry> entries = wal.readFrom(0L);
    
    // Read sequence range
    List<WALEntry> rangeEntries = wal.readRange(5L, 10L);
    
    // Point-in-time recovery
    Instant recoveryPoint = Instant.parse("2025-08-28T10:30:00Z");
    List<WALEntry> timeEntries = wal.readFrom(recoveryPoint);
    
    // Read timestamp range
    Instant startTime = Instant.parse("2025-08-28T10:00:00Z");
    Instant endTime = Instant.parse("2025-08-28T11:00:00Z");
    List<WALEntry> timeRangeEntries = wal.readRange(startTime, endTime);
}
```

### Recovery

The WAL automatically recovers the last sequence number and entry count when reopened:

```java
// First session
try (FileBasedWAL wal = new FileBasedWAL(walDir)) {
    WALEntry entry = wal.createAndAppend(ByteBuffer.wrap("data".getBytes()));
    // Application crashes here
}

// Second session - automatic recovery
try (FileBasedWAL wal = new FileBasedWAL(walDir)) {
    // Automatically recovers last sequence number
    long lastSeq = wal.getCurrentSequenceNumber();
    // Continue operations with proper sequence number continuation
    WALEntry newEntry = wal.createAndAppend(ByteBuffer.wrap("recovered_data".getBytes()));
    System.out.println("New entry sequence: " + newEntry.getSequenceNumber()); // Will be lastSeq + 1
}
```

## Architecture

### Core Components

- **WALPageHeader**: POJO for the page header with sequence/timestamp ranges, entry count, and CRC32. Enables O(1) recovery and efficient range queries.
- **WALEntry**: POJO for the actual log entry. Immutable data record containing sequence number, timestamp, and user data (ByteBuffer).
- **WriteAheadLog**: Interface for WAL contracts.
- **FileBasedWAL**: File-based implementation with rotation and CRC32. Thread-safe high-level API providing entry creation, batch operations, reading, and recovery.

### Storage Format

**WAL Log Files: `wal-{index}.log`**

A WAL file, such as `wal-0.log`, is a collection of fixed-size 4KB pages. The file grows as new data is written, and a new file is created (rotated) when the size limit is reached (default: 64MB).

```
+----------------------+
|      wal-0.log       |
+----------------------+
|   WAL Page 1 (4KB)   |
+----------------------+
|   WAL Page 2 (4KB)   |
+----------------------+
|         ...          |
+----------------------+
|   WAL Page N (4KB)   |
+----------------------+
```

**Page Structure (4096 bytes):**

Each 4KB page consists of a fixed-size header and a data section that contains the WAL entries.

```
+------------------------------------------------+
|          WAL Page (4096 bytes)                 |
+------------------------------------------------+
|  +------------------------------------------+  |
|  |           Page Header (44 bytes)         |  |
|  +------------------------------------------+  |
|  |  Magic Number (4B)                       |  |
|  |  First Sequence (8B)                     |  |
|  |  Last Sequence (8B)                      |  |
|  |  First Timestamp (8B)                    |  |
|  |  Last Timestamp (8B)                     |  |
|  |  Entry Count (2B)                        |  |
|  |  Continuation Flags (2B)                 |  |
|  |  Header CRC32 (4B)                       |  |
|  +------------------------------------------+  |
|  |           Data Section (4052 bytes)      |  |
|  +------------------------------------------+  |
|  |  WAL Entry 1 (Variable Length)           |  |
|  |  WAL Entry 2 (Variable Length)           |  |
|  |  ...                                     |  |
|  |  Free Space                              |  |
|  +------------------------------------------+  |
+------------------------------------------------+
```

- **Page Header Fields (44 bytes)**:
  - Magic Number (4B) - `0xDEADBEEF` for validation
  - First Sequence (8B) - Lowest sequence number in page
  - Last Sequence (8B) - Highest sequence number in page
  - First Timestamp (8B) - Earliest entry timestamp milliseconds in page
  - Last Timestamp (8B) - Latest entry timestamp milliseconds in page
  - Entry Count (2B) - Number of entries in page
  - Continuation Flags (2B) - FIRST_PART=1, MIDDLE_PART=2, LAST_PART=4
  - Header CRC32 (4B) - Validates header integrity
- **Data Section (4052 bytes)** - Contains WAL entries or continuation data for entries spanning multiple pages (for more see: [Record Spanning Examples](#record-spanning-examples))
- **Free Space** - Unused bytes at page end, occurs when:
  - Page is flushed before completely full by either manual sync or read operations (for more see: [Page Flushing to Disk](#page-flushing-to-disk))
  - Next entry is too large to fit in remaining space

**WAL Entry Format**

Entries are stored sequentially in the data section of a page. An entry is composed of a fixed-size header and a variable-length data payload.

- Entry Type (1B) - DATA=1, CHECKPOINT=2
- Sequence Number (8B)
- Timestamp Milliseconds (8B)
- Data Length (4B)
- Data (variable)
- CRC32 (4B)

```
+------------------------------------------------+
|           WAL Entry (Variable Length)          |
+------------------------------------------------+
|  Entry Type (1B)     |  Sequence Number (8B)   |
+------------------------------------------------+
|  Timestamp (8B)      |  Data Length (4B)       |
+------------------------------------------------+
|             Data (Variable Length)             |
+------------------------------------------------+
|                   CRC32 (4B)                   |
+------------------------------------------------+
```

### Page Flushing to Disk

Pages and WAL entries are buffered in memory and written to disk when any of the following conditions occur:

1. Page becomes full - When the current page reaches 4KB capacity
2. Manual sync - When sync() method is called explicitly for durability
3. Reading from log - When any of the read*() methods are called to make sure all data is on disk
4. WAL closure - When close() method is called to ensure all data is persisted

### Record Spanning Examples

Multiple Small Entries (fit in one page):

```
Page 1: [Header: flags=0, seq=101-103][Entry seq=101][Entry seq=102][Entry seq=103][Free space]
```

Large Entry (spans 3 pages):

⚠️ Large entry is not yet implemented. Currently, Pintor throws an exception if an entry is too large to fit in a single page.

```
Page 1: [Header: flags=FIRST_PART, seq=100-100][Entry header + 4KB data][No free space]
Page 2: [Header: flags=MIDDLE_PART, seq=100-100][4KB continuation data][No free space]  
Page 3: [Header: flags=LAST_PART, seq=100-100][2KB final data][Free space]
```

Mixed Page (small entry + large entry + small entry):

⚠️ Large entry is not yet implemented. Currently, Pintor throws an exception if an entry is too large to fit in a single page.

```
Page 1: [Header: flags=FIRST_PART, seq=200-201][Entry seq=200 (1KB)][Entry seq=201 header + 3KB data][No free space]
Page 2: [Header: flags=MIDDLE_PART, seq=201-201][4KB continuation data for seq=201][No free space]
Page 3: [Header: flags=LAST_PART, seq=201-202][1KB final data for seq=201][Entry seq=202][Free space]
```

**Key Rules:**
- Every page always has a 44-byte header
- Continuation flags in header indicate spanning record parts
- Spanning records share the same sequence number across all pages
- First/Last sequence in header reflects actual entry sequences in that page
- Middle pages contain only continuation data, no new entry headers

### Recovery Process:
1. Seek to last page boundary in newest file
2. Read 44-byte page header from the last page
3. Extract sequence and timestamp metadata instantly from header
4. Check continuation flags to understand page content type
5. Total recovery time: O(1) regardless of file size

### Reading from sequence number range or timestamp range

1. Scan all WAL files: Read the first and last page headers from each WAL file
2. Filter overlapping files: Build a list of WAL files whose sequence/timestamp ranges overlap with the requested range
3. Process overlapping files: For each overlapping WAL file, read all pages
4. Skip non-overlapping pages: Within each file, skip pages whose ranges don't overlap with the requested range
5. Filter and return entries: From overlapping pages, return only WAL entries that fall within the requested range

## API Summary

Pintor is lightweight with minimal external dependencies, relying only on:
- [Project Reactor](https://projectreactor.io/) for streaming large amounts of data efficiently. All read operations return reactive `Publisher<WALEntry>`  that enable backpressure handling and prevent memory exhaustion when processing large datasets.
- [SLF4J](https://www.slf4j.org/) for structured logging and diagnostics.

### WriteAheadLog

- `WALEntry createAndAppend(ByteBuffer data)`: Create single entry from ByteBuffer.
- `List<WALEntry> createAndAppendBatch(List<ByteBuffer> dataList)`: Batch create from ByteBuffers.
- `Publisher<WALEntry> readFrom(long fromSequenceNumber)`: Read entries from sequence number.
- `Publisher<WALEntry> readFrom(Instant fromTimestamp)`: Read entries from timestamp.
- `Publisher<WALEntry> readRange(long fromSequenceNumber, long toSequenceNumber)`: Read entries in sequence range.
- `Publisher<WALEntry> readRange(Instant fromTimestamp, Instant toTimestamp)`: Read entries in timestamp range.
- `void sync()`: Force sync to disk.
- `void truncate(long upToSequenceNumber)`: Truncate log up to sequence.
- `long getCurrentSequenceNumber()`: Get last sequence number.
- `long getNextSequenceNumber()`: Get next available sequence.
- `long size()`: Get entry count.
- `boolean isEmpty()`: Check if WAL is empty.

### Configuration

```java
// Custom configuration
WriteAheadLog wal = new FileBasedWAL(
    walDirectory,
    64 * 1024 * 1024  // max file size (default: 64MB)
);

// Or use default settings
WriteAheadLog wal = new FileBasedWAL(walDirectory);
```

### Flexible Data Handling

Pintor's data entries are intentionally designed to use `ByteBuffer` to maximize flexibility. This allows developers to send any data structure - such as JSON, serialized objects, or custom binary formats - without being constrained by a rigid schema. For example, you can include custom fields like transaction IDs, operation types, or metadata in the payload. These fields can later be used for custom filtering during reads or recovery, enabling application-specific logic like transaction tracking or selective replay.

#### Example: JSON with ByteBuffer
```java
try (WriteAheadLog wal = new FileBasedWAL(Paths.get("/path/to/wal"))) {
    ByteBuffer jsonData = ByteBuffer.wrap("{\"txnId\":\"txn_4001\",\"operation\":\"INSERT\"}".getBytes());
    WALEntry entry = wal.createAndAppend(jsonData);
}
```

#### Example: Structured Data with ByteBuffer
```java
try (WriteAheadLog wal = new FileBasedWAL(Paths.get("/path/to/wal"))) {
    ByteBuffer buffer = ByteBuffer.allocate(128)
        .putInt(4001) // Transaction ID
        .put("INSERT".getBytes());
    buffer.flip();
    WALEntry entry = wal.createAndAppend(buffer);
}
```

## Contributing

We welcome contributions to make Pintor even better! Here's how:

1. Fork the repo and create a branch.
2. Follow Google Java code style.
3. Add tests for changes.
4. Submit a pull request.

Report bugs or suggest features via GitHub Issues.

## License
Apache 2.0 License - see LICENSE file.


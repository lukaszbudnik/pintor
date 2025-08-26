package com.github.lukaszbudnik.wal;

/** Exception thrown when WAL operations fail */
public class WALException extends Exception {

  public WALException(String message) {
    super(message);
  }

  public WALException(String message, Throwable cause) {
    super(message, cause);
  }

  public WALException(Throwable cause) {
    super(cause);
  }
}

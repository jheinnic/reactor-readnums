package info.jchein.apps.nr.codetest.ingest.segments.logunique;

public enum WriteFailureType
{
   EXISTS_NOT_WRITABLE("Unable to open %s for writing.  It exists, but is not writable"),
   CANNOT_CREATE("Unable to open %s for writing.  Either it is a directory, exists and is not openable for writing, or does not exist and is not creatable"),
   IO_EXCEPTION_ON_WRITE("IOException thrown while attempting to write %s at offset %d with %d bytes already written and %d bytes remaining"),
   IO_EXCEPTION_ON_CREATE("IOException thrown while attempting to create %s as a new file opened for writing"),
   IO_EXCEPTION_ON_OPEN("IOException thrown while trying to truncate and open pre-existing file at %s"),
   IO_EXCEPTION_ON_CLOSE("IOException thrown on attempting to flush buffers, release resources, and close log file previously opened at %s"),
   FILE_WAS_CLOSED("Cannot write to log file at %s because it has already been closed"),
   WRITE_RETURNS_NEGATIVE("Failed to write complete batch to %s at offset %d before FileChannel.write() signalled it could write no more.  Wrote %d bytes from the last buffer, leaving %d unwritten.");
   
   private final String formatString;

   WriteFailureType(String formatString) {
      this.formatString = formatString;
   }
   
   String getFormatString() {
      return this.formatString;
   }
}

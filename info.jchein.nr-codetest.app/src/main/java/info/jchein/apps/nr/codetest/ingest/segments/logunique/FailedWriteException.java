package info.jchein.apps.nr.codetest.ingest.segments.logunique;

import java.io.File;

public class FailedWriteException
extends RuntimeException
{
   private static final long serialVersionUID = -1749444689673235499L;

   public FailedWriteException( File outputFilePath, WriteFailureType failureType, long writeOffset, int bytesWritten, int bytesRemaining )
   {
      super(
         String.format(
            failureType.getFormatString(), outputFilePath.toString(), writeOffset, bytesWritten, bytesRemaining));
   }

   public FailedWriteException( File outputFilePath, WriteFailureType failureType )
   {
      super(
         String.format(
            failureType.getFormatString(), outputFilePath.toString()));
   }

   public FailedWriteException( File outputFilePath, WriteFailureType failureType, long writeOffset, int bytesWritten, int bytesRemaining, Throwable cause )
   {
      super(
         String.format(
            failureType.getFormatString(), outputFilePath.toString(), writeOffset, bytesWritten, bytesRemaining),
         cause);
   }

   public FailedWriteException( File outputFilePath, WriteFailureType failureType, Throwable cause )
   {
      super(
         String.format(
            failureType.getFormatString(), outputFilePath.toString()),
         cause);
   }
}

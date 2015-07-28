package info.jchein.apps.nr.codetest.ingest.messages;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;


public class WriteFileBuffer
extends AbstractReusableObject<IWriteFileBuffer>
implements IWriteFileBuffer
{
   private final ByteBuffer writeBuffer;
   private final AtomicLong nextWriteFileOffset;
   private boolean isBufferFilling;
   private long fileWriteOffset;
   private int entryCount;
   private int skipCount;


   public WriteFileBuffer(
      final OnReturnCallback releaseCallback,
      final int poolIndex,
      final AtomicLong nextWriteFileOffset,
      final int maxMessagesPerFileBuffer )
   {
      super(releaseCallback, poolIndex);

      writeBuffer = ByteBuffer.allocate(maxMessagesPerFileBuffer * Constants.FILE_ENTRY_SIZE);
      this.nextWriteFileOffset = nextWriteFileOffset;
      fileWriteOffset = -1;
      isBufferFilling = true;
      entryCount = 0;
      skipCount = 0;
   }


   @Override
   public final IWriteFileBuffer castToInterface() {
      return this;
   }


   @Override
   public void acceptUniqueInput(final byte[] message)
   {
      writeBuffer.put(message);
      entryCount++;
   }


   @Override
   public void trackSkippedDuplicate()
   {
      skipCount++;
   }


   @Override
   public int getMessageCapacity()
   {
      return writeBuffer.capacity() / Constants.FILE_ENTRY_SIZE;
   }


   @Override
   public int getMessageCapacityRemaining()
   {
      return writeBuffer.remaining() / Constants.FILE_ENTRY_SIZE;
   }


   @Override
   public int getMessageCapacityUtilized()
   {
      return (writeBuffer.capacity() - writeBuffer.remaining()) / Constants.FILE_ENTRY_SIZE;
   }


//   @Override
//   public int getByteCapacity()
//   {
//      return writeBuffer.capacity();
//   }
//
//
//   @Override
//   public int getByteCapacityRemaining()
//   {
//      return writeBuffer.remaining();
//   }


   @Override
   public WriteFileBuffer afterWrite()
   {
      fileWriteOffset = nextWriteFileOffset.getAndAdd(entryCount * Constants.FILE_ENTRY_SIZE);
      super.afterWrite();
      return this;
   }


   @Override
   public WriteFileBuffer beforeRead()
   {
      super.beforeRead();

      if (isBufferFilling == true) {
         flipBufferMode();
      }

      return this;
   }


   @Override
   public ByteBuffer getBufferToDrain()
   {
      if (isBufferFilling) {
         flipBufferMode();
      }

      return writeBuffer;
   }


   @Override
   public long getFileWriteOffset()
   {
      return fileWriteOffset;
   }


   @Override
   public ICounterIncrements loadCounterDeltas(final ICounterIncrements deltaCounters)
   {
      deltaCounters.setDeltas(entryCount, skipCount);
      return deltaCounters;
   }


//   @Override
//   public final int getEntryCount()
//   {
//      return entryCount;
//   }
//
//
//   @Override
//   public final int getSkipCount()
//   {
//      return skipCount;
//   }


   /**
    * Buffers start in write mode on allocation, then flip to read mode, then back to write mode, etc.
    */
   void flipBufferMode()
   {
      writeBuffer.flip();
      isBufferFilling = !isBufferFilling;
   }


   @Override
   public void recycle()
   {
      writeBuffer.clear();
      isBufferFilling = true;
      fileWriteOffset = -1;
      entryCount = 0;
      skipCount = 0;
   }


   @Override
   protected String innerToString()
   {
      return new StringBuilder()
      .append("LocalWriteBuffer [fileWriteOffset=")
      .append(fileWriteOffset)
      .append(", entryCount=")
      .append(entryCount)
      .append(", skipCount=")
      .append(skipCount)
      .append(", isBufferFilling=")
      .append(isBufferFilling)
      .append(", buffer.position()=")
      .append(writeBuffer.position())
      .append(", buffer.limit()=")
      .append(writeBuffer.limit())
//      .append(", getByteCapacity()=")
//      .append(getByteCapacity())
      .append(", getMessageCapacity()=")
      .append(getMessageCapacity())
//      .append(", getByteCapacityRemaining()=")
//      .append(getByteCapacityRemaining())
      .append(", getMessageCapacityRemaining()=")
      .append(getMessageCapacityRemaining())
      .append(", getMessageCapacityUtilized()=")
      .append(getMessageCapacityUtilized())
      .append("]")
      .toString();
   }
}



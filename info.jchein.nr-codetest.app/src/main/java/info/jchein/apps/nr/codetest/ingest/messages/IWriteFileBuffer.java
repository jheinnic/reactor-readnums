package info.jchein.apps.nr.codetest.ingest.messages;


import java.nio.ByteBuffer;

import info.jchein.apps.nr.codetest.ingest.reusable.IReusable;


public interface IWriteFileBuffer extends IReusable
{
   public void acceptUniqueInput(byte[] message);


   public void trackSkippedDuplicate();


   public int getMessageCapacity();


   public int getMessageCapacityRemaining();


   public int getMessageCapacityUtilized();


   /*
   public int getByteCapacity();


   public int getByteCapacityRemaining();
   */


   @Override
   public IWriteFileBuffer afterWrite();


   @Override
   public IWriteFileBuffer beforeRead();

   /**
    * Arranges internal buffer for use writing to an output file, with position=0 and limit=entryCount*entrySize.
    *
    * @return
    */
   public ByteBuffer getBufferToDrain();


   public long getFileWriteOffset();


//   public int getEntryCount();
//
//
//   public int getSkipCount();


   /**
    * Adds the content of accepted/skipped counters stored in an instance of this interface to corresponding counter
    * values encapsulated by argument <code>deltaCounters</code>
    *
    * It is extremely important to call {@link #beforeRead()} to ensure visibility and sanity checking have occurred
    * BEFORE calling this method!!
    *
    * @param deltaContainer
    *
    * @see #beforeRead()
    */
   public ICounterIncrements loadCounterDeltas(ICounterIncrements deltaCounters);
}
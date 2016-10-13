package info.jchein.apps.nr.codetest.ingest.messages;

import java.nio.ByteBuffer;

import info.jchein.apps.nr.codetest.ingest.reusable.IReusable;


public interface IRawInputBatch
extends IReusable
{
   public void acceptUniqueInput(int message);


   public void trackSkippedDuplicate();

   
	/**
	 * Write sync method invoked by thread that uses {@link #acceptUniqueInput(int)} and {@link #trackSkippedDuplicate()}
	 * to populate an instance of this class with a batch of message content and a duplicate reject count for the same
	 * time period when the encapsulated messages were queued.
	 * 
	 * This method must be called before {@link #transferToFileBuffer(IWriteFileBuffer)} or
	 * {@link #loadCounterDeltas(ICounterIncrements)} are called by any other thread.
	 */
   public IRawInputBatch afterWrite();


   /**
    * Transfers the content of implementing instance to parameter <code>buf</code>'s encapsulated {@link ByteBuffer}.
    *
    * It is extremely important to call {@link #beforeRead()} to ensure visibility and sanity checking have occurred
    * BEFORE calling this method!!
    *
    * @param buf {@link IWriteFileBuffer} whose {@link ByteBuffer} will be loaded with the transformed content of
    *             implementing instance's int[] batch contents.
    * @return True if contents of the implementing instance's int array were successfully converted to bytes and
    * loaded into <code>buf</code>'s {@link ByteBuffer}, or false if there was insufficient capacity left to
    * attempt requested transfer.
    *
    * @see #beforeRead()
    */
   public boolean transferToFileBuffer(IWriteFileBuffer buf);


   /**
	 * Adds the content of accepted/skipped counters stored in an instance of this interface to corresponding counter
	 * values encapsulated by argument <code>deltaCounters</code>
	 *
	 * It is extremely important to call {@link #transferToFileBuffer(IWriteFileBuffer)} before this method because that
	 * method is where the read visibility assurance is made. BEFORE calling this method!!
	 * 
	 * @param deltaContainer
	 *           An uncommitted counter collector that will be populated and marked for a quick return trip.
	 *
	 * @see #transferToFileBuffer(IWriteFileBuffer)
	 */
   public void loadCounterDeltas(ICounterIncrements deltaCounters);


   /**
    *
    */
   public int getEntryCount();

}

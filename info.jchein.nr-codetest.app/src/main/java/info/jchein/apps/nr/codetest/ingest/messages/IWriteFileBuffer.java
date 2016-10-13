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


   public IWriteFileBuffer afterWrite();


	/**
	 * Called after any other thread invokes {@link #afterWrite()} to ensure visibility on and gain access to buffer with
	 * bytes to flush to output file at offset retrievable through {@link #getFileWriteOffset()} after this method
	 * returns.
	 *
	 * @return A ByteBuffer configured for reading from its first byte with content that is to be transferred to an
	 *         output file at the offset given by {@link #getFileWriteOffset()}
	 * @see {@link #afterWrite()}
	 * @see {@link #getFileWriteOffset()}
	 */
	public ByteBuffer getByteBufferToFlush();


   public long getFileWriteOffset();


   /**
	 * Adds the content of accepted/skipped counters stored in an instance of this interface to corresponding counter
	 * values encapsulated by argument <code>deltaCounters</code>
	 *
	 * It is extremely important to call {@link #getByteBufferToFlush()} to ensure visibility and sanity checking have
	 * occurred BEFORE calling this method. Additionally, the caller should have completed the write operation defined by
	 * this instance before calling this method.
	 *
	 * @param deltaContainer
	 *           A reserved counter container that has not yet had values committed.
	 *
	 * @see #beforeRead()
	 */
   public ICounterIncrements loadCounterDeltas(ICounterIncrements deltaCounters);
}
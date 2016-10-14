package info.jchein.apps.nr.codetest.ingest.messages;


import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;


/**
 * Collects accepted payload messages in the most compact form available, an array of integers.
 *
 * Once each data partition has populated its share worth of inputs in a RawInputBatch, the thread used to merge the
 * per-partition flows will consolidate its own RawInputBatch with those from every other data partition thread by
 * using them to populate ad ByteBuffer suitable for file I/O and adding their accepted/rejected counters to reach
 * an overall pair of accepted/rejected counters for the entire window.  The result is then passed to the output file
 * reporter for writing and incrementing the authoritative application counters.
 *
 * @author John Heinnickel
 */
public class RawInputBatch
extends AbstractReusableObject<IRawInputBatch, RawInputBatch>
implements IRawInputBatch
{
   final int[] batchBuffer;
   final byte[] xferBytes;
   private int entryCount;
   private int skipCount;

   private static final byte BYTE_0 = 48;
   private static final int[] DIVISORS = { 100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10 };
	private static final int MESSAGE_SIZE_MINUS_ONE = Constants.VALID_MESSAGE_SIZE - 1;

   public RawInputBatch( final OnReturnCallback releaseCallback, final int poolIndex, final int flushAfterNInputs )
   {
      super(releaseCallback, poolIndex);
      batchBuffer = new int[flushAfterNInputs+1];
      entryCount = 0;
      skipCount = 0;

      // When allocating a temporary 9-byte array for loading a write buffer, realize that because this
      // will be done by a single thread, the preallocated array can be populated with the delimiter bytes
      // one time here without having to re-copy them later. Sweet!
      xferBytes = new byte[Constants.FILE_ENTRY_SIZE];
      int jj = Constants.FILE_ENTRY_SIZE - Constants.DELIMITER_SIZE;
      for (int ii = 0; ii < Constants.DELIMITER_SIZE; ii++, jj++) {
         xferBytes[jj] = Constants.DELIMITER_BYTES[ii];
      }
   }


   @Override
   public final IRawInputBatch castToInterface() {
      return this;
   }


   @Override
   public void acceptUniqueInput(final int message)
   {
      batchBuffer[entryCount++] = message;
   }


   @Override
   public void trackSkippedDuplicate()
   {
      skipCount++;
   }


   void getBytesFromInt( final int candidate )
   {
      assert candidate > 0;
      assert candidate < Constants.UNIQUE_POSSIBLE_MESSAGE_COUNT;

      int bitSource = candidate;
      for (int ii = 0; ii < MESSAGE_SIZE_MINUS_ONE; ii++) {
         xferBytes[ii] = (byte) (BYTE_0 + (bitSource / DIVISORS[ii]));
         bitSource = bitSource % DIVISORS[ii];
      }
      xferBytes[MESSAGE_SIZE_MINUS_ONE] = (byte) (BYTE_0 + bitSource);

   }


	@Override
	public RawInputBatch afterWrite()
	{
		batchBuffer[entryCount] = Integer.MIN_VALUE;
		return super.afterWrite();
	}


   /**
    * If <code>buf</code> has capacity remaining for the contents of this batch object, <code>transferToFileBuffer(buf)<code>
    * will populate <code>buf</code>'s ByteBuffer with the batch contents and return the number of bytes the write actually
    * consumed.  If there is insufficient capacity, nothing will be transferred to <code>buf</code>'s ByteBuffer, and
    * transferToFileBuffer() will return -1.
    *
    * @param buf
    */
   @Override
   public boolean transferToFileBuffer(final IWriteFileBuffer buf)
   {
		super.beforeRead();
      final int capacity = buf.getMessageCapacityRemaining();
      if (entryCount > capacity)
         return false;
      else {
			// final ByteBuffer bbuf = buf.getBufferForFilling(entryCount, skipCount);
			// final ByteBuffer bbuf = buf.getByteBufferToFlush();
         for (int ii=0; ii<entryCount; ii++) {
            getBytesFromInt(batchBuffer[ii]);
				buf.acceptUniqueInput(xferBytes);
         }
			buf.trackSkippedDuplicates(skipCount);
      }

      return true;
   }


   @Override
   public int getEntryCount()
   {
      return entryCount;
   }


   @Override
	public void loadCounterDeltas(final ICounterIncrements deltaContainer)
	{
		deltaContainer.setDeltas(entryCount, skipCount);
	}


	@Override
   public void recycle()
   {
      // Saving time by allowing the buffer to remain dirty, since the "entryCount" value serves as a
      // lastUsed index, we always know where to start writing and how far to read until.
      entryCount = 0;
      skipCount = 0;
   }


   @Override
   protected String innerToString()
   {
      return new StringBuilder()
      .append("RawInputBatch [entryCount=")
      .append(entryCount)
      .append(", skipCount=")
      .append(skipCount)
      .append("]")
      .toString();
   }
}

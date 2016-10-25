package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.messages.MessageInput;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;


public final class InputMessageCodec
extends Codec<Buffer, MessageInput, MessageInput>
{
   // private static final Logger LOG = LoggerFactory.getLogger(InputMessageCodec.class);

   private static final byte[] TERMINATE = "terminate".getBytes();
   private static final Buffer EMPTY_BUFFER = Buffer.wrap(new byte[0]);

   private static final byte BYTE_0 = 48;
	// private static final byte BYTE_9 = 57;
   private static final byte BYTE_NL = 13;

	private static final byte INT_MSGBUF_1s = 8;
	private static final byte INT_MSGBUF_10s = 7;
	private static final byte INT_MSGBUF_100s = 6;
	private static final byte INT_MSGBUF_1000s = 5;
	private static final byte INT_MSGBUF_10000s = 4;
	private static final byte INT_MSGBUF_100000s = 3;
	private static final byte INT_MSGBUF_1000000s = 2;
	private static final byte INT_MSGBUF_10000000s = 1;
	private static final byte INT_MSGBUF_100000000s = 0;

	private static final byte INT_MESSAGE_1s = 8;
	private static final byte INT_MESSAGE_10s = 7;
	private static final byte INT_MESSAGE_100s = 6;
	private static final byte INT_MESSAGE_1000s = 5;
	private static final byte INT_MESSAGE_10000s = 4;
	private static final byte INT_MESSAGE_100000s = 3;
	private static final byte INT_MESSAGE_1000000s = 2;
	private static final byte INT_MESSAGE_10000000s = 1;
	private static final byte INT_MESSAGE_100000000s = 0;

	private static final byte INT_PREFIX_1s = 5;
	private static final byte INT_PREFIX_10s = 4;
	private static final byte INT_PREFIX_100s = 3;
	private static final byte INT_PREFIX_1000s = 2;
	private static final byte INT_PREFIX_10000s = 1;
	private static final byte INT_PREFIX_100000s = 0;

	private static final byte INT_SUFFIX_1s = 2;
	private static final byte INT_SUFFIX_10s = 1;
	private static final byte INT_SUFFIX_100s = 0;

	private static final byte SHORT_SUFFIX_1s = 2;
	private static final byte SHORT_SUFFIX_10s = 1;
	private static final byte SHORT_SUFFIX_100s = 0;
	// private static final byte SHORT_SUFFIX_1000s = 0;

   // Eliminate the need for multiplication when mapping a nine digit byte sequence to an integer by indexing partial
   // sums from a static finally created table generated once on class loading MessageUtils.
	private static final int[][] INT_MESSAGE_PLACES = new int[9][10];

	// Indices for prefix/suffix extraction to an int value
	private static final int[][] INT_PREFIX_PLACES = new int[6][10];
	private static final int[][] INT_SUFFIX_PLACES = new int[3][10];

	// To avoid converting indices 6, 7, and 8 to 0, 1, and 2, this array makes a tradeoff
	// by allocating unused data cells for first-dimension indices 0 through 5.
	private static final short[][] SHORT_SUFFIX_PLACES = new short[3][10];


	static {
      int intPlaceBase = 1;
      int intPlaceValue = 0;
		for (int ii = INT_MESSAGE_1s; ii >= 0; ii--, intPlaceBase = intPlaceValue, intPlaceValue = 0) {
         for (int jj = 0; jj < 10; jj++, intPlaceValue += intPlaceBase) {
            INT_MESSAGE_PLACES[ii][jj] = intPlaceValue;
         }
      }

		INT_PREFIX_PLACES[INT_PREFIX_100000s] = INT_MESSAGE_PLACES[INT_MESSAGE_100000s];
		INT_PREFIX_PLACES[INT_PREFIX_10000s] = INT_MESSAGE_PLACES[INT_MESSAGE_10000s];
		INT_PREFIX_PLACES[INT_PREFIX_1000s] = INT_MESSAGE_PLACES[INT_MESSAGE_1000s];
		INT_PREFIX_PLACES[INT_PREFIX_100s] = INT_MESSAGE_PLACES[INT_MESSAGE_100s];
		INT_PREFIX_PLACES[INT_PREFIX_10s] = INT_MESSAGE_PLACES[INT_MESSAGE_10s];
		INT_PREFIX_PLACES[INT_PREFIX_1s] = INT_MESSAGE_PLACES[INT_MESSAGE_1s];

		INT_SUFFIX_PLACES[INT_SUFFIX_100s] = INT_MESSAGE_PLACES[INT_MESSAGE_100s];
		INT_SUFFIX_PLACES[INT_SUFFIX_10s] = INT_MESSAGE_PLACES[INT_MESSAGE_10s];
		INT_SUFFIX_PLACES[INT_SUFFIX_1s] = INT_MESSAGE_PLACES[INT_MESSAGE_1s];

		// Rather than copy, recalculate the short values. The reduced value range makes
		// copying from source arrays a challenge not worth resolving to save O(n^2) for
		// n === 3 during one time initialization.
      short shortPlaceBase = 1;
      short shortPlaceValue = 0;
		for (int ii = SHORT_SUFFIX_1s; ii >= SHORT_SUFFIX_100s; ii--, shortPlaceBase =
			shortPlaceValue, shortPlaceValue = 0)
		{
         for (int jj = 0; jj < 10; jj++, shortPlaceValue += shortPlaceBase) {
            SHORT_SUFFIX_PLACES[ii][jj] = shortPlaceValue;
         }
      }
   }

	private final short[][] suffixPartitionPlaces;
	private final byte[] partitionCycles;


	// private static final class AllocatedBatch<T extends IReusable> {
	// private final IReusableAllocator<T> reusableAllocator;
	// private final ArrayList<T> currentAllocation;
	// private Iterator<T> currentIterator;
	//
	// AllocatedBatch(final int allocationSize, final IReusableAllocator<T> reusableAllocator) {
	// this.reusableAllocator = reusableAllocator;
	// this.currentAllocation = new ArrayList<>(allocationSize);
	// this.currentIterator = this.currentAllocation.iterator();
	// }
	//
	// public boolean isEmpty()
	// {
	// return !currentIterator.hasNext();
	// }
	//
	// public void renewAllocation(
	// final int batchAllocationSize,
	// final IReusableAllocator<T> reusableAllocator)
	// {
	// this.currentAllocation.clear();
	// this.currentAllocation.ensureCapacity(batchAllocationSize);
	// reusableAllocator.allocateBatch(
	// batchAllocationSize, currentAllocation);
	// this.currentIterator = currentAllocation.iterator();
	// }
	//
	// public T allocateNext()
	// {
	// final T retVal = currentIterator.next();
	//
	// Verify.verifyNotNull(retVal, "Null allocation?");
	// Verify.verify(
	// retVal.getReferenceCount() == 1,
	// "Reference count was %s, not 1, for %s",
	// retVal.getReferenceCount(), retVal);
	// // Verify.verify(retVal.getPrefix() == Integer.MIN_VALUE, "%s", retVal);
	//
	// return retVal;
	// }
	// }



	public InputMessageCodec(final byte dataPartitionCount) {
      super();
		this.suffixPartitionPlaces = new short[3][10];
		for (int ii = SHORT_SUFFIX_1s; ii >= SHORT_SUFFIX_100s; ii--) {
			for (int jj = 0; jj < 10; jj++) {
				this.suffixPartitionPlaces[ii][jj] = (short) (SHORT_SUFFIX_PLACES[ii][jj] % dataPartitionCount);
			}
		}
      
		final int maxSuffixModSum = dataPartitionCount * 3;
		this.partitionCycles = new byte[maxSuffixModSum];
		for (int ii = 0, kk = 0; ii < 3; ii++) {
			for (byte jj = 0; jj < dataPartitionCount; jj++, kk++) {
				this.partitionCycles[kk] = jj;
			}
		}
   }


   @Override
   public Function<Buffer,MessageInput> decoder(final Consumer<MessageInput> next)
   {
      final byte[] bytes9 = new byte[Constants.VALID_MESSAGE_SIZE];
      final byte[] bytes10 = new byte[Constants.WIN_ALT_VALID_MESSAGE_SIZE];
      final Function<Buffer, MessageInput> retFn;

      // The two variants below differ only by whether they invoke next.accept() or not with their calculated result.
      // It's a lot to repeat, but it spares us an redundant if(next == null) check inside the returned function and
      // allows us to instead perform that instruction just once, here. The things we do in the name of performance...
      //
      // Each step in the series of if/else blocks below applies a validity or a message type Predicate to the current
      // constraints, then moves on towards message's source Buffer.  The net effect is a process of elimination that
      // first removes the most readily checked Invalid Message direct semantic checks.
     if (next == null) {
         retFn = (final Buffer b) -> {
            final int bytesRemaining = b.remaining();

            // LOG.info("Decoding {}", b);
            // First eliminate message size overflow or underflow.
            if (bytesRemaining < Constants.VALID_MESSAGE_SIZE)
               return getMessageInputUnderflow();
            else if (bytesRemaining == Constants.WIN_ALT_VALID_MESSAGE_SIZE) {
               b.read(bytes10);
               if (bytes10[Constants.VALID_MESSAGE_SIZE] != BYTE_NL)
                  return getMessageInputOverflow();
					else 
						return identifyBuffer(bytes10);
            } else if (bytesRemaining > Constants.VALID_MESSAGE_SIZE)
               return getMessageInputOverflow();
            else {
               b.read(bytes9);
					return identifyBuffer(bytes9);
            }
         };
      } else {
         retFn = (final Buffer b) -> {
            final int bytesRemaining = b.remaining();

            // LOG.info("Decoding {}", b);
            // First eliminate message size overflow or underflow.
            if (bytesRemaining < Constants.VALID_MESSAGE_SIZE) {
               next.accept(getMessageInputUnderflow());
            } else if (bytesRemaining == Constants.WIN_ALT_VALID_MESSAGE_SIZE) {
               b.read(bytes10);
					if (bytes10[Constants.VALID_MESSAGE_SIZE] != BYTE_NL)
						next.accept(getMessageInputOverflow());
					else 
						next.accept(identifyBuffer(bytes10));
				} else if (bytesRemaining > Constants.VALID_MESSAGE_SIZE)
					next.accept(getMessageInputOverflow());
				else {
               b.read(bytes9);
					next.accept(identifyBuffer(bytes9));
            }

            return null;
         };
      }

      return retFn;
   }


	private MessageInput identifyBuffer(final byte[] bytes9)
	{
		MessageInput candidate;
		candidate =
			((candidate = isTerminateMsg(bytes9)) != null)
				? candidate
				: ((candidate = ifNineDigitMessage(bytes9)) != null)
					? candidate
					: getMessageInputInvalidContent();
		return candidate;
	}


   MessageInput ifNineDigitMessage(final byte[] msgBuf)
   {
		final byte ones = (byte) (msgBuf[INT_MSGBUF_1s] - BYTE_0);
		final byte tens = (byte) (msgBuf[INT_MSGBUF_10s] - BYTE_0);
		final byte huns = (byte) (msgBuf[INT_MSGBUF_100s] - BYTE_0);
      
   	final short suffix = (short) (
   		SHORT_SUFFIX_PLACES[SHORT_SUFFIX_1s][ones] +
			SHORT_SUFFIX_PLACES[SHORT_SUFFIX_10s][tens] +
			SHORT_SUFFIX_PLACES[SHORT_SUFFIX_100s][huns]);

		// Every thread in the input batch pool will examine this message, and only one will accept it. For this to work,
		// the message must be retained long enough for every thread to have a chance to see it, not only the one that
		// accepts it.  So increase the retention count to match the number of data partitions.  Each thread must then
		// release it once whether it accepts it for further processing or not or realizes it belongs to a different
		// partition and rejects it.
		final byte partitionIndex =
			this.partitionCycles[
			   this.suffixPartitionPlaces[SHORT_SUFFIX_1s][ones] +
				this.suffixPartitionPlaces[SHORT_SUFFIX_10s][tens] +
				this.suffixPartitionPlaces[SHORT_SUFFIX_100s][huns]];

		return MessageInput.builder()
			.messageBytes(msgBuf)
			.suffix(suffix)
			.partitionIndex(partitionIndex)
			.build();
   }


   /**
    * Allocate a message from the ReusableAllocation object pool's pre-reserved batch, refilling it with
    * new content if necessary.
    * @return
    */
	// MessageInput allocateNextMessage()
	// {
	// final AllocatedBatch<MessageInput> reservations =
	// preAllocatedMessages.get();
	// if (reservations.isEmpty()) {
	// reservations.renewAllocation(
	// nineDigitBatchAllocationSize, MessageInputAllocator);
	// }
	// return reservations.allocateNext();
	// }


	/**
	 * Convert a valid NINE_DIGITS message buffer to its integer value. This method must ONLY be called with inputs that
	 * satisfy a constraint where {@link #isValidMsg(byte[])} == {@link MessageSyntaxType#NINE_DIGITS}
	 *
	 * @param msgBuf
	 * @return
	 */
	public static int parseMessage(final byte[] msgBuf)
	{
		return 
			INT_MESSAGE_PLACES[INT_MESSAGE_1s][msgBuf[INT_MSGBUF_1s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_10s][msgBuf[INT_MSGBUF_10s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_100s][msgBuf[INT_MSGBUF_100s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_1000s][msgBuf[INT_MSGBUF_1000s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_10000s][msgBuf[INT_MSGBUF_10000s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_100000s][msgBuf[INT_MSGBUF_100000s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_1000000s][msgBuf[INT_MSGBUF_1000000s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_10000000s][msgBuf[INT_MSGBUF_10000000s] - BYTE_0] +
			INT_MESSAGE_PLACES[INT_MESSAGE_100000000s][msgBuf[INT_MSGBUF_100000000s] - BYTE_0];
//		int retVal = 0;
//		for (byte ii = 8; ii >= 0; ii--) {
//			final byte nextByte = msgBuf[ii];
//			if ((nextByte < BYTE_0) || (nextByte > BYTE_9)) return Integer.MIN_VALUE;
//			else {
//				retVal += INT_MESSAGE_PLACES[ii][nextByte - BYTE_0];
//			}
//		}
//
//		return retVal;
	}


   /**
    * Given just the first six digits of a 9-digit sequence, return its decimal equivalent.
    *
    * prefix[0] == fullMessage[0]  : 100,000's digit (was 100,000,000's digit)
    * prefix[1] == fullMessage[1]  : 10,000's digit  (was 10,000,000's digit)
    * prefix[2] == fullMessage[2]  : 1,000's digit   (was 1,000,000's digit)
    * prefix[3] == fullMessage[3]  : 100's digit     (was 100,000's digit)
    * prefix[4] == fullMessage[4]  : 10's digit      (was 10,000's digit)
    * prefix[5] == fullMessage[5]  : 1's digit       (was 1,000's digit)
    *
    * @param prefix
    * @return
    */
   public static int parsePrefix(final byte[] msgBuf)
   {
		// Similar to parseMessage, but uses an array that has been shifted over by 3 places to
		// essentially divide by 1000 while decoding.
		return 
			INT_PREFIX_PLACES[INT_PREFIX_1s][msgBuf[INT_MSGBUF_1000s] - BYTE_0] +
			INT_PREFIX_PLACES[INT_PREFIX_10s][msgBuf[INT_MSGBUF_10000s] - BYTE_0] +
			INT_PREFIX_PLACES[INT_PREFIX_100s][msgBuf[INT_MSGBUF_100000s] - BYTE_0] +
			INT_PREFIX_PLACES[INT_PREFIX_1000s][msgBuf[INT_MSGBUF_1000000s] - BYTE_0] +
			INT_PREFIX_PLACES[INT_PREFIX_10000s][msgBuf[INT_MSGBUF_10000000s] - BYTE_0] +
			INT_PREFIX_PLACES[INT_PREFIX_100000s][msgBuf[INT_MSGBUF_100000000s] - BYTE_0];
   }


   /**
    * Given just the last three digits of a 9-digit sequence, return its decimal equivalent.
    *
    * suffix[0] == fullMessage[6] suffix[1] == fullMessage[7] suffix[2] == fullMessage[8]
    *
    * @param prefix
    * @return
    */
	public static short parseSuffix(final byte[] msgBuf)
   {
   	return (short) (
   		SHORT_SUFFIX_PLACES[SHORT_SUFFIX_1s][msgBuf[INT_MSGBUF_1s] - BYTE_0] +
			SHORT_SUFFIX_PLACES[SHORT_SUFFIX_10s][msgBuf[INT_MSGBUF_10s] - BYTE_0] +
			SHORT_SUFFIX_PLACES[SHORT_SUFFIX_100s][msgBuf[INT_MSGBUF_100s] - BYTE_0]);
   }


	/**
	 * Given just the last three digits of a 9-digit sequence, return its decimal equivalent modulo the data partition
	 * count.
	 *
	 * suffix[0] == fullMessage[6]
	 * suffix[1] == fullMessage[7]
	 * suffix[2] == fullMessage[8]
	 *
	 * @param prefix
	 * @return
	 */
	byte parsePartitionIndex(final byte[] msgBuf)
	{
		return this.partitionCycles[
		   this.suffixPartitionPlaces[SHORT_SUFFIX_1s][msgBuf[INT_MSGBUF_1s] - BYTE_0] +
			this.suffixPartitionPlaces[SHORT_SUFFIX_10s][msgBuf[INT_MSGBUF_10s] - BYTE_0] +
			this.suffixPartitionPlaces[SHORT_SUFFIX_100s][msgBuf[INT_MSGBUF_100s] - BYTE_0]];
	}


   /**
    * Test whether a known valid message buffer contains a terminate message. This method must ONLY be called with
    * inputs that satisfy a constraint where {@link #isValidMsg(byte[])} == {@link MessageSyntaxType#NINE_DIGITS}
    *
    * @param msgBuf
    *           A valid 9-byte message array (or 10-byte with trailing newline on Windows)
    * @return true if msgBuf contains 'terminate', false otherwise.
    */
   public static MessageInput isTerminateMsg(final byte[] msgBuf)
   {
      for (int ii=Constants.VALID_MESSAGE_SIZE-1; ii>=0; ii--) {
         if (TERMINATE[ii] != msgBuf[ii]) return null;
      }
      return TERMINATE_FLYWEIGHT;
   }


   // Only 9-digit messages are dynamically allocated because they are the only ones that truly need to be stateful.
   // Invalid Error Codes and the Terminate Command are carried by pre-fabricated Flyweight objects not associated with
   // an active {@link ReusableObjectAllocator}, and therefore non-Recyclable.
	private static final MessageInput TERMINATE_FLYWEIGHT = MessageInput.terminate();
	private static final MessageInput INVALID_UNDERFLOW = MessageInput.invalidInput();
	private static final MessageInput INVALID_OVERFLOW = MessageInput.invalidInput();
	private static final MessageInput INVALID_CONTENT = MessageInput.invalidInput();

   static MessageInput getMessageInputUnderflow()
   {
      return INVALID_UNDERFLOW;
   }


   static MessageInput getMessageInputOverflow()
   {
      return INVALID_OVERFLOW;
   }


   static MessageInput getMessageInputInvalidContent()
   {
      return INVALID_CONTENT;
   }


   @Override
   public Buffer apply(final MessageInput t)
   {
      return EMPTY_BUFFER;
   }
}

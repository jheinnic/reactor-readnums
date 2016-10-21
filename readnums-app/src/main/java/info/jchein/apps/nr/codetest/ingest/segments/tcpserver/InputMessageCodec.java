package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import static info.jchein.apps.nr.codetest.ingest.messages.IInputMessage.MessageKind.INVALID_INPUT;
import static info.jchein.apps.nr.codetest.ingest.messages.IInputMessage.MessageKind.TERMINATE_CMD;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import info.jchein.apps.nr.codetest.ingest.messages.InputMessage;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;

public final class InputMessageCodec
extends Codec<Buffer, IInputMessage, IInputMessage>
{
   // private static final Logger LOG = LoggerFactory.getLogger(InputMessageCodec.class);

   private static final byte[] TERMINATE = "terminate".getBytes();
   private static final Buffer EMPTY_BUFFER = Buffer.wrap(new byte[0]);

   private static final byte BYTE_0 = 48;
   private static final byte BYTE_9 = 57;
   private static final byte BYTE_NL = 13;

   private static final byte INT_PLACE_1s = 8;
	private static final byte INT_PLACE_10s = 7;
	private static final byte INT_PLACE_100s = 6;
   private static final byte INT_PLACE_1000s = 5;
   // private static final byte INT_PLACE_10000s = 4;
   // private static final byte INT_PLACE_100000s = 3;
   // private static final byte INT_PLACE_1000000s = 2;
   // private static final byte INT_PLACE_10000000s = 1;
   // private static final byte INT_PLACE_100000000s = 0;

   // private static final byte SHORT_PLACE_1s = 2;
   // private static final byte SHORT_PLACE_10s = 1;
   // private static final byte SHORT_PLACE_100s = 0;
   // private static final byte SHORT_PLACE_1000s = 0;

   // Eliminate the need for multiplication when mapping a nine digit byte sequence to an integer by indexing partial
   // sums from a static finally created table generated once on class loading MessageUtils.
	private static final int[][] INT_DECIMAL_PLACES = new int[9][10];
	private static final int[][] INT_PREFIX_PLACES = new int[6][10];

	// To avoid converting indices 6, 7, and 8 to 0, 1, and 2, this array makes a tradeoff
	// by allocating unused data cells for first-dimension indices 0 through 5.
	private static final short[][] SHORT_SUFFIX_PLACES = new short[9][10];


	static {
      int intPlaceBase = 1;
      int intPlaceValue = 0;
      for (int ii = INT_PLACE_1s; ii >= 0; ii--, intPlaceBase = intPlaceValue, intPlaceValue = 0) {
         for (int jj = 0; jj < 10; jj++, intPlaceValue += intPlaceBase) {
            INT_DECIMAL_PLACES[ii][jj] = intPlaceValue;
         }
      }

      INT_PREFIX_PLACES[0] = INT_DECIMAL_PLACES[3];
      INT_PREFIX_PLACES[1] = INT_DECIMAL_PLACES[4];
      INT_PREFIX_PLACES[2] = INT_DECIMAL_PLACES[5];
      INT_PREFIX_PLACES[3] = INT_DECIMAL_PLACES[6];
      INT_PREFIX_PLACES[4] = INT_DECIMAL_PLACES[7];
      INT_PREFIX_PLACES[5] = INT_DECIMAL_PLACES[8];

      short shortPlaceBase = 1;
      short shortPlaceValue = 0;
		for (int ii = INT_PLACE_1s; ii >= (INT_PLACE_1000s + 1); ii--, shortPlaceBase =
			shortPlaceValue, shortPlaceValue = 0)
		{
         for (int jj = 0; jj < 10; jj++, shortPlaceValue += shortPlaceBase) {
            SHORT_SUFFIX_PLACES[ii][jj] = shortPlaceValue;
         }
      }
   }

   private final IReusableAllocator<IInputMessage> inputMessageAllocator;
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



   public InputMessageCodec(
   	final byte dataPartitionCount,
		final IReusableAllocator<IInputMessage> inputMessageAllocator )
   {
      super();
      this.inputMessageAllocator = inputMessageAllocator;
		this.suffixPartitionPlaces = new short[9][10];
		for (int ii = INT_PLACE_1s; ii > INT_PLACE_1000s; ii--) {
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
   public Function<Buffer,IInputMessage> decoder(final Consumer<IInputMessage> next)
   {
      final byte[] bytes9 = new byte[Constants.VALID_MESSAGE_SIZE];
      final byte[] bytes10 = new byte[Constants.WIN_ALT_VALID_MESSAGE_SIZE];
      final Function<Buffer, IInputMessage> retFn;

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
            IInputMessage candidate;

            // LOG.info("Decoding {}", b);
            // First eliminate message size overflow or underflow.
            if (bytesRemaining < Constants.VALID_MESSAGE_SIZE)
               return getInputMessageUnderflow();
            else if (bytesRemaining == Constants.WIN_ALT_VALID_MESSAGE_SIZE) {
               b.read(bytes10);
               if (bytes10[Constants.VALID_MESSAGE_SIZE] != BYTE_NL)
                  return getInputMessageOverflow();
               else if ((candidate = isTerminateMsg(bytes10)) != null)
                  return candidate;
               else {
                  if ((candidate = ifNineDigitMessage(bytes10)) != null) return candidate;
                  else
                     return getInputMessageInvalidContent();
               }
            } else if (bytesRemaining > Constants.VALID_MESSAGE_SIZE)
               return getInputMessageOverflow();
            else {
               b.read(bytes9);
               if ((candidate = isTerminateMsg(bytes9)) != null)
                  return candidate;
               else {
                  if ((candidate = ifNineDigitMessage(bytes9)) != null) return candidate;
                  else
                     return getInputMessageInvalidContent();
               }
            }
         };
      } else {
         retFn = (final Buffer b) -> {
            final int bytesRemaining = b.remaining();
            IInputMessage candidate;

            // LOG.info("Decoding {}", b);
            // First eliminate message size overflow or underflow.
            if (bytesRemaining < Constants.VALID_MESSAGE_SIZE) {
               next.accept(getInputMessageUnderflow());
            } else if (bytesRemaining == Constants.WIN_ALT_VALID_MESSAGE_SIZE) {
               b.read(bytes10);
               if (bytes10[Constants.VALID_MESSAGE_SIZE] != BYTE_NL) {
                  next.accept(getInputMessageOverflow());
               } else if ((candidate = isTerminateMsg(bytes10)) != null) {
                  next.accept(candidate);
               } else if ((candidate = ifNineDigitMessage(bytes10)) != null) {
                  next.accept(candidate);
               } else {
                  next.accept(getInputMessageInvalidContent());
               }
            } else if (bytesRemaining > Constants.VALID_MESSAGE_SIZE) {
               next.accept(getInputMessageOverflow());
            } else {
               b.read(bytes9);
               if ((candidate = isTerminateMsg(bytes9)) != null) {
                  next.accept(candidate);
               } else if ((candidate = ifNineDigitMessage(bytes9)) != null) {
                  next.accept(candidate);
               } else {
                  next.accept(getInputMessageInvalidContent());
               }
            }

            return null;
         };
      }

      return retFn;
   }


   IInputMessage ifNineDigitMessage(final byte[] msgBuf)
   {
		final byte ones = (byte) (msgBuf[INT_PLACE_1s] - BYTE_0);
		final byte tens = (byte) (msgBuf[INT_PLACE_10s] - BYTE_0);
		final byte huns = (byte) (msgBuf[INT_PLACE_100s] - BYTE_0);
      
   	final short suffix = (short) (
   		SHORT_SUFFIX_PLACES[INT_PLACE_1s][ones] +
			SHORT_SUFFIX_PLACES[INT_PLACE_10s][tens] +
			SHORT_SUFFIX_PLACES[INT_PLACE_100s][huns]);

		// Every thread in the input batch pool will examine this message, and only one will accept it. For this to work,
		// the message must be retained long enough for every thread to have a chance to see it, not only the one that
		// accepts it.  So increase the retention count to match the number of data partitions.  Each thread must then
		// release it once whether it accepts it for further processing or not or realizes it belongs to a different
		// partition and rejects it.
		final byte partitionIndex =
			this.partitionCycles[
				this.suffixPartitionPlaces[INT_PLACE_1s][ones] +
				this.suffixPartitionPlaces[INT_PLACE_10s][tens] +
				this.suffixPartitionPlaces[INT_PLACE_100s][huns]];

		return this.inputMessageAllocator.allocate()
			.setMessagePayload(msgBuf, suffix, partitionIndex);
   }


   /**
    * Allocate a message from the ReusableAllocation object pool's pre-reserved batch, refilling it with
    * new content if necessary.
    * @return
    */
	// IInputMessage allocateNextMessage()
	// {
	// final AllocatedBatch<IInputMessage> reservations =
	// preAllocatedMessages.get();
	// if (reservations.isEmpty()) {
	// reservations.renewAllocation(
	// nineDigitBatchAllocationSize, inputMessageAllocator);
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
      int retVal = 0;
      for( byte ii=8; ii>= 0; ii-- ) {
         final byte nextByte = msgBuf[ii];
         if ((nextByte < BYTE_0) || (nextByte > BYTE_9))
            return Integer.MIN_VALUE;
         else {
            retVal += INT_DECIMAL_PLACES[ii][nextByte - BYTE_0];
         }
      }

      return retVal;
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
      // Similar to parseIntValue, but uses an array that has been shifted over by 3 places to
      // essentially divide by 1000 while decoding.
      int retVal = 0;
      for( byte ii=INT_PLACE_1000s; ii>= 0; ii-- ) {
         final byte nextByte = msgBuf[ii];
         if ((nextByte < BYTE_0) || (nextByte > BYTE_9))
            return Integer.MIN_VALUE;
         else {
            retVal += INT_PREFIX_PLACES[ii][nextByte - BYTE_0];
         }
      }

      return retVal;
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
   		SHORT_SUFFIX_PLACES[INT_PLACE_1s][msgBuf[INT_PLACE_1s]] +
			SHORT_SUFFIX_PLACES[INT_PLACE_10s][msgBuf[INT_PLACE_10s]] +
			SHORT_SUFFIX_PLACES[INT_PLACE_100s][msgBuf[INT_PLACE_100s]]);
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
			this.suffixPartitionPlaces[INT_PLACE_1s][msgBuf[INT_PLACE_1s]] +
			this.suffixPartitionPlaces[INT_PLACE_10s][msgBuf[INT_PLACE_10s]] +
			this.suffixPartitionPlaces[INT_PLACE_100s][msgBuf[INT_PLACE_100s]]];
	}


   /**
    * Test whether a known valid message buffer contains a terminate message. This method must ONLY be called with
    * inputs that satisfy a constraint where {@link #isValidMsg(byte[])} == {@link MessageSyntaxType#NINE_DIGITS}
    *
    * @param msgBuf
    *           A valid 9-byte message array (or 10-byte with trailing newline on Windows)
    * @return true if msgBuf contains 'terminate', false otherwise.
    */
   public static IInputMessage isTerminateMsg(final byte[] msgBuf)
   {
      for (int ii=Constants.VALID_MESSAGE_SIZE-1; ii>=0; ii--) {
         if (TERMINATE[ii] != msgBuf[ii]) return null;
      }
      return TERMINATE_FLYWEIGHT;
   }


   // Only 9-digit messages are dynamically allocated because they are the only ones that truly need to be stateful.
   // Invalid Error Codes and the Terminate Command are carried by pre-fabricated Flyweight objects not associated with
   // an active {@link ReusableObjectAllocator}, and therefore non-Recyclable.
   private static final IInputMessage TERMINATE_FLYWEIGHT = new InputMessage(TERMINATE_CMD);
   private static final IInputMessage INVALID_UNDERFLOW = new InputMessage(INVALID_INPUT);
   private static final IInputMessage INVALID_OVERFLOW = new InputMessage(INVALID_INPUT);
   private static final IInputMessage INVALID_CONTENT = new InputMessage(INVALID_INPUT);

   static IInputMessage getInputMessageUnderflow()
   {
      return INVALID_UNDERFLOW;
   }


   static IInputMessage getInputMessageOverflow()
   {
      return INVALID_OVERFLOW;
   }


   static IInputMessage getInputMessageInvalidContent()
   {
      return INVALID_CONTENT;
   }


   @Override
   public Buffer apply(final IInputMessage t)
   {
      return EMPTY_BUFFER;
   }
}

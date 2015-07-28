package info.jchein.apps.nr.codetest.ingest.messages;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;

public class InputMessage
extends AbstractReusableObject<IInputMessage>
implements IInputMessage
{
   private static final OnReturnCallback FLYWEIGHT_ON_RETURN_PLACEHOLDER = (final long l) -> { };
   private static final int FLYWEIGHT_POOL_INDEX = -905;

   private final IInputMessage.MessageKind kind;
   private final byte[] messageBytes;
   private int prefix;
   private short suffix;
   private byte partitionIndex;

   /**
    * Create a NINE_DIGITS message.
    *
    * NINE_DIGITS messages must be allocated from a ReusableObjectPool that is responsible for creating them because they require assigning state.
    *
    * @param onReturn
    * @param poolIndex
    */
   public InputMessage( final OnReturnCallback onReturn, final int poolIndex )
   {
      super(onReturn, poolIndex);
      kind = IInputMessage.MessageKind.NINE_DIGITS;
      messageBytes = new byte[Constants.FILE_ENTRY_SIZE];
      System.arraycopy(Constants.DELIMITER_BYTES, 0, messageBytes, Constants.VALID_MESSAGE_SIZE, Constants.DELIMITER_SIZE);
      prefix = Integer.MIN_VALUE;
      suffix = Short.MIN_VALUE;
      partitionIndex = Byte.MIN_VALUE;
   }


   /**
    * Create a TERMINATE_CMD or an INVALID_INPUT message.
    *
    * TERMINATE_CMD and INVALID_INPUT objects are immutable fly-weights.  They must are not created by a ReusableObjectAllocator.
    * @return
    */
   public InputMessage( final IInputMessage.MessageKind messageKind ) {
      super( FLYWEIGHT_ON_RETURN_PLACEHOLDER, FLYWEIGHT_POOL_INDEX );
      kind = messageKind;
      messageBytes = null;
      prefix = Integer.MAX_VALUE;
      suffix = Short.MAX_VALUE;
      partitionIndex = Byte.MAX_VALUE;
   }


   @Override
   public final IInputMessage castToInterface() {
      return this;
   }


   @Override
   public final IInputMessage.MessageKind getKind()
   {
      return kind;
   }


   @Override
   public final byte[] getMessageBytes() {
      return messageBytes;
   }


   @Override
   public final int getMessage()
   {
      return (prefix*1000) + suffix;
   }


   @Override
   public final int getPrefix()
   {
      return prefix;
   }


   @Override
   public final short getSuffix()
   {
      return suffix;
   }


   @Override
   public final byte getPartitionIndex()
   {
      return partitionIndex;
   }


   @Override
   public final void setMessagePayload(
      final byte[] bytes, final int messagePrefix, final short messageSuffix, final byte numPartitions )
   {
      Preconditions.checkState(
         kind == IInputMessage.MessageKind.NINE_DIGITS,
         "Only NINE_DIGITS message types acquired from a ReusableObjectAllocator support assignable state");

      System.arraycopy(bytes, 0, messageBytes, 0, Constants.VALID_MESSAGE_SIZE);
      prefix = messagePrefix;
      suffix = messageSuffix;
      partitionIndex = (byte) (messageSuffix % numPartitions);
   }


   @Override
   public final void recycle()
   {
      Arrays.fill(messageBytes, (byte) 0);
      prefix = Integer.MIN_VALUE;
      suffix = Short.MIN_VALUE;
      partitionIndex = Byte.MIN_VALUE;
   }


   @Override
   protected final String innerToString()
   {
      if (kind == IInputMessage.MessageKind.NINE_DIGITS) return
         new StringBuilder()
         .append("InputMessage [kind=")
         .append(kind)
         .append(", prefix=")
         .append(prefix)
         .append(", suffix=")
         .append(suffix)
         .append(", partitionIndex=")
         .append(partitionIndex)
         .append("]")
         .toString();
      else
         return
            new StringBuilder()
            .append("InputMessage [kind=")
            .append(kind)
            .append("]")
            .toString();
   }
}

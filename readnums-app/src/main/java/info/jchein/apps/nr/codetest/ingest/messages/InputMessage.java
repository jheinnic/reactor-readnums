package info.jchein.apps.nr.codetest.ingest.messages;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableObjectInternal;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.InputMessageCodec;

public class InputMessage
extends AbstractReusableObject<IInputMessage, InputMessage>
implements IInputMessage
{
	private static final OnReturnCallback FLYWEIGHT_ON_RETURN_PLACEHOLDER =
		(final IReusableObjectInternal<?> l) -> { /* No Operation */ };
   private static final int FLYWEIGHT_POOL_INDEX = -905;
	private static final byte[] MESSAGE_RESET_BYTES = new byte[Constants.FILE_ENTRY_SIZE];


	static {
		Arrays.fill(MESSAGE_RESET_BYTES, (byte) 0);
	}

   private final IInputMessage.MessageKind kind;
   private final byte[] messageBytes;
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
		suffix = Short.MIN_VALUE;
		partitionIndex = Byte.MIN_VALUE;
      messageBytes = new byte[Constants.FILE_ENTRY_SIZE];
		Arrays.fill(messageBytes, (byte) 0);
		System.arraycopy(
			Constants.DELIMITER_BYTES, 0, messageBytes,
			Constants.VALID_MESSAGE_SIZE, Constants.DELIMITER_SIZE);
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
		suffix = Short.MIN_VALUE;
		partitionIndex = Byte.MIN_VALUE;
   }


	private static final String TYPE_DISPLAY_NAME = InputMessage.class.getSimpleName();


	@Override
	protected String getTypeName()
	{
		return TYPE_DISPLAY_NAME;
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
   public final int getPrefix()
   {
		return InputMessageCodec.parsePrefix(this.messageBytes);
   }


   @Override
	public final short getSuffix()
	{
		return this.suffix;
	}


	@Override
   public final byte getPartitionIndex()
   {
      return partitionIndex;
   }


	@Override
	public InputMessage beforeRead()
	{
		return super.beforeRead();
	}


   @Override
	public final InputMessage setMessagePayload(
   	final byte[] bytes, final short messageSuffix, final byte partitionIndex)
   {
      Preconditions.checkState(
			this.kind == IInputMessage.MessageKind.NINE_DIGITS,
         "Only NINE_DIGITS message types acquired from a ReusableObjectAllocator support assignable state");
		System.arraycopy(bytes, 0, this.messageBytes, 0, Constants.VALID_MESSAGE_SIZE);
		this.suffix = messageSuffix;
		this.partitionIndex = partitionIndex;
		return super.afterWrite();
   }


   @Override
   public final void recycle()
   {
		assert this.kind == IInputMessage.MessageKind.NINE_DIGITS;
		this.suffix = Short.MIN_VALUE;
		this.partitionIndex = Byte.MIN_VALUE;
		System.arraycopy(MESSAGE_RESET_BYTES, 0, this.messageBytes, 0, Constants.VALID_MESSAGE_SIZE);
   }


   @Override
   protected final String innerToString()
   {
		if (this.kind == IInputMessage.MessageKind.NINE_DIGITS)
			return new StringBuilder()
				.append("InputMessage [kind=")
				.append(this.kind)
				.append(", prefix=")
				.append(
					InputMessageCodec.parsePrefix(
						this.messageBytes))
				.append(", suffix=")
				.append(this.suffix)
				.append(", partitionIndex=")
				.append(this.partitionIndex)
				.append(", messageBytes=")
				.append(
					Arrays.toString(this.messageBytes))
				.append("]")
				.toString();
      else
      	return new StringBuilder()
      		.append("InputMessage [kind=")
            .append(this.kind)
            .append("]")
            .toString();
   }
}

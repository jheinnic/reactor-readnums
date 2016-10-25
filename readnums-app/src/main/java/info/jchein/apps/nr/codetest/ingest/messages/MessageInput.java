package info.jchein.apps.nr.codetest.ingest.messages;


import java.util.Arrays;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import info.jchein.apps.nr.codetest.ingest.config.Constants;


@AutoValue
public abstract class MessageInput
{
	// static final Logger LOG = LoggerFactory.getLogger(MessageInput.class);

	// @Override
	// public final MessageInput castToInterface() {
	// return this;
	// }

	public static enum MessageKind
	{
		NINE_DIGITS,
		TERMINATE_CMD,
		INVALID_INPUT;
	}


	// @Override
	public abstract MessageKind getKind();


	// @Override
	public abstract byte[] getMessageBytes();


	// @Override
	// public abstract int getPrefix();


	// @Override
	public abstract short getSuffix();


	// @Override
	public abstract byte getPartitionIndex();


	public abstract Builder toBuilder();


	public static Builder builder()
	{
		final Builder retVal = new AutoValue_MessageInput.Builder();
		retVal.kind(MessageKind.NINE_DIGITS);
		return retVal;
	}


	public static final MessageInput INVALID_INPUT =
 builder().kind(MessageKind.INVALID_INPUT)
		.messageBytes(new byte[0])
		// .prefix(-1)
		.suffix((short) -1)
		.partitionIndex((byte) -1)
		.build();

	public static final MessageInput TERMINATE_CMD =
 builder().kind(MessageKind.TERMINATE_CMD)
		.messageBytes(new byte[0])
		// .prefix(-1)
		.suffix((short) -1)
		.partitionIndex((byte) -1)
		.build();

	public static MessageInput invalidInput()
	{
		return INVALID_INPUT;
	}


	public static MessageInput terminate()
	{
		return TERMINATE_CMD;
	}


	@AutoValue.Builder
	public abstract static class Builder
	{
		abstract Builder kind(MessageKind value);


		abstract MessageKind getKind();

		public abstract Builder messageBytes(byte[] value);

		abstract byte[] getMessageBytes();

		public abstract Builder suffix(short value);

		abstract short getSuffix();

		public abstract Builder partitionIndex(byte value);

		abstract byte getPartitionIndex();

		abstract MessageInput auto_build();

		public MessageInput build()
		{
			if (getKind() == MessageKind.NINE_DIGITS) {
				byte[] origBuf = getMessageBytes();
				byte[] msgBuf = Arrays.copyOf(origBuf, Constants.FILE_ENTRY_SIZE);
				System.arraycopy(Constants.DELIMITER_BYTES, 0, msgBuf, Constants.VALID_MESSAGE_SIZE,
					Constants.DELIMITER_SIZE);
				messageBytes(msgBuf);
				// LOG.info("Orig: {}, Actual: {}", Arrays.toString(origBuf), Arrays.toString(msgBuf));
			}
			return auto_build();
		}

		public MessageInput safeBuild()
		{
			Preconditions.checkState(getMessageBytes() != null);
			// final int prefix = getPrefix();
			// Preconditions.checkState(prefix >= 0);
			// Preconditions.checkState(prefix <= 999999);
			
			final short suffix = getSuffix();
			Preconditions.checkState(suffix >= 0);
			Preconditions.checkState(suffix <= 999);
			
			return build();
		}
	}
}

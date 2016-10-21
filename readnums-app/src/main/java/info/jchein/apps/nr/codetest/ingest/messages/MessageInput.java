package info.jchein.apps.nr.codetest.ingest.messages;

import com.google.auto.value.AutoValue;


@AutoValue
public abstract class MessageInput
implements IInputMessage
{
	// @Override
	// public final IInputMessage castToInterface() {
	// return this;
	// }

   @Override
	public abstract IInputMessage.MessageKind getKind();

   @Override
	public abstract byte[] getMessageBytes();

   @Override
	public abstract int getPrefix();

   @Override
	public abstract short getSuffix();

	@Override
	public abstract byte getPartitionIndex();
}

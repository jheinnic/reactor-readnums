package info.jchein.apps.nr.codetest.ingest.messages;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;


public class WriteFileBuffer
extends AbstractReusableObject<IWriteFileBuffer, WriteFileBuffer>
implements IWriteFileBuffer
{
	private final ByteBuffer writeBuffer;
	private final AtomicLong nextWriteFileOffset;
	private boolean isBufferFilling;
	private long fileWriteOffset;
	private int entryCount;
	private int skipCount;


	public WriteFileBuffer( final OnReturnCallback releaseCallback, final int poolIndex,
		final AtomicLong nextWriteFileOffset, final int maxMessagesPerFileBuffer )
	{
		super(releaseCallback, poolIndex);

		writeBuffer = ByteBuffer.allocate(maxMessagesPerFileBuffer * Constants.FILE_ENTRY_SIZE);
		this.nextWriteFileOffset = nextWriteFileOffset;
		fileWriteOffset = -1;
		isBufferFilling = true;
		entryCount = 0;
		skipCount = 0;
	}


	@Override
	public final IWriteFileBuffer castToInterface()
	{
		return this;
	}


	@Override
	public void acceptUniqueInput(final byte[] message)
	{
		writeBuffer.put(message);
		entryCount++;
	}


	@Override
	public void trackSkippedDuplicate()
	{
		skipCount++;
	}


	@Override
	public int getMessageCapacity()
	{
		return writeBuffer.capacity() / Constants.FILE_ENTRY_SIZE;
	}


	@Override
	public int getMessageCapacityRemaining()
	{
		return writeBuffer.remaining() / Constants.FILE_ENTRY_SIZE;
	}


	@Override
	public int getMessageCapacityUtilized()
	{
		return (writeBuffer.capacity() - writeBuffer.remaining()) / Constants.FILE_ENTRY_SIZE;
	}


	// @Override
	// public int getByteCapacity()
	// {
	// return writeBuffer.capacity();
	// }
	//
	//
	// @Override
	// public int getByteCapacityRemaining()
	// {
	// return writeBuffer.remaining();
	// }

	@Override
	public WriteFileBuffer afterWrite()
	{
		fileWriteOffset = nextWriteFileOffset.getAndAdd(entryCount * Constants.FILE_ENTRY_SIZE);
		isBufferFilling = false;
		super.afterWrite();
		return this;
	}


	@Override
	public ByteBuffer getByteBufferToFlush()
	{
		// Must ensure read visibility before checking the flag that dictates whether or not the
		// buffer has had all content loaded into it and is ready to be flipped for reading.
		super.beforeRead();
		assert(!isBufferFilling);
		return (ByteBuffer) writeBuffer.flip();
	}


	@Override
	public long getFileWriteOffset()
	{
		return fileWriteOffset;
	}


	@Override
	public ICounterIncrements loadCounterDeltas(final ICounterIncrements deltaCounters)
	{
		return deltaCounters.setDeltas(entryCount, skipCount);
	}


	@Override
	public void recycle()
	{
		writeBuffer.clear();
		isBufferFilling = true;
		fileWriteOffset = -1;
		entryCount = 0;
		skipCount = 0;
	}


	@Override
	protected String innerToString()
	{
		return new StringBuilder().append("WriteFileBuffer [fileWriteOffset=")
			.append(fileWriteOffset)
			.append(", entryCount=")
			.append(entryCount)
			.append(", skipCount=")
			.append(skipCount)
			.append(", isBufferFilling=")
			.append(isBufferFilling)
			.append(", buffer.position()=")
			.append(writeBuffer.position())
			.append(", buffer.limit()=")
			.append(writeBuffer.limit())
			.append(", getMessageCapacity()=")
			.append(getMessageCapacity())
			.append(", getMessageCapacityRemaining()=")
			.append(getMessageCapacityRemaining())
			.append(", getMessageCapacityUtilized()=")
			.append(getMessageCapacityUtilized())
			.append("]")
			.toString();
	}
}

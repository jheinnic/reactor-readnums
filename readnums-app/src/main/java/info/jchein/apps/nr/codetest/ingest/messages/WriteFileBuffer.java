package info.jchein.apps.nr.codetest.ingest.messages;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;


public class WriteFileBuffer
extends AbstractReusableObject<IWriteFileBuffer, WriteFileBuffer>
implements IWriteFileBuffer
{
	static final Logger LOG = LoggerFactory.getLogger(WriteFileBuffer.class);

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


	private static final String TYPE_DISPLAY_NAME = WriteFileBuffer.class.getSimpleName();


	@Override
	protected String getTypeName()
	{
		return TYPE_DISPLAY_NAME;
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
	public void trackSkippedDuplicates(final int skipCount)
	{
		this.skipCount += skipCount;
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

	@Override
	public WriteFileBuffer afterWrite()
	{
		this.isBufferFilling = false;
		super.afterWrite();
		// LOG.info("After write file buffer after write: {}", this);
		return this;
	}


	@Override
	public ByteBuffer getByteBufferToFlush()
	{
		// Must ensure read visibility before checking the flag that dictates whether or not the
		// buffer has had all content loaded into it and is ready to flip for reading.
		// LOG.info("Before read file buffer: {}", this);
		super.beforeRead();
		assert(!isBufferFilling);
		this.fileWriteOffset = this.nextWriteFileOffset.get();
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
		this.nextWriteFileOffset.getAndAdd(this.entryCount * Constants.FILE_ENTRY_SIZE);
		this.writeBuffer.clear();
		this.isBufferFilling = true;
		this.fileWriteOffset = -1;
		this.entryCount = 0;
		this.skipCount = 0;
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

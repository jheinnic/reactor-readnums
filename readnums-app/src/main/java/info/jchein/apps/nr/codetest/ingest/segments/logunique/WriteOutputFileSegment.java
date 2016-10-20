package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import org.reactivestreams.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.ICounterIncrements;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.core.Dispatcher;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.Streams;


/**
 * Reactive consumer utility for writing batches of fixed sized byte arrays to a file with each entry followed by a
 * native line separator character.
 *
 * @author John
 */
public class WriteOutputFileSegment
extends AbstractSegment
implements IStatsProviderSupplier
{
	private static final Logger LOG = LoggerFactory.getLogger(WriteOutputFileSegment.class);

	private final File outputLogFile;
	private final short concurrentFileWriters;
	private final BatchInputSegment batchInputSegment;
	private final Dispatcher writeOutputFileDispatcher;
	private final Processor<IWriteFileBuffer, IWriteFileBuffer> writeOutputFileWorkProcessor;
	private final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator;

	private FileOutputStream outputFileStream;
	private Stream<Stream<ICounterIncrements>> reportCounterIncrementsStream;


	public WriteOutputFileSegment( final String outputFilePath, final short concurrentFileWriters,
		final EventBus eventBus, final BatchInputSegment batchInputSegment,
		final Dispatcher writeOutputFileDispatcher,
		final Processor<IWriteFileBuffer, IWriteFileBuffer> writeOutputFileWorkProcessor,
		final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator )
	{
		super(eventBus);
		this.concurrentFileWriters = concurrentFileWriters;
		this.outputLogFile = new File(outputFilePath);
		this.batchInputSegment = batchInputSegment;
		this.writeOutputFileDispatcher = writeOutputFileDispatcher;
		this.writeOutputFileWorkProcessor = writeOutputFileWorkProcessor;
		this.counterIncrementsAllocator = counterIncrementsAllocator;
	}


	@Override
	public int getPhase()
	{
		return 500;
	}


	@Override
	protected Function<Event<Long>, Boolean> doStart()
	{
		LOG.info("Output writer is opening output file");
		openFile();
		LOG.info("Output file is available for writing");

		if (concurrentFileWriters > 1) {
			final ArrayList<Stream<ICounterIncrements>> partitions =
				new ArrayList<>(concurrentFileWriters);

			for (byte ii = 0; ii < concurrentFileWriters; ii++) {
				final Stream<IWriteFileBuffer> partitionedStreamRoot =
					batchInputSegment.getLoadedWriteFileBufferStream()
						.process(writeOutputFileWorkProcessor);
				partitions.add(partitionedStreamRoot.map(writeBuffer -> processBatch(writeBuffer)));
			}
			reportCounterIncrementsStream = Streams.from(partitions);
		} else {
			reportCounterIncrementsStream = batchInputSegment.getLoadedWriteFileBufferStream()
				.process(writeOutputFileWorkProcessor)
				.map(writeBuffer -> processBatch(writeBuffer))
				.nest();
		}

		return evt -> Boolean.TRUE;
	}


	public Stream<Stream<ICounterIncrements>> getReportCounterIncrementsStream()
	{
		return reportCounterIncrementsStream;
	}


	void openFile()
	{
		try {
			outputFileStream = new FileOutputStream(outputLogFile, false);
			LOG.info("Output file stream to {} open for writing", outputLogFile);
		}
		catch (final FileNotFoundException e) {
			if (outputLogFile.exists())
				throw new FailedWriteException(outputLogFile, WriteFailureType.EXISTS_NOT_WRITABLE, e);
			else throw new FailedWriteException(outputLogFile, WriteFailureType.CANNOT_CREATE, e);
		}
	}


	/**
	 * Flush a buffer containing some fixed size entries to the output log, with a native line separator between each
	 * entry.
	 *
	 * It is the caller's responsibility to ensure that the ByteBuffer received has been flipped or is otherwise
	 * configured such that its position points at the first byte to write and its limit points at the last.
	 *
	 * @param accepted
	 */
	ICounterIncrements processBatch(final IWriteFileBuffer batchDef)
	{
		final ByteBuffer buf = batchDef.getByteBufferToFlush();
		long writeOffset = batchDef.getFileWriteOffset();

		try {
			int bytesWritten = 0;
			short passCount = 0;
			final FileChannel outputChannel = outputFileStream.getChannel();
			while ((bytesWritten >= 0) && buf.hasRemaining()) {
				bytesWritten = outputChannel.write(buf, writeOffset);
				if (bytesWritten > 0) {
					writeOffset += bytesWritten;
				}
				passCount++;
			}

			if ((passCount > 1) && LOG.isInfoEnabled()) {
				LOG.info(
					String.format(
						"Wrote %d bytes in %d passes, leaving %s", buf.position(), passCount,
						buf.toString()));
			}

			if ((bytesWritten < 0) || buf.hasRemaining()) throw new FailedWriteException(
				outputLogFile, WriteFailureType.WRITE_RETURNS_NEGATIVE, writeOffset, buf.position(),
				buf.remaining());

			if (LOG.isDebugEnabled()) {
				LOG.debug("Drained buffer to output file and recycled WriteFileBuffer message.", buf);
			}

			return batchDef.loadCounterDeltas(counterIncrementsAllocator.allocate());
		}
		catch (final IOException e) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(
					String.format(
						"Failed to drain buffer to output file %s at offset %d.  Event still recyled.",
						outputLogFile, writeOffset),
					e);
			}
			throw new FailedWriteException(
				outputLogFile, WriteFailureType.IO_EXCEPTION_ON_WRITE, writeOffset, buf.position(),
				buf.remaining(), e);
		}
		finally {
			batchDef.release();
		}
	}


	void close()
	{
		try {
			outputFileStream.close();
		}
		catch (final IOException e) {
			LOG.error(
				String.format(
					"Exception thrown on closing %s while cleaning up resources to shutdown.",
					outputLogFile),
				e);
			throw new FailedWriteException(outputLogFile, WriteFailureType.IO_EXCEPTION_ON_CLOSE, e);
		}
	}


	@Override
	public Iterable<IStatsProvider> get()
	{
		final ArrayList<IStatsProvider> retVal = new ArrayList<>(1);
		retVal.add(
			new ResourceStatsAdapter(
				"Output File Writers' RingBufferWorkProcessor", writeOutputFileWorkProcessor));
		return retVal;
	}
}

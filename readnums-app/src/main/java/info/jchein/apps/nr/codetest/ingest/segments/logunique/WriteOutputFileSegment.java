package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.reactivestreams.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.ICounterIncrements;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.action.Control;
import reactor.rx.action.support.TapAndControls;


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
	private final long reportIntervalInSeconds;

	private final Timer ingestionTimer;
	private final BatchInputSegment batchInputSegment;
	private final Processor<IWriteFileBuffer, IWriteFileBuffer> writeOutputFileProcessor;
	private final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator;
	private final CounterOverall cumulativeValues = new CounterOverall();

	private final ReentrantLock shutdownLock = new ReentrantLock();
   private final boolean[] seenOnComplete = {false, false};
	private final Condition completed = shutdownLock.newCondition();

	private FileOutputStream outputFileStream;
	private Stream<ICounterIncrements> reportCounterIncrementsStream;

	// private Control terminalContol;


	public WriteOutputFileSegment( final String outputFilePath, final short concurrentFileWriters,
		final long reportIntervalInSeconds, final EventBus eventBus, final Timer reportingTimer,
		final BatchInputSegment batchInputSegment,
		final Processor<IWriteFileBuffer, IWriteFileBuffer> writeOutputFileProcessor,
		final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator )
	{
		super(eventBus);
		this.reportIntervalInSeconds = reportIntervalInSeconds;
		this.outputLogFile = new File(outputFilePath);
		this.batchInputSegment = batchInputSegment;
		this.ingestionTimer = reportingTimer;
		this.writeOutputFileProcessor = writeOutputFileProcessor;
		this.counterIncrementsAllocator = counterIncrementsAllocator;

		Preconditions.checkArgument(
			concurrentFileWriters == 1, "Only one concurrent writer is supported at this time.");
	}


	@Override
	public int getPhase()
	{
		return 500;
	}


	@Override
	protected Function<Event<Long>, Boolean> doStart()
	{
		LOG.info("Output writer opening output file");
		openFile();
		LOG.info("Output file available for writing");

		final Control terminalControl = initFileWriterPartial();
		LOG.info("Data collection is online");

		return evt -> {
			long nanosTimeout = evt.getData()
				.longValue();

			shutdownLock.lock();
			try {
				if (seenOnComplete[0] == false) {
					terminalControl.cancel();
				}
				while (seenOnComplete[0] == false) {
					try {
						nanosTimeout = completed.awaitNanos(nanosTimeout);
					}
					catch (final InterruptedException e) {
						LOG.error("Clean shutdown aborted by thread interruption!");
						Thread.interrupted();
						return Boolean.FALSE;
					}
				}

				LOG.info("Write output file with counters segment acknowledges a clean shutdown");
				return Boolean.TRUE;
			}
			finally {
				shutdownLock.unlock();
			}
		};
	}


	private Control initFileWriterPartial()
	{
		this.reportCounterIncrementsStream =
 this.batchInputSegment.getLoadedWriteFileBufferStream()
				.process(this.writeOutputFileProcessor)
			.map(writeBuffer -> processBatch(writeBuffer))
			.observeError(Throwable.class, (v, e) -> {
				final StringBuffer msg = new StringBuffer(1024);

				int partitionNum = 0;
				final Iterator<TapAndControls<IWriteFileBuffer>> tapIter =
					this.batchInputSegment.getPartitionedTaps();
				while (tapIter.hasNext()) {
					msg.append("-- Last on partition index = ")
						.append(partitionNum++)
						.append(" was ")
						.append(tapIter.next()
							.get())
						.append('\n');
				}
				LOG.error(
					String.format(
						"Tap scan on error triggered by %s: %s-- Exception: {}", v, msg.toString()),
					e);
			});

		LOG.info("Console reporting interval is every {} seconds", Long.valueOf(reportIntervalInSeconds));

		return this.reportCounterIncrementsStream.observeCancel(evt -> {
				// Toggle the first seenOnComplete flag once input to the final window is recognized by
				// observing a SHUTDOWN event being fed to the window boundary. Note that we are taking
				// advantage observeCancel()'s bug that causes it to trigger on SHUTDOWN signals rather
				// than CANCEL signals since there is no native observeShutdown() observer!
				shutdownLock.lock();
				try {
					seenOnComplete[0] = true;
					LOG.info(
						"Performance stats segment receives an end of stream signal.  No additional data will follow.");
				}
				finally {
					shutdownLock.unlock();
				}
			})
			.window(this.reportIntervalInSeconds, TimeUnit.SECONDS, this.ingestionTimer)
			.combine()
			.consume(statStream -> {
				// statStream.startWith(
				// Streams.just(
				// counterIncrementsAllocator.allocate()
				// .setDeltas(0, 0))
				// )
				statStream.reduce(
					counterIncrementsAllocator.allocate().setDeltas(0, 0),
					(prevStat, nextStat) -> {
						nextStat.beforeRead();
						prevStat.incrementDeltas(nextStat);
						nextStat.release();

						return prevStat;
					}
				).consume(deltaSum -> {
					// First argument to format aggregates the total unique counter and returns the duration
					// since the last update. Remaining arguments are simple getters. If this is later
					// rearranged, understand that initial call to incrementUniqueValues() establishes time
					// duration for subsequent call to getTotalDuration() as well as total unique counter for
					// subsequent call to getTotalUniques(). Call to incrementUniqueValues() must therefore
					// precede either call to other two methods called out in this comment.
					LOG.info(
						String.format(
							"During the last %d seconds, %d unique 9-digit inputs were logged and %d redundant inputs were discarded.\nSince service launch (%d seconds), %d unique 9-digit inputs have been logged.\n\n",
							Long.valueOf(
								TimeUnit.NANOSECONDS.toSeconds(
									cumulativeValues.incrementUniqueValues(deltaSum.getDeltaUniques()))),
							Integer.valueOf(deltaSum.getDeltaUniques()),
							Integer.valueOf(deltaSum.getDeltaDuplicates()),
							Long.valueOf(TimeUnit.NANOSECONDS.toSeconds(cumulativeValues.getTotalDuration())),
							Integer.valueOf(cumulativeValues.getTotalUniques())));
					deltaSum.release();
				});
			});
	}


	public Stream<ICounterIncrements> getReportCounterIncrementsStream()
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
				"Output File Writers' RingBufferProcessor", this.writeOutputFileProcessor));

		return retVal;
	}
}

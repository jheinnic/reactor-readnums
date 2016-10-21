package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.core.Dispatcher;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;


public class BatchInputSegment
extends AbstractSegment
implements IStatsProviderSupplier
{
	private static final Logger LOG = LoggerFactory.getLogger(BatchInputSegment.class);

	private final short ioCount;
	private final long ioPeriod;
	private final TimeUnit ioTimeUnit;
	private final byte numDataPartitions;
	private final IUniqueMessageTrie uniqueTest;

	private final Timer ingestionTimer;
	private final Dispatcher handoffDispatcher;
	private final Dispatcher rawInputDispatcher;

	private final Broadcaster<Stream<IInputMessage>> streamsToMerge;
	private final ArrayList<Broadcaster<IInputMessage>> fanOutBroadcasters;
	private final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator;

	private Stream<IWriteFileBuffer> loadedWriteFileBufferStream;
	private final ArrayList<Stream<IWriteFileBuffer>> streamTerminals;
	private final int[] skipCounters;


	public BatchInputSegment( final short ioCount, final long ioPeriod, final TimeUnit ioTimeUnit,
		final byte numDataPartitions, final EventBus eventBus, final Timer ingestionTimer,
		final IUniqueMessageTrie uniqueTest,
		final Broadcaster<Stream<IInputMessage>> streamsToMerge,
		final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator,
		final Dispatcher rawInputDispatcher,
		final Dispatcher handoffDispatcher,
		final ArrayList<Broadcaster<IInputMessage>> fanOutBroadcasters )
	{
		super(eventBus);
		this.ioCount = ioCount;
		this.ioPeriod = ioPeriod;
		this.ioTimeUnit = ioTimeUnit;
		this.uniqueTest = uniqueTest;
		this.ingestionTimer = ingestionTimer;
		this.streamsToMerge = streamsToMerge;
		this.numDataPartitions = numDataPartitions;
		this.fanOutBroadcasters = fanOutBroadcasters;
		this.rawInputDispatcher = rawInputDispatcher;
		this.handoffDispatcher = handoffDispatcher;
		this.writeFileBufferAllocator = writeFileBufferAllocator;

		this.streamTerminals = new ArrayList<>(this.numDataPartitions);
		this.skipCounters = new int[this.numDataPartitions];

	}


	@Override
	public int getPhase()
	{
		return 300;
	}


	@Override
	protected Function<Event<Long>, Boolean> doStart()
	{
		// this.streamsToMerge begins as:
		// -- A Stream of:
		// -- Streams per TCP Channel of:
		// -- Streams, each keyed by a Data Partition index and made of:
		// -- InputMessages from the same Data Partition index
		// First flatten out each TCP Channel's stream to yield:
		// -- A Stream of
		// -- Streams, each keyed by a Data Partition index and made of:
		// -- InputMessages from the same Data Partition index
		// Dispatch each of the nested streams to a worker dedicated to the stream's
		// associated data partition index. Do all remaining work on that thread.
		// -- Flatten down to a data partitioned Stream of InputMessages
		// -- Apply filtering test for uniqueness
		// -- Load a merged write buffer and counters
		// -- Dispatch to the I/O thread

		for (int ii = 0; ii < this.numDataPartitions; ii++) {
			final int partitionIndex = ii;
			final Broadcaster<IInputMessage> nextBcast = this.fanOutBroadcasters.get(ii);
			
			streamTerminals.add(nextBcast.log("on broadcast")
				.filter(inputMsg -> {
				inputMsg.beforeRead();
				if (this.uniqueTest.isUnique(inputMsg.getPrefix(), inputMsg.getSuffix())) {
					return true;
				} else {
					skipCounters[partitionIndex] += 1;
					return false;
				}
			})
				.buffer(this.ioCount, this.ioPeriod, this.ioTimeUnit, this.ingestionTimer)
				.log("Buffer2")
				.map(messageList -> {
					IWriteFileBuffer retVal = this.writeFileBufferAllocator.allocate();
					for (IInputMessage nextMsg : messageList) {
						retVal.acceptUniqueInput(nextMsg.getMessageBytes());
					}
					retVal.trackSkippedDuplicates(skipCounters[partitionIndex]);
					skipCounters[partitionIndex] = 0;

					retVal.afterWrite();
					return retVal;
				})
				.log("for writer"));
		}

		this.streamsToMerge.<IInputMessage> merge()
			.capacity(this.ioCount)
			.window(this.ioCount, this.ioPeriod, this.ioTimeUnit, this.ingestionTimer)
			.log("Window")
			.consumeOn(this.rawInputDispatcher, batchedStream -> {
				batchedStream.groupBy(
					inputMsg -> Integer.valueOf(inputMsg.getPartitionIndex())
				).consumeOn(this.rawInputDispatcher, groupedStream -> {
					final Broadcaster<IInputMessage> groupBroadcaster = 
						this.fanOutBroadcasters.get(groupedStream.key());
					groupedStream.consume(inputMsg -> {
						groupBroadcaster.onNext(inputMsg);
					});
				});
			});

		this.loadedWriteFileBufferStream = Streams.merge(streamTerminals)
			.log("Transformed and merged");

		return evt -> Boolean.TRUE;
	}


	public Stream<IWriteFileBuffer> getLoadedWriteFileBufferStream()
	{
		return loadedWriteFileBufferStream;
	}


	@Override
	public Iterable<IStatsProvider> get()
	{
		final ArrayList<IStatsProvider> retVal =
			new ArrayList<>(this.numDataPartitions);
		for (int ii = 0; ii < this.numDataPartitions; ii++) {
			retVal.add(
				new ResourceStatsAdapter(
					"Partitioned Input RingBufferProcessor-" + ii,
					this.fanOutBroadcasters.get(ii).getDispatcher()));
		}
		retVal.add(
			new ResourceStatsAdapter(
				"Handoff RingBufferProcessor", this.handoffDispatcher));
		retVal.add(
			new ResourceStatsAdapter(
				"Raw Input RingBufferProcessor", this.rawInputDispatcher));

		return retVal;
	}
}

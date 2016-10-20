package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

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
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.stream.GroupedStream;


public class BatchInputSegment
extends AbstractSegment
implements IStatsProviderSupplier
{
	// private static final Logger LOG = LoggerFactory.getLogger(BatchInputSegment.class);

	private final short ioCount;
	private final long ioPeriod;
	private final TimeUnit ioTimeUnit;
	private final byte numDataPartitions;
	private final Timer ingestionTimer;
	private final IUniqueMessageTrie uniqueTest;
	private final Broadcaster<Stream<GroupedStream<Integer, IInputMessage>>> streamsToMerge;
	private final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator;
	// private final Processor<GroupedStream<Integer, IInputMessage>, GroupedStream<Integer, IInputMessage>>[]
	// fanOutProcessors;
	private final Dispatcher socketHandoffDispatcher;
	private final Dispatcher[] fanOutDispatchers;

	private Stream<IWriteFileBuffer> loadedWriteFileBufferStream;


	public BatchInputSegment( final short ioCount, final long ioPeriod, final TimeUnit ioTimeUnit,
		final byte numDataPartitions, final EventBus eventBus, final Timer ingestionTimer,
		final IUniqueMessageTrie uniqueTest,
		final Broadcaster<Stream<GroupedStream<Integer, IInputMessage>>> streamsToMerge,
		final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator,
		final Dispatcher socketHandoffDispatcher,
		final Dispatcher[] fanOutDispatchers )
		// final Processor<GroupedStream<Integer, IInputMessage>, GroupedStream<Integer, IInputMessage>>[] fanOutDispatchers )
	{
		super(eventBus);
		this.ioCount = ioCount;
		this.ioPeriod = ioPeriod;
		this.ioTimeUnit = ioTimeUnit;
		this.uniqueTest = uniqueTest;
		this.ingestionTimer = ingestionTimer;
		this.numDataPartitions = numDataPartitions;
		this.streamsToMerge = streamsToMerge;
		this.socketHandoffDispatcher = socketHandoffDispatcher;
		this.writeFileBufferAllocator = writeFileBufferAllocator;
		this.fanOutDispatchers = fanOutDispatchers;
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
		this.loadedWriteFileBufferStream = this.streamsToMerge.subscribeOn(this.socketHandoffDispatcher)
			.<GroupedStream<Integer, IInputMessage>> merge()
			.<Integer> groupBy(channelSubStream -> channelSubStream.key())
			.<IWriteFileBuffer> flatMap(
				partitionStream -> partitionStream
					.subscribeOn(this.fanOutDispatchers[partitionStream.key()])
					.<IInputMessage> merge()
					.window(this.ioCount, this.ioPeriod, this.ioTimeUnit, this.ingestionTimer)
					.<IWriteFileBuffer> flatMap(nestedWindow -> {
						return nestedWindow.reduce(
							this.writeFileBufferAllocator.allocate(),
							(final IWriteFileBuffer writeBuf, final IInputMessage msg) -> {
							msg.beforeRead();
							if (this.uniqueTest.isUnique(msg.getPrefix(), msg.getSuffix()))
								writeBuf.acceptUniqueInput(msg.getMessageBytes());
							else writeBuf.trackSkippedDuplicates(1);
							msg.release();

							return writeBuf;
						})
							.<IWriteFileBuffer> map(writeBuf -> writeBuf.afterWrite());
					}));

		return evt -> Boolean.TRUE;
	}


	public Stream<IWriteFileBuffer> getLoadedWriteFileBufferStream()
	{
		return loadedWriteFileBufferStream;
	}


	@Override
	public Iterable<IStatsProvider> get()
	{
		final ArrayList<IStatsProvider> retVal = new ArrayList<>(numDataPartitions);
		for (int ii = 0; ii < numDataPartitions; ii++) {
			retVal.add(
				new ResourceStatsAdapter(
					"Partitioned Input RingBufferProcessor-" + ii, fanOutDispatchers[ii]));
		}
		return retVal;
	}
}

package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.messages.MessageInput;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.InputMessageCodec;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.Control;
import reactor.rx.action.support.TapAndControls;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.stream.GroupedStream;


public class BatchInputSegment
extends AbstractSegment
implements IStatsProviderSupplier
{
	static final Logger LOG = LoggerFactory.getLogger(BatchInputSegment.class);

	private final short ioCount;
	private final long ioPeriod;
	private final TimeUnit ioTimeUnit;
	private final byte numDataPartitions;
	private final IUniqueMessageTrie uniqueTest;
	private final Timer ingestionTimer;
	private final Broadcaster<MessageInput> streamsToMerge;
	private final ArrayList<Broadcaster<MessageInput>> fanOutBroadcasters;
	private final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator;

	private final int[] skipCounters;
	private final ArrayList<Stream<IWriteFileBuffer>> streamTerminals;

	private Stream<IWriteFileBuffer> loadedWriteFileBufferStream;

	public BatchInputSegment( final short ioCount, final long ioPeriod, final TimeUnit ioTimeUnit,
		final byte numDataPartitions, final EventBus eventBus, final Timer ingestionTimer,
		final IUniqueMessageTrie uniqueTest,
 final Broadcaster<MessageInput> streamsToMerge,
		final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator,
		final ArrayList<Broadcaster<MessageInput>> fanOutBroadcasters )
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
		this.writeFileBufferAllocator = writeFileBufferAllocator;
		this.skipCounters = new int[this.numDataPartitions];
		this.streamTerminals = new ArrayList<>(this.numDataPartitions);
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
		// -- MessageInputs from the same Data Partition index
		// First flatten out each TCP Channel's stream to yield:
		// -- A Stream of
		// -- Streams, each keyed by a Data Partition index and made of:
		// -- MessageInputs from the same Data Partition index
		// Dispatch each of the nested streams to a worker dedicated to the stream's
		// associated data partition index. Do all remaining work on that thread.
		// -- Flatten down to a data partitioned Stream of MessageInputs
		// -- Apply filtering test for uniqueness
		// -- Load a merged write buffer and counters
		// -- Dispatch to the I/O thread

		for (int ii = 0; ii < this.numDataPartitions; ii++) {
			final int partitionIndex = ii;
			final Broadcaster<MessageInput> nextBcast = this.fanOutBroadcasters.get(ii);
			
			this.streamTerminals.add(
				nextBcast.filter(inputMsg -> {
					final boolean retVal =
						this.uniqueTest.isUnique(
							InputMessageCodec.parsePrefix(
								inputMsg.getMessageBytes()
							), inputMsg.getSuffix());

					if (!retVal) 
						skipCounters[partitionIndex] += 1;

					return retVal;
				}).buffer(
					this.ioCount, this.ioPeriod, this.ioTimeUnit, this.ingestionTimer
				).map(messageList -> {
					IWriteFileBuffer retVal = this.writeFileBufferAllocator.allocate();
					for (MessageInput nextMsg : messageList) {
						retVal.acceptUniqueInput(nextMsg.getMessageBytes());
					}
					retVal.trackSkippedDuplicates(skipCounters[partitionIndex]);
					skipCounters[partitionIndex] = 0;

					return retVal.afterWrite();
				}).combine()
			);
		}

		Stream<GroupedStream<Integer, MessageInput>> groupBy =
			this.streamsToMerge.groupBy(inputMsg -> Integer.valueOf(inputMsg.getPartitionIndex()));
		Stream<GroupedStream<Integer, MessageInput>> groupByCombined = groupBy.combine();

		final Control terminalControl = groupByCombined.consume(groupedStream -> {
			final Broadcaster<MessageInput> groupBroadcaster = 
				this.fanOutBroadcasters.get(groupedStream.key());
			
			LOG.info(
				"Pre-batch, post-group nested stream dispatched by {} of {} with a capacity of {}",
				groupedStream.getDispatcher(),
				groupedStream.getDispatcher()
					.backlogSize(),
				groupedStream.getCapacity());

			LOG.info(
				"Parallel broadcast stream dispatched by {} of {} with a capacity of {}",
				groupBroadcaster.getDispatcher(),
				groupBroadcaster.getDispatcher()
					.backlogSize(),
				groupBroadcaster.getCapacity());

			groupedStream.consume(groupBroadcaster::onNext);
			// inputMsg -> {
			// groupBroadcaster.onNext(inputMsg);
			// });
		});

		LOG.info(
			"Post-merge outer stream dispatched by {} of {} with a capacity of {}",
			this.streamsToMerge.getDispatcher(),
			this.streamsToMerge.getDispatcher()
				.backlogSize(),
			this.streamsToMerge.getCapacity());
		LOG.info(
			"Post-groupBy outer stream dispatched by {} of {} with a capacity of {}",
			groupBy.getDispatcher(),
			groupBy.getDispatcher()
				.backlogSize(),
			groupBy.getCapacity());
		LOG.info(
			"Combined outer stream dispatched by {} of {} with a capacity of {}",
			groupByCombined.getDispatcher(),
			groupByCombined.getDispatcher()
				.backlogSize(),
			groupByCombined.getCapacity());

		this.loadedWriteFileBufferStream = Streams.merge(this.streamTerminals);

		return evt -> {
			terminalControl.cancel();
			return Boolean.TRUE;
		};
	}


	public Stream<IWriteFileBuffer> getLoadedWriteFileBufferStream()
	{
		return this.loadedWriteFileBufferStream;
	}


	public Iterator<TapAndControls<IWriteFileBuffer>> getPartitionedTaps()
	{
		return this.streamTerminals.stream()
			.map(stream -> stream.tap())
			.iterator();
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

		return retVal;
	}
}

package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;


import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.messages.MessageInput;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import io.netty.channel.Channel;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.fn.Consumer;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

public class ConnectionHandler
implements ReactorChannelHandler<MessageInput, Object, ChannelStream<MessageInput, Object>>,
IStatsProviderSupplier
{
	private static final Logger LOG = LoggerFactory.getLogger(ConnectionHandler.class);

	// The AtomicdReference.compareAndSet() method can spuriously fail, even though its preconditions are met. In order
	// to avoid time and GC fuse length re-allocating the same object in such cases, attempt the compareAndSet call with
	// any given set of inputs at least N times before falling back to retrying the entire associated operation.
	private static final int MAX_COMPARE_SET_ATTEMPTS = 3;

	// A marker value use to indicate that at the end of an Atomic update to the SocketRegistry, no connection slots were
	// available during onConnect().
	private static final int ALLOCATION_FAILURE = -1;

	private final int maxConcurrentSockets;
	private final Consumer<Void> terminationConsumer;
	private final Broadcaster<MessageInput> streamsToMerge;
	private final AtomicReference<SocketRegistry> socketRegistryHolder;

	public ConnectionHandler(
		final int maxConcurrentSockets, final Consumer<Void> terminationConsumer,
		final Broadcaster<MessageInput> streamsToMerge )
	{
		this.maxConcurrentSockets = maxConcurrentSockets;
		this.terminationConsumer  = terminationConsumer;
		this.streamsToMerge		 = streamsToMerge;
		this.socketRegistryHolder = 
			new AtomicReference<>(
				new SocketRegistry(maxConcurrentSockets));
	}


	@Override
	public Publisher<Void> apply(final ChannelStream<MessageInput, Object> channelStream)
	{
		LOG.info("In connection handler");

		// This server has no need to write data back out to the client, so wire the to-Client duplex side of
		// channelStream to process a Stream that contains zero message signals followed by an onComplete signal. Make
		// sure the data flows through the channelStream's write subscriber and isn't just passively available as the next
		// signal ready for delivery. writeWith is a passive observer that does NOT generate demand of its own, but it
		// will process any row that moves through it to satisfy a downstream Processor or a hot downstream Stream
		// segment.
		final Broadcaster<Object> outputToStream = Broadcaster.create(SynchronousDispatcher.INSTANCE);
		outputToStream.log("output to channel")
			.consume();
		final Stream<Void> retVal = channelStream.writeWith(outputToStream);

		if (onConnected(channelStream, outputToStream) != null) {
			channelStream.on()
				.close(v -> onDisconnected(channelStream));
		};
		
		LOG.info("Initial original capacity: {}", channelStream.getCapacity());
		LOG.info("Initial original dispatcher: {} of {}",
			channelStream.getDispatcher(), // .toString(),
			channelStream.getDispatcher()
				.backlogSize());

		Stream<MessageInput> filteredStream =
			channelStream.dispatchOn(
				this.streamsToMerge.getDispatcher()
			).filter(evt -> {
				switch (evt.getKind()) {
					case NINE_DIGITS: {
						return true;
					}
					case TERMINATE_CMD: {
						this.terminationConsumer.accept(null);
						closeChannel(channelStream, outputToStream);
						break;
					}
					case INVALID_INPUT: {
						closeChannel(channelStream, outputToStream);
					}
				}
	
				return false;
			});
		filteredStream.consume(this.streamsToMerge::onNext);

		LOG.info("Filtered capacity: {}", filteredStream.getCapacity());
		LOG.info("Filtered dispatcher: {} of {}",
			filteredStream.getDispatcher(), // .toString(),
			filteredStream.getDispatcher()
				.backlogSize());
		LOG.info("Second original capacity: {}", channelStream.getCapacity());
		LOG.info("Second original dispatcher: {} of {}",
			channelStream.getDispatcher(), // .toString(),
			channelStream.getDispatcher()
				.backlogSize());
		return retVal;
	}


	private ChannelStreamController<MessageInput> onConnected(
		final ChannelStream<MessageInput, Object> channelStream,
		final Broadcaster<Object> outputToStream)
	{
		final ChannelStreamController<MessageInput> controller =
			new ChannelStreamController<>(channelStream);

		// Attempt to reserve a connection slot and atomically update the registry. If unsuccessful, use the function
		// generated above to abort the connection. Otherwise, return the ConnectionContext object created for the newly
		// filled slot entry.
		int socketIdx = this.maxConcurrentSockets;
		do {
			final SocketRegistry prevSocketRegistry = this.socketRegistryHolder.get();
			final SocketRegistry nextSocketRegistry =
				prevSocketRegistry.allocateNextConnectionFor(channelStream, controller);

			if (nextSocketRegistry == null) {
				socketIdx = ALLOCATION_FAILURE;
				closeChannel(channelStream, outputToStream);
			} else {
				for (int ii = 0; (ii < MAX_COMPARE_SET_ATTEMPTS) &&
					(socketIdx == this.maxConcurrentSockets); ii++)
				{
					if (this.socketRegistryHolder.compareAndSet(prevSocketRegistry, nextSocketRegistry)) {
						socketIdx = nextSocketRegistry.socketsAvailable;
					}
				}
			}
		}
		while (socketIdx == this.maxConcurrentSockets);

		if ((socketIdx != ALLOCATION_FAILURE) && LOG.isInfoEnabled()) {
			final Channel channel = (Channel) channelStream.delegate();
			LOG.info(
				String.format(
					"Accepted connection from %s to %s, leaving %d connection slots still available.",
					channel.remoteAddress(),  // .toString(),
					channel.localAddress(),  // .toString(),
					socketIdx));
		} else if ((socketIdx == ALLOCATION_FAILURE) && LOG.isWarnEnabled()) {
			final Channel channel = (Channel) channelStream.delegate();
			LOG.warn(
				"Rejected connection from {} to {}, as that would have left fewer than 0 slots available.",
				new Object[] { channel.remoteAddress(), channel.localAddress() });
		}

		return (socketIdx != ALLOCATION_FAILURE) ? controller : null;
	}


	private void onDisconnected(final ChannelStream<MessageInput, Object> channelStream)
	{
		boolean stillConnected = true;
		SocketRegistry nextSocketRegistry = null;
		for (int ii = 0; (ii < MAX_COMPARE_SET_ATTEMPTS) && stillConnected; ii++) {
			final SocketRegistry prevSocketRegistry = this.socketRegistryHolder.get();
			nextSocketRegistry = prevSocketRegistry.releaseConnectionFor(channelStream);

			if (this.socketRegistryHolder.compareAndSet(prevSocketRegistry, nextSocketRegistry)) {
				stillConnected = false;
				if (LOG.isInfoEnabled()) {
					final Channel channel = (Channel) channelStream.delegate();
					LOG.info(String.format(
						"Returned a connection slot to the avaialable pool after closing connection from %s to %s, leaving %d slots available.",
						channel.remoteAddress().toString(),
						channel.localAddress().toString(),
						Integer.valueOf(nextSocketRegistry.socketsAvailable)));
				}
			}
		}

		if (stillConnected) {
			final Channel channel = (Channel) channelStream.delegate();
			LOG.error(
				String.format(
					"Failed to return a connection slot to the avaialable pool in %d retries after closing connection from %s to %s.",
					channel.remoteAddress().toString(),
					channel.localAddress().toString(),
					MAX_COMPARE_SET_ATTEMPTS));
		}
	}


	private void closeChannel(
		final ChannelStream<MessageInput, Object> channelStream,
		Broadcaster<Object> outputToStream)
	{
		final Channel channel = (Channel) channelStream.delegate();
		LOG.info(String.format(
			"In closeChannel() for connection from %s to %s",
			channel.remoteAddress().toString(),
			channel.localAddress().toString()));
			
		/*
		 * final ChannelStreamController<MessageInput> resourceAdapter = this.socketRegistryHolder.get()
		 * .lookupResourceAdapter(channelStream); if (resourceAdapter.call() == false) throw new
		 * CloseConnectionFailedException(channelStream);
		 */
		outputToStream.onComplete();
	};


	public boolean close()
	{
		// Block new connections without preventing existing connections from being closed by updating the SocketRegistry
		// to reflect -1 * maxConnnections available (which will never grow back to a positive number of available
		// sockets, and will still allow existing sockets to close cleanly). Don't disrupt existing data flow--allow
		// anything in the pipeline to flow through normally, but actively close connections once newly established ones
		// are blocked to cut off the source of new data.
		final SocketRegistry socketRegistry =
			this.socketRegistryHolder
				.getAndUpdate(registry -> registry.cancelSocketAvailability(this.maxConcurrentSockets));
		boolean retVal = true;
		for (final ChannelStreamController<MessageInput> nextStream : socketRegistry.openSocketContexts.values()) {
			retVal = nextStream.call() && retVal;
		}
		return retVal;
	}


	@Override
	public Iterable<IStatsProvider> get()
	{
		final ArrayList<IStatsProvider> retVal = new ArrayList<>(1);
		retVal.add(
			new ResourceStatsAdapter(
				"Connection Handler Socket Handoff RingBufferProcessor",
				this.streamsToMerge.getDispatcher()));

		return retVal;
	}
}
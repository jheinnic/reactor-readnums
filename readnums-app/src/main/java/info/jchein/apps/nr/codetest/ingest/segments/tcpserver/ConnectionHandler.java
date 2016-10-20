package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;


import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import io.netty.channel.Channel;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.stream.GroupedStream;

public class ConnectionHandler
implements ReactorChannelHandler<IInputMessage, IInputMessage, ChannelStream<IInputMessage, IInputMessage>>
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
	private final Broadcaster<Stream<GroupedStream<Integer, IInputMessage>>> streamsToMerge;
	private final Dispatcher socketHandoffDispatcher;
   private final AtomicReference<SocketRegistry> socketRegistryHolder;



	public ConnectionHandler(
		final int maxConcurrentSockets, final Consumer<Void> terminationConsumer,
		final Dispatcher socketHandoffDispatcher,
		final Broadcaster<Stream<GroupedStream<Integer, IInputMessage>>> streamsToMerge )
	{
      this.maxConcurrentSockets = maxConcurrentSockets;
		this.terminationConsumer  = terminationConsumer;
		this.socketHandoffDispatcher = socketHandoffDispatcher;
      this.streamsToMerge       = streamsToMerge;
		this.socketRegistryHolder = 
			new AtomicReference<>(
				new SocketRegistry(maxConcurrentSockets));
   }


   @Override
   public Publisher<Void> apply(final ChannelStream<IInputMessage, IInputMessage> channelStream)
   {
      LOG.info("In connection handler");
		if (onConnected(channelStream) != null) {
         channelStream.on().close(v -> onDisconnected(channelStream));
      };

		this.streamsToMerge.onNext(
			channelStream.dispatchOn(this.socketHandoffDispatcher)
			.filter(evt -> {
				switch (evt.getKind()) {
					case NINE_DIGITS: {
						return true;
					}
					case TERMINATE_CMD: {
						terminationConsumer.accept(null);
						closeChannel(channelStream);
						break;
					}
					case INVALID_INPUT: {
						closeChannel(channelStream);
					}
				}

				return false;
			})
			.groupBy(evt -> {
				return Byte.toUnsignedInt(evt.getPartitionIndex());
			})
      );

      // This server has no need to write data back out to the client, so wire the to-Client duplex side of
      // channelStream to process a Stream that contains zero message signals followed by an onComplete signal. Make
      // sure the data flows through the channelStream's write subscriber and isn't just passively available as the next
		// signal ready for delivery. writeWith is a passive observer that does NOT generate demand of its own, but it
      // will process any row that moves through it to satisfy a downstream Processor or a hot downstream Stream segment.
		LOG.info("Holding write channel open on a Stream with no data that never completes.");
		final Stream<IInputMessage> noData = Streams.never();
		noData.consume();
		// LOG.info("Consuming never...");
      return channelStream.writeWith(noData);
   }


	private ChannelStreamController<IInputMessage>
	onConnected(final ChannelStream<IInputMessage, IInputMessage> channelStream)
   {
		final ChannelStreamController<IInputMessage> controller =
			new ChannelStreamController<>(channelStream);

      // Attempt to reserve a connection slot and atomically update the registry. If unsuccessful, use the function
      // generated above to abort the connection. Otherwise, return the ConnectionContext object created for the newly
      // filled slot entry.
      int socketIdx = maxConcurrentSockets;
      do {
         final SocketRegistry prevSocketRegistry = socketRegistryHolder.get();

         final SocketRegistry nextSocketRegistry =
            prevSocketRegistry.allocateNextConnectionFor(channelStream, controller);

         if (nextSocketRegistry == null) {
            socketIdx = ALLOCATION_FAILURE;
            closeChannel(channelStream);
         } else {
            for (int ii = 0; (ii < MAX_COMPARE_SET_ATTEMPTS) && (socketIdx == maxConcurrentSockets); ii++) {
               if (socketRegistryHolder.compareAndSet(prevSocketRegistry, nextSocketRegistry)) {
                  socketIdx = nextSocketRegistry.socketsAvailable;
               }
            }
         }
      } while (socketIdx == maxConcurrentSockets);

      if ((socketIdx != ALLOCATION_FAILURE) && LOG.isInfoEnabled()) {
         final Channel channel = (Channel) channelStream.delegate();
         LOG.info(String.format(
            "Accepted connection from %s to %s, leaving %d connection slots still available.",
            channel.remoteAddress()
            .toString(),
            channel.localAddress()
            .toString(),
            socketIdx));
      } else if ((socketIdx == ALLOCATION_FAILURE) && LOG.isWarnEnabled()) {
         final Channel channel = (Channel) channelStream.delegate();
         LOG.warn(
            "Rejected connection from {} to {}, as that would have left -1 slots available.",
            new Object[] { channel.remoteAddress(), channel.localAddress() });
      }

		return (socketIdx != ALLOCATION_FAILURE) ? controller : null;
   }


   private void onDisconnected(final ChannelStream<IInputMessage, IInputMessage> channelStream)
   {
      int socketsLeft = -1;

      do {
         final SocketRegistry prevSocketRegistry = socketRegistryHolder.get();
         final SocketRegistry nextSocketRegistry =
            prevSocketRegistry.releaseConnectionFor(channelStream);

         for (int ii = 0; (ii < MAX_COMPARE_SET_ATTEMPTS) && (socketsLeft < 0); ii++) {
            if (socketRegistryHolder.compareAndSet(prevSocketRegistry, nextSocketRegistry)) {
               socketsLeft = nextSocketRegistry.socketsAvailable;
            }
         }
      } while (socketsLeft < 0);

      if (LOG.isInfoEnabled()) {
         final Channel channel = (Channel) channelStream.delegate();
         LOG.info(String.format(
            "Returned a connection slot to the avaialable pool after closing connection from %s to %s, leaving %d slots available.",
            channel.remoteAddress().toString(),
            channel.localAddress().toString(),
            Integer.valueOf(socketsLeft)));
      }
   }


   private void closeChannel(final ChannelStream<IInputMessage, IInputMessage> channelStream)
   {
      final Channel channel = (Channel) channelStream.delegate();
   	LOG.info(String.format(
   		"In closeChannel() for connection from %s to %s",
         channel.remoteAddress().toString(),
         channel.localAddress().toString()));
   		
   	final ChannelStreamController<IInputMessage> resourceAdapter =
         socketRegistryHolder.get()
         .lookupResourceAdapter(channelStream);
      if (resourceAdapter.call() == false) throw new CloseConnectionFailedException(channelStream);
   };


   public boolean close()
   {
      // Block new connections without preventing existing connections from being closed by updating the SocketRegistry
      // to reflect -1 * maxConnnections available (which will never grow back to a positive number of available
      // sockets, and will still allow existing sockets to close cleanly). Don't disrupt existing data flow--allow
      // anything in the pipeline to flow through normally, but actively close connections once newly established ones
      // are blocked to cut off the source of new data.
      final SocketRegistry socketRegistry =
         socketRegistryHolder.getAndUpdate(
            registry -> registry.cancelSocketAvailability(maxConcurrentSockets));
      boolean retVal = true;
      for (final ChannelStreamController<IInputMessage> nextStream : socketRegistry.openSocketContexts.values()) {
         retVal = nextStream.call() && retVal;
      }
      return retVal;
   }
}
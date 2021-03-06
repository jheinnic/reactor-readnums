package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import info.jchein.apps.nr.codetest.ingest.messages.MessageInput;
import reactor.io.net.ChannelStream;

class SocketRegistry
{
	final ImmutableMap<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> openSocketContexts;
   final int socketsAvailable;


   SocketRegistry( final int maxConcurrentSockets )
   {
      super();
      this.openSocketContexts =
			ImmutableMap
				.<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> builder()
         .build();
      this.socketsAvailable = maxConcurrentSockets;
   }


   private SocketRegistry(
		final ImmutableMap<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> openSocketContexts,
      final int socketsAvailable )
   {
      super();
      this.openSocketContexts = openSocketContexts;
      this.socketsAvailable = socketsAvailable;
   }


   SocketRegistry allocateNextConnectionFor(
final ChannelStream<MessageInput, Object> channelStream,
		ChannelStreamController<MessageInput> controller)
   {
      if (this.socketsAvailable <= 0) {
         return null;
      }

      final int nextSocketIdx = this.socketsAvailable - 1;

		final ImmutableMap.Builder<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> mapBuilder =
			ImmutableMap
				.<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> builder();

      // Construct a new connection map, starting with a clone of the outgoing map, then adding an entry for
      // the newly-reserved socket.
      mapBuilder.putAll(this.openSocketContexts);
      mapBuilder.put(channelStream, controller);

      return new SocketRegistry(mapBuilder.build(), nextSocketIdx);
   }


	SocketRegistry releaseConnectionFor(ChannelStream<MessageInput, Object> channelStream)
   {
      final int nextSocketIdx = this.socketsAvailable + 1;

      // Allocate builders for the next objects to build.
		final ImmutableMap.Builder<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> mapBuilder =
			ImmutableMap
				.<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> builder();

      // Construct a new connection map. Iterate over the outgoing map and include any entry with a non-matching
      // socket hook as key.
		for (Map.Entry<ChannelStream<MessageInput, Object>, ChannelStreamController<MessageInput>> nextEntry : this.openSocketContexts
			.entrySet())
		{
         if (nextEntry.getKey() != channelStream) {
            mapBuilder.put(nextEntry.getKey(), nextEntry.getValue());
         }
      }

      return new SocketRegistry(mapBuilder.build(), nextSocketIdx);
   }


	SocketRegistry cancelSocketAvailability(int availableReductionCount)
   {
		return new SocketRegistry(
			this.openSocketContexts, this.socketsAvailable - availableReductionCount);
   }


   ChannelStreamController<MessageInput> lookupResourceAdapter(
final ChannelStream<MessageInput, Object> channelStream)
   {
      final ChannelStreamController<MessageInput> retVal =
         this.openSocketContexts.get(channelStream);
      if (retVal == null) {
         throw new NoSuchConnectionException(channelStream);
      }

      return retVal;
   }
}
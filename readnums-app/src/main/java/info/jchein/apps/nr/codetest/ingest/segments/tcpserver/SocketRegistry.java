package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;

import java.util.Map;

import reactor.io.net.ChannelStream;

import com.google.common.collect.ImmutableMap;

class SocketRegistry
{
   final ImmutableMap<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> openSocketContexts;
   final int socketsAvailable;


   SocketRegistry( final int maxConcurrentSockets )
   {
      super();
      this.openSocketContexts =
         ImmutableMap.<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> builder()
         .build();
      this.socketsAvailable = maxConcurrentSockets;
   }


   private SocketRegistry(
      final ImmutableMap<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> openSocketContexts,
      final int socketsAvailable )
   {
      super();
      this.openSocketContexts = openSocketContexts;
      this.socketsAvailable = socketsAvailable;
   }


   SocketRegistry allocateNextConnectionFor(
      final ChannelStream<IInputMessage, IInputMessage> channelStream, ChannelStreamController<IInputMessage> controller)
   {
      if (this.socketsAvailable <= 0) {
         return null;
      }

      final int nextSocketIdx = this.socketsAvailable - 1;

      final ImmutableMap.Builder<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> mapBuilder =
         ImmutableMap.<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> builder();

      // Construct a new connection map, starting with a clone of the outgoing map, then adding an entry for
      // the newly-reserved socket.
      mapBuilder.putAll(this.openSocketContexts);
      mapBuilder.put(channelStream, controller);

      return new SocketRegistry(mapBuilder.build(), nextSocketIdx);
   }


   SocketRegistry releaseConnectionFor(ChannelStream<IInputMessage, IInputMessage> channelStream)
   {
      final int nextSocketIdx = this.socketsAvailable + 1;

      // Allocate builders for the next objects to build.
      final ImmutableMap.Builder<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> mapBuilder =
         ImmutableMap.<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> builder();

      // Construct a new connection map. Iterate over the outgoing map and include any entry with a non-matching
      // socket hook as key.
      for (Map.Entry<ChannelStream<IInputMessage, IInputMessage>, ChannelStreamController<IInputMessage>> nextEntry : this.openSocketContexts.entrySet()) {
         if (nextEntry.getKey() != channelStream) {
            mapBuilder.put(nextEntry.getKey(), nextEntry.getValue());
         }
      }

      return new SocketRegistry(mapBuilder.build(), nextSocketIdx);
   }


   SocketRegistry cancelSocketAvailability(int maxConcurrentConnections)
   {
      return new SocketRegistry(this.openSocketContexts, -1 * maxConcurrentConnections);
   }


   ChannelStreamController<IInputMessage> lookupResourceAdapter(
      final ChannelStream<IInputMessage, IInputMessage> channelStream)
   {
      final ChannelStreamController<IInputMessage> retVal =
         this.openSocketContexts.get(channelStream);
      if (retVal == null) {
         throw new NoSuchConnectionException(channelStream);
      }

      return retVal;
   }
}
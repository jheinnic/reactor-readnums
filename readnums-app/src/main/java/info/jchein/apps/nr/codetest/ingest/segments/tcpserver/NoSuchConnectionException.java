package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import reactor.io.net.ChannelStream;

public class NoSuchConnectionException
extends IllegalArgumentException
{
   private static final long serialVersionUID = -1232850002064253684L;

   private final ChannelStream<?, ?> channelStream;

   public NoSuchConnectionException( ChannelStream<?, ?> channelStream )
   {
      super("No known connection from " + channelStream.remoteAddress());
      this.channelStream = channelStream;
   }

   public ChannelStream<?, ?> getChannelStream()
   {
      return channelStream;
   }
}

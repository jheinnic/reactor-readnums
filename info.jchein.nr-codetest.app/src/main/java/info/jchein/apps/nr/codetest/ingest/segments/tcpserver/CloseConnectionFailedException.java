package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import reactor.io.net.ChannelStream;

/**
 * Exception class fed into the Subscriber and/or 
 * @author John
 *
 */
public class CloseConnectionFailedException
extends RuntimeException
{
   private static final long serialVersionUID = -3300457581731237260L;

   private final ChannelStream<?, ?> channelStream;

   public CloseConnectionFailedException( ChannelStream<?, ?> channelStream )
   {
      super("Failed to close connection from " + channelStream.remoteAddress());
      this.channelStream = channelStream;
   }

   public ChannelStream<?, ?> getChannelStream()
   {
      return channelStream;
   }

}

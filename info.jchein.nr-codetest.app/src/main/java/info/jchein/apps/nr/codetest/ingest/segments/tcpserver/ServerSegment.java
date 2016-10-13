package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import java.net.StandardProtocolFamily;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetStreams;
import reactor.io.net.impl.netty.NettyServerSocketOptions;
import reactor.io.net.impl.netty.tcp.NettyTcpServer;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Promise;

public class ServerSegment
extends AbstractSegment
{
   private static final Logger LOG = LoggerFactory.getLogger(ServerSegment.class);

   private final ConnectionHandler connectionHandler;
   private final TcpServer<IInputMessage,IInputMessage> tcpServer;

   public ServerSegment(
      final String bindHost,
      final int bindPort,
      final int maxConcurrentSockets,
      final int socketReceiveBufferSize,
      final int socketTimeoutMillis,
      final EventBus eventBus,
      final Environment environment,
      final ConnectionHandler connectionHandler,
      final Codec<Buffer, IInputMessage, IInputMessage> codec )
   {
      super(eventBus);

      this.connectionHandler = connectionHandler;
      tcpServer = NetStreams.<IInputMessage,IInputMessage> tcpServer(
         NettyTcpServer.class,
         aSpec -> {
            return aSpec.env(environment)
            .codec(codec)
            .dispatcher("serverDispatcher")
            .options(
               new NettyServerSocketOptions()
               .protocolFamily(StandardProtocolFamily.INET)
               .rcvbuf(socketReceiveBufferSize)
               .backlog(maxConcurrentSockets)
               .timeout(socketTimeoutMillis)
					.prefetch(10) // Long.MAX_VALUE)
               .keepAlive(false)
					.tcpNoDelay(false)
               .sndbuf(8192)
            )
            .listen(bindHost, bindPort);
         });
   }


   @Override
   public int getPhase()
   {
      return 400;
   }


   @Override
   protected Function<Event<Long>,Boolean> doStart() {
      LOG.info("Launching TCP server component...");
      tcpServer.start(connectionHandler);
      LOG.info("TCP server online and accepting connections!");

      return shutdownEvent -> {
         final long timeoutNanos = shutdownEvent.getData().longValue();
         LOG.info("TCP server component shutting down...");

         final boolean retValOne = connectionHandler.close();
         if (retValOne) {
            LOG.info("Connection handle socket pool shutdown cleanly");
         } else {
            LOG.warn("Connection handler socket pool failed to shutdown cleanly." );
         }

         boolean retValTwo;
         try {
            final Promise<Void> promise = tcpServer.shutdown();
            retValTwo = promise.awaitSuccess(timeoutNanos, TimeUnit.NANOSECONDS);
            if ( retValTwo ) {
               LOG.info("TCP server component offline!");
            } else {
               LOG.warn("TCP server component failed to shutdown cleanly!");
            }
         } catch (final InterruptedException e) {
            LOG.warn("TCP server component shutdown thread interrupted!");
            Thread.interrupted();
            retValTwo = false;
         }

         return Boolean.valueOf(retValOne && retValTwo);
      };
   }
}


iport java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.Environment
import reactor.core.Dispatcher
import reactor.io.codec.DelimitedCodec
import reactor.io.codec.StringCodec
import reactor.io.net.ChannelStream
import reactor.io.net.NetStreams
import reactor.io.net.ReactorChannelHandler
import reactor.io.net.impl.netty.tcp.NettyTcpServer
import reactor.io.net.tcp.TcpServer
import reactor.rx.Promise

class Server<T> {
	package Logger LOG = LoggerFactory::getLogger(typeof(Server))
	// We cannot prevent the server from opening additional connections beyond five, but we can detect when the
	// condition has occurred and refuse to read anything from the 6th socket onward. It has to be Atomic since reactor uses a pool of I/O threads to invoke the RegisterChannelHandler (TcpServer.start()'s single argument) at runtime, so lock-free concurrent access through volatile memory is the least intrusive we can get...
	final AtomicInteger socketsPermitted = new AtomicInteger(5)
	final Dispatcher defaultDispatcher = Environment::sharedDispatcher()
	final TcpServer<String, String> tcpServer
	ReactorChannelHandler<String, String, ChannelStream<String, String>> handler

	new() {
		this.tcpServer = NetStreams::<String, String>tcpServer(typeof(NettyTcpServer), null) // Set the bind address and Reactor environment.
		// Set a codec that will separate the input stream on line feeds and return each sequence of characters
		// between delimiters as a byte array.
	}

	def void startupServer() {
		this.tcpServer.start(handler)
	}

	def void shutdownServer() {
		synchronized (tcpServer) {
			val Promise<Void> shutdownResult = tcpServer.shutdown()
			val long begin_t = System::currentTimeMillis()
			try {
				if (shutdownResult.awaitSuccess(Constants::SERVER_SHUTDOWN_TOLERANCE_IN_SECONDS, TimeUnit::SECONDS)) {
					val long delta_t = System::currentTimeMillis() - begin_t
					System::err.println(
						String::format(
							"Server gracefully shutdown in %f milliseconds.  Shutting down reactor engine...", delta_t))
				} else {
					System::err.println(
						String::format(
							"Graceful server shutdown tolerance exceeded after %d seconds.  Attempting reactor engine shutdown anyway...",
							Constants::SERVER_SHUTDOWN_TOLERANCE_IN_SECONDS))
				}
			} catch (InterruptedException e) {
				System::err.
					println(
						"Graceful server shutdown aborted by thread interrupt signal.  Attempting reactor engine shutdown anyway..."
					)
				e.printStackTrace()
			} catch (Exception e) {
				System::err.
					println(
						"Graceful server shutdown aborted by thrown exception.  Attempting reactor engine shutdown anyway..."
					)
				e.printStackTrace()
			}
			tcpServer.notifyAll()
		}

	}

}

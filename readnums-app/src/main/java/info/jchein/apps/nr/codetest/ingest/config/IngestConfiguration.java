package info.jchein.apps.nr.codetest.ingest.config;


import java.util.ArrayList;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import com.google.common.base.Preconditions;

import info.jchein.apps.nr.codetest.ingest.app.ApplicationWatchdog;
import info.jchein.apps.nr.codetest.ingest.app.console.ConsoleFactory;
import info.jchein.apps.nr.codetest.ingest.app.console.IConsole;
import info.jchein.apps.nr.codetest.ingest.messages.EventConfiguration;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.messages.MessageInput;
import info.jchein.apps.nr.codetest.ingest.perfdata.RingBufferUtils;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.BatchInputSegment;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.BufferStatusSegment;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.UniqueMessageTrie;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.WriteOutputFileSegment;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.ConnectionHandler;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.InputMessageCodec;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.ServerSegment;
import reactor.Environment;
import reactor.bus.EventBus;
import reactor.core.Dispatcher;
import reactor.core.config.DispatcherType;
import reactor.core.dispatch.wait.AgileWaitingStrategy;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.timer.HashWheelTimer;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.DelimitedCodec;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

@Configuration
@Import( {ParametersConfiguration.class, EventConfiguration.class} )
// @EnableReactor("reactor-environment")
public class IngestConfiguration
{
   private static final Logger LOG = LoggerFactory.getLogger(IngestConfiguration.class);

   IngestConfiguration() { }

   @Autowired
   ParametersConfiguration paramsConfig;

   @Autowired
   EventConfiguration eventsConfig;

   /*=====================+
    | Reactor Environment |
    +=====================*/

   @Bean
   @Scope("singleton")
   public Environment reactorEnvironment() {
		// The default error journal handler is already ideal.
		// .initialize(throwable -> LOG.error("Reactor environment caught an unhandled error", throwable))
		return Environment.initialize()
			.assignErrorJournal();
   }

   /*=====================+
    | Application Console |
    +=====================*/


   @Bean
   @Scope("singleton")
   public IConsole applicationConsole() {
      return ConsoleFactory.getConsole();
   }


   /*=======================+
    | LifeCycle and Control |
    +=======================*/

   @Bean
   @Scope("singleton")
   public Dispatcher lifecycleEventBusDispatcher()
   {
      final String dispatcherName =
      	paramsConfig.lifecycleEventBusName + "Dispatcher";
		final Dispatcher eventBusDispatcher =
         Environment.newDispatcher(
            dispatcherName,
				RingBufferUtils.nextSmallestPowerOf2(paramsConfig.lifecycleEventBusBufferSize),
				paramsConfig.lifecycleEventBusThreads, DispatcherType.RING_BUFFER);

      reactorEnvironment()
      	.setDispatcher(dispatcherName, eventBusDispatcher);

      return eventBusDispatcher;
   }


   @Bean
   @Scope("singleton")
   public EventBus lifecycleEventBus()
   {
      return EventBus.config()
			.env(reactorEnvironment())
			.dispatcher(lifecycleEventBusDispatcher())
			.dispatchErrorHandler(t -> {
				t.fillInStackTrace();
				LOG.error("First match EventBus trapped a dispatcher error:", t);
			})
			.uncaughtErrorHandler(t -> {
				t.fillInStackTrace();
				LOG.error("Uncaught error trapped by first match EventBus:", t);
			})
			.consumerNotFoundHandler(
				key -> LOG.error("No consumer found for routing Event with key {}", key))
			.firstEventRouting()
			.get();
   }

   @Bean
   @Scope("singleton")
   @Autowired
   ApplicationWatchdog applicationWatchdog( final ConfigurableApplicationContext applicationContext ) {
      return new ApplicationWatchdog(
			applicationConsole(), codec(), streamsToMerge(), applicationContext
      );
   }


   @Bean
   @Scope("singleton")
   @Autowired
   Consumer<Void> terminateApplicationConsumer( final ApplicationWatchdog applicationWatchdog ) {
      return applicationWatchdog.getTerminateApplicationConsumer();
   }


   /*========================+
    | Access to Input Bridge |
    +========================*/

	@Bean
	@Scope("prototype")
	AgileWaitingStrategy agileWaitingStrategy()
	// LiteBlockingWaitStrategy agileWaitingStrategy()
	{
		final AgileWaitingStrategy retVal = new AgileWaitingStrategy();
		// retVal.nervous();
		return retVal;
		// return new LiteBlockingWaitStrategy();
	}

   @Bean
   @Scope("singleton")
	Dispatcher handoffDispatcher()
   {
		return Environment.newDispatcher(
			"handoffDispatcher",
			RingBufferUtils.nextSmallestPowerOf2(paramsConfig.fanOutRingBufferSize),
			paramsConfig.dataPartitionCount,
			DispatcherType.RING_BUFFER);
	}


	// @Bean
	// @Scope("singleton")
	// Broadcaster<Stream<GroupedStream<Integer, MessageInput>>> mergedSocketsBroadcaster()
	// {
	// return Broadcaster.<Stream<GroupedStream<Integer, MessageInput>>> create(
	// reactorEnvironment(), handoffDispatcher());
	// }


   /*================+
    | Access Segment |
    +================*/

   @Bean
   @Scope("singleton")
   Codec<Buffer, MessageInput, MessageInput> codec() {
		return new DelimitedCodec<>(
         true, codecDelegate());
   }

   @Bean
   @Scope("singleton")
   InputMessageCodec codecDelegate() {
		return new InputMessageCodec(paramsConfig.dataPartitionCount);
	}


   @Bean
   @Scope("singleton")
   @Autowired
   ConnectionHandler connectionHandler( final Consumer<Void> terminationConsumer )
   {
      return new ConnectionHandler(
			paramsConfig.maxConcurrentSockets, terminationConsumer, handoffDispatcher(), streamsToMerge());
   }


	@Bean
	@Scope("singleton")
	Broadcaster<Stream<MessageInput>> streamsToMerge()
	{
		// return Broadcaster.create(reactorEnvironment(), handoffDispatcher());
		return Broadcaster.create(reactorEnvironment(), lifecycleEventBusDispatcher());
	}


   @Bean
   @Scope("singleton")
   @Autowired
   ServerSegment serverSegment( final ConnectionHandler connectionHandler ) {
      return new ServerSegment(
         this.paramsConfig.bindHost,
         this.paramsConfig.bindPort,
         this.paramsConfig.maxConcurrentSockets,
         this.paramsConfig.socketReceiveBufferBytes,
         this.paramsConfig.socketTimeoutMilliseconds,
         lifecycleEventBus(),
         reactorEnvironment(),
         connectionHandler,
         codec()
      );
   }

   /*========================+
    | Input Batching Segment |
    +========================*/


   @Bean
   @Scope("singleton")
   Timer ingestionTimer()
   {
      // final Timer retVal = new HashWheelTimer(
      return new HashWheelTimer(
         "inputBatchingTimer",
         this.paramsConfig.ingestionTimerResolutionMillis,
         RingBufferUtils.nextSmallestPowerOf2(this.paramsConfig.ingestionTimerWheelSize),
         this.paramsConfig.ingestionTimerWaitKind.get(),
         Executors.newFixedThreadPool(
         	1, new NamedDaemonThreadFactory("inputBatchingTimer")));
		// Environment.get()...;
		// return retVal;
   }


   @Bean
   @Scope("singleton")
   UniqueMessageTrie uniqueMessageTrie()
   {
      return new UniqueMessageTrie();
   }


   @Bean
   @Scope("singleton")
	// Processor<GroupedStream<Integer, MessageInput>, GroupedStream<Integer, MessageInput>>[]
   Dispatcher[] fanOutDispatchers()
	{
		// final Processor<GroupedStream<Integer, MessageInput>, GroupedStream<Integer, MessageInput>>[] fanOutProcessors =
      //    new Processor[paramsConfig.dataPartitionCount];
      final Dispatcher[] fanOutDispatchers = new Dispatcher[paramsConfig.dataPartitionCount];
      for (int ii = 0; ii < paramsConfig.dataPartitionCount; ii++) {
         fanOutDispatchers[ii] =
				Environment.newDispatcher(
					"fanOutDispatcher-" + ii,
					RingBufferUtils.nextSmallestPowerOf2(paramsConfig.fanOutRingBufferSize), 4,
					DispatcherType.RING_BUFFER);
			// new RingBufferDispatcher(
			// err -> LOG.error("Error", err), ProducerType.MULTI, liteDispatchWaitStrategy());
      }

		return fanOutDispatchers;
   }


	ArrayList<Broadcaster<MessageInput>> fanOutBroadcasters()
	{
		Dispatcher[] fanOutDispatchers = fanOutDispatchers();
		ArrayList<Broadcaster<MessageInput>> retVal = new ArrayList<>(fanOutDispatchers.length);

		for (final Dispatcher dispatcher : fanOutDispatchers) {
			retVal.add(Broadcaster.create(reactorEnvironment(), dispatcher));
		}

		return retVal;
	}


   @Bean
   @Scope("singleton")
   BatchInputSegment batchInputSegment() {
      return new BatchInputSegment(
         paramsConfig.flushAfterNInputs,
         paramsConfig.flushEveryInterval,
         paramsConfig.flushEveryTimeUnits,
         paramsConfig.dataPartitionCount,
         lifecycleEventBus(),
         ingestionTimer(),
         uniqueMessageTrie(),
         streamsToMerge(),
			eventsConfig.writeFileBufferAllocator(),
			fanOutBroadcasters());
   }


   //=================//
   // Output Pipeline //
   //=================//

	@Bean
	@Scope("singleton")
	RingBufferProcessor<IWriteFileBuffer> writeOutputFileProcessor()
	{
		return RingBufferProcessor.<IWriteFileBuffer> share(
			"writeOutputFileProcessor",
			RingBufferUtils.nextSmallestPowerOf2(paramsConfig.writeOutputRingBufferSize),
			agileWaitingStrategy(), true);
	}


   @Bean
   @Scope("singleton")
   WriteOutputFileSegment writeOutputFileSegment()
   {
		final short numConcurrentWriters = paramsConfig.numConcurrentWriters;
		final int reportIntervalInSeconds = paramsConfig.reportIntervalSeconds;

		Preconditions.checkArgument(
			numConcurrentWriters == 1,
			"No more than one output writer thread and at least one output writer thread is supported at this time.");
		// Preconditions.checkArgument(
		// RingBufferUtils.nextSmallestPowerOf2(numConcurrentWriters) == numConcurrentWriters,
		// "Concurrent writers configuration parameter must be set to a power of 2");

      return new WriteOutputFileSegment(
			paramsConfig.outputLogFilePath, numConcurrentWriters, reportIntervalInSeconds,
			lifecycleEventBus(), ingestionTimer(), batchInputSegment(), writeOutputFileProcessor(),
			eventsConfig.incrementCountersAllocator());
   }


	// ==============================//
	// Buffer Status Report Segment //
	// ==============================//


	@Bean(autowire = Autowire.BY_TYPE, name = "bufferStatusSegment")
   @Scope("singleton")
	BufferStatusSegment bufferStatusSegment()
   {
		return new BufferStatusSegment(
			this.paramsConfig.reportIntervalSeconds, lifecycleEventBus(), ingestionTimer());
   }
}

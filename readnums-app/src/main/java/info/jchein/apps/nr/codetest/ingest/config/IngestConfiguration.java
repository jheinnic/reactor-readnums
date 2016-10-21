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
import info.jchein.apps.nr.codetest.ingest.messages.ICounterIncrements;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.perfdata.RingBufferUtils;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.BatchInputSegment;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.PerfCounterSegment;
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
	AgileWaitingStrategy liteDispatchWaitStrategy()
	{
		return new AgileWaitingStrategy();
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
	// Broadcaster<Stream<GroupedStream<Integer, IInputMessage>>> mergedSocketsBroadcaster()
	// {
	// return Broadcaster.<Stream<GroupedStream<Integer, IInputMessage>>> create(
	// reactorEnvironment(), handoffDispatcher());
	// }


   /*================+
    | Access Segment |
    +================*/

   @Bean
   @Scope("singleton")
   Codec<Buffer, IInputMessage, IInputMessage> codec() {
		return new DelimitedCodec<>(
         true, codecDelegate());
   }

   @Bean
   @Scope("singleton")
   InputMessageCodec codecDelegate() {
		return new InputMessageCodec(
			paramsConfig.dataPartitionCount, eventsConfig.inputMessageAllocator());
   }


   @Bean
   @Scope("singleton")
   @Autowired
   ConnectionHandler connectionHandler( final Consumer<Void> terminationConsumer )
   {
      return new ConnectionHandler(
			paramsConfig.maxConcurrentSockets,
			terminationConsumer,
			streamsToMerge());
   }


	@Bean
	@Scope("singleton")
	Broadcaster<Stream<IInputMessage>> streamsToMerge()
	{
		return Broadcaster.create(reactorEnvironment(), handoffDispatcher());
	}


   @Bean
   @Scope("singleton")
   @Autowired
   ServerSegment serverSegment( final ConnectionHandler connectionHandler ) {
      return new ServerSegment(
         paramsConfig.bindHost,
         paramsConfig.bindPort,
         paramsConfig.maxConcurrentSockets,
         paramsConfig.socketReceiveBufferBytes,
         paramsConfig.socketTimeoutMilliseconds,
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
         paramsConfig.batchTimerResolutionMillis,
         RingBufferUtils.nextSmallestPowerOf2(paramsConfig.batchTimerWheelSize),
         paramsConfig.batchTimerWaitKind.get(),
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


	// @Bean
	// @Scope("singleton")
	// Dispatcher rawInputDispatcher()
	// {
	// Dispatcher retVal =
	// new WorkQueueDispatcher(
	// "rawInputDispatcher", 5,
	// RingBufferUtils.nextSmallestPowerOf2(paramsConfig.fanOutRingBufferSize),
	// err -> LOG.error("Error", err), ProducerType.MULTI, liteDispatchWaitStrategy());
	//
	// Environment.dispatcher("rawInputDispatcher", retVal);
	// return retVal;
	// }


   @Bean
   @Scope("singleton")
	// Processor<GroupedStream<Integer, IInputMessage>, GroupedStream<Integer, IInputMessage>>[]
   Dispatcher[] fanOutDispatchers()
	{
		// final Processor<GroupedStream<Integer, IInputMessage>, GroupedStream<Integer, IInputMessage>>[] fanOutProcessors =
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


	ArrayList<Broadcaster<IInputMessage>> fanOutBroadcasters()
	{
		Dispatcher[] fanOutDispatchers = fanOutDispatchers();
		ArrayList<Broadcaster<IInputMessage>> retVal = new ArrayList<>(fanOutDispatchers.length);

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
			handoffDispatcher(),
			fanOutBroadcasters());
   }
   
	// @Bean
	// @Scope("singleton")
	// FillWriteBuffersSegment fillWriteBuffersSegment() {
	// return new FillWriteBuffersSegment(
	// lifecycleEventBus(),
	// batchInputSegment(),
	// eventsConfig.writeFileBufferAllocator());
	// }


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
			liteDispatchWaitStrategy(), true);
	}


	// @Bean
	// @Scope("singleton")
	// Dispatcher writeOutputFileDispatcher()
	// {
	// final int bufferSize = paramsConfig.writeOutputRingBufferSize;
	//
	// return Environment.newDispatcher(
	// "writeOutputFileDispatcher",
	// RingBufferUtils.nextSmallestPowerOf2(bufferSize),
	// 1, DispatcherType.RING_BUFFER);
	// }


   @Bean
   @Scope("singleton")
   WriteOutputFileSegment writeOutputFileSegment()
   {
		final short numConcurrentWriters = paramsConfig.numConcurrentWriters;
		Preconditions.checkArgument(
			numConcurrentWriters == 1,
			"No more than one output writer thread and at least one output writer thread is supported at this time.");
		// Preconditions.checkArgument(
		// RingBufferUtils.nextSmallestPowerOf2(numConcurrentWriters) == numConcurrentWriters,
		// "Concurrent writers configuration parameter must be set to a power of 2");

      return new WriteOutputFileSegment(
         paramsConfig.outputLogFilePath,
         numConcurrentWriters,
         lifecycleEventBus(),
         batchInputSegment(),
			writeOutputFileProcessor(),
         eventsConfig.incrementCountersAllocator());
   }


   //=====================//
   // PerfCounter Segment //
   //=====================//

   @Bean
   @Scope("singleton")
   RingBufferProcessor<ICounterIncrements> perfCounterProcessor()
   {
		final byte numConcurrentWriters = paramsConfig.numConcurrentWriters;
		if (numConcurrentWriters == 1) {
			return RingBufferProcessor.<ICounterIncrements> share(
				"statsProcessor",
				RingBufferUtils.nextSmallestPowerOf2(paramsConfig.perfCounterRingBufferSize),
				new AgileWaitingStrategy());
		} else {
			return RingBufferProcessor.<ICounterIncrements> share(
				"statsProcessor",
				RingBufferUtils.nextSmallestPowerOf2(paramsConfig.perfCounterRingBufferSize),
				new AgileWaitingStrategy());
		}
   }

	// @Bean
	// @Scope("singleton")
	// Dispatcher perfCounterDispatcher()
	// {
	// final short numConcurrentWriters = paramsConfig.numConcurrentWriters;
	// Preconditions.checkArgument(
	// numConcurrentWriters == 1,
	// "No more than one output writer thread and at least one output writer thread is supported at this time.");
	//
	// return Environment.newDispatcher(
	// "statsAggregator",
	// RingBufferUtils.nextSmallestPowerOf2(paramsConfig.perfCounterRingBufferSize),
	// 1, DispatcherType.RING_BUFFER);
	// }


	@Bean
	@Scope("singleton")
   Timer reportingTimer()
   {
      return new HashWheelTimer(
         "reportingTimer",
         paramsConfig.reportTimerResolutionMillis,
         RingBufferUtils.nextSmallestPowerOf2(paramsConfig.reportTimerWheelSize),
			paramsConfig.reportTimerWaitKind.get(),
			Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("reportingTimer")));
   }


   @Bean( autowire=Autowire.BY_TYPE, name="perfCounterSegment" )
   @Scope("singleton")
   PerfCounterSegment perfCounterSegment()
   {
      return new PerfCounterSegment(
         paramsConfig.reportIntervalSeconds,
         lifecycleEventBus(),
         reportingTimer(),
         writeOutputFileSegment(),
         perfCounterProcessor(),
         eventsConfig.incrementCountersAllocator());
   }
}

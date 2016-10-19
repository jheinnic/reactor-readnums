package info.jchein.apps.nr.codetest.ingest.config;

import java.util.concurrent.Executors;

import org.reactivestreams.Processor;
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
import info.jchein.apps.nr.codetest.ingest.segments.logunique.FillWriteBuffersSegment;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.PerfCounterSegment;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.UniqueMessageTrie;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.WriteOutputFileSegment;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.ConnectionHandler;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.InputMessageCodec;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.ServerSegment;
import reactor.Environment;
import reactor.bus.EventBus;
import reactor.core.Dispatcher;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.wait.AgileWaitingStrategy;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.timer.HashWheelTimer;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.DelimitedCodec;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
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
   ParametersConfiguration parametersConfiguration;

   @Autowired
   EventConfiguration eventsConfiguration;

   /*=====================+
    | Reactor Environment |
    +=====================*/

   @Bean
   @Scope("singleton")
   public Environment reactorEnvironment() {
      return Environment.initialize().assignErrorJournal();
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
      final Dispatcher eventBusDispatcher =
         Environment.newDispatcher(
            parametersConfiguration.lifecycleEventBusName + "Dispatcher",
            RingBufferUtils.nextSmallestPowerOf2(
               parametersConfiguration.lifecycleEventBusBufferSize),
            parametersConfiguration.lifecycleEventBusThreads);
      reactorEnvironment().setDispatcher(
         parametersConfiguration.lifecycleEventBusName + "Dispatcher", eventBusDispatcher);

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
         key -> LOG.error("No consumer found for routing Event with key {}", key)
      )
      .firstEventRouting()
      .get();
   }

   @Bean
   @Scope("singleton")
   @Autowired
   ApplicationWatchdog applicationWatchdog( final ConfigurableApplicationContext applicationContext ) {
      return new ApplicationWatchdog(
         applicationConsole(), codec(), mergedSocketsBroadcaster(), applicationContext
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
   @Scope("singleton")
   Dispatcher mergedSocketsDispatcher()
   {
      final Dispatcher mergedSocketsDispatcher =
         new RingBufferDispatcher(
            "mergedSocketsDispatcher",
            RingBufferUtils.nextSmallestPowerOf2(parametersConfiguration.batchInputRingBufferSize),
            err -> LOG.error("Error", err),
            ProducerType.MULTI,
            new AgileWaitingStrategy());
            //new BusySpinWaitStrategy());

      // Environment.newDispatcher( "mergedSocketsDispatcher", 1);
      reactorEnvironment().setDispatcher("mergedSocketsDispatcher", mergedSocketsDispatcher);
      return mergedSocketsDispatcher;
   }


   @Bean
   @Scope("singleton")
   Broadcaster<IInputMessage> mergedSocketsBroadcaster() {
      return Broadcaster.<IInputMessage>create(
         reactorEnvironment(), mergedSocketsDispatcher());
   }


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
         parametersConfiguration.dataPartitionCount,
         parametersConfiguration.inputMsgAllocBatchSize,
         eventsConfiguration.inputMessageAllocator());
   }


   @Bean
   @Scope("singleton")
   @Autowired
   ConnectionHandler connectionHandler( final Consumer<Void> terminationConsumer )
   {
      return new ConnectionHandler(
         parametersConfiguration.maxConcurrentSockets,
         streamsToMerge(),
         terminationConsumer );
   }

   @Bean
   @Scope("singleton")
   @Autowired
   ServerSegment serverSegment( final ConnectionHandler connectionHandler ) {
      return new ServerSegment(
         parametersConfiguration.bindHost,
         parametersConfiguration.bindPort,
         parametersConfiguration.maxConcurrentSockets,
         parametersConfiguration.socketReceiveBufferBytes,
         parametersConfiguration.socketTimeoutMilliseconds,
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
      return new HashWheelTimer(
         "inputBatchingTimer",
         parametersConfiguration.batchTimerResolutionMillis,
         RingBufferUtils.nextSmallestPowerOf2(parametersConfiguration.batchTimerWheelSize),
         parametersConfiguration.batchTimerWaitKind.get(),
         Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory(
            "inputBatchingTimer-run")));
   }


   @Bean
   @Scope("singleton")
   Broadcaster<Stream<IInputMessage>> streamsToMerge()
   {
      return Broadcaster.create(reactorEnvironment(), SynchronousDispatcher.INSTANCE);
   }


   @Bean
   @Scope("singleton")
   UniqueMessageTrie uniqueMessageTrie()
   {
      return new UniqueMessageTrie();
   }


   @Bean
   @Scope("singleton")
   Processor<IInputMessage,IInputMessage>[] fanOutProcessors() {
      @SuppressWarnings("unchecked")
      final Processor<IInputMessage, IInputMessage>[] fanOutProcessors =
         new Processor[parametersConfiguration.dataPartitionCount];
      for (int ii = 0; ii < parametersConfiguration.dataPartitionCount; ii++) {
         fanOutProcessors[ii] =
            RingBufferProcessor.create(
               "fanOutProcessor-" + ii, parametersConfiguration.batchInputRingBufferSize, true);
      }

      return fanOutProcessors;
   }


   @Bean
   @Scope("singleton")
   BatchInputSegment batchInputSegment() {
      return new BatchInputSegment(
         parametersConfiguration.flushAfterNInputs,
         parametersConfiguration.flushEveryInterval,
         parametersConfiguration.flushEveryTimeUnits,
         parametersConfiguration.dataPartitionCount,
         lifecycleEventBus(),
         ingestionTimer(),
         uniqueMessageTrie(),
         streamsToMerge(),
			eventsConfiguration.rawInputBatchAllocator(),
         fanOutProcessors());
   }
   
   @Bean
   @Scope("singleton")
   FillWriteBuffersSegment fillWriteBuffersSegment() {
   	return new FillWriteBuffersSegment(
   		lifecycleEventBus(),
   		batchInputSegment(), 
   		eventsConfiguration.writeFileBufferAllocator());
   }


   //=================//
   // Output Pipeline //
   //=================//

   @Bean
   @Scope("singleton")
   RingBufferWorkProcessor<IWriteFileBuffer> writeOutputFileWorkProcessor()
   {
      return RingBufferWorkProcessor.<IWriteFileBuffer> share(
         "writeOutputFileWorkProcessor",
         RingBufferUtils.nextSmallestPowerOf2(parametersConfiguration.writeLogRingBufferSize));
   }


   @Bean
   @Scope("singleton")
   WriteOutputFileSegment writeOutputFileSegment()
   {
      final byte numConcurrentWriters = parametersConfiguration.numConcurrentWriters;
      Preconditions.checkArgument(
         RingBufferUtils.nextSmallestPowerOf2(numConcurrentWriters) == numConcurrentWriters,
         "Conccurent writers configuration parameter must be set to a power of 2");

      return new WriteOutputFileSegment(
         parametersConfiguration.outputLogFilePath,
         numConcurrentWriters,
         lifecycleEventBus(),
			fillWriteBuffersSegment(),
         writeOutputFileWorkProcessor(),
         eventsConfiguration.incrementCountersAllocator());
   }


   //=====================//
   // PerfCounter Segment //
   //=====================//

   @Bean
   @Scope("singleton")
   RingBufferProcessor<ICounterIncrements> perfCounterProcessor()
   {
		final byte numConcurrentWriters = parametersConfiguration.numConcurrentWriters;
		if (numConcurrentWriters == 1) {
			return RingBufferProcessor.<ICounterIncrements> create(
				"perfCounterRingBuffer",
				RingBufferUtils.nextSmallestPowerOf2(parametersConfiguration.perfCounterRingBufferSize));
		} else {
			return RingBufferProcessor.<ICounterIncrements> share(
				"perfCounterRingBuffer",
				RingBufferUtils.nextSmallestPowerOf2(parametersConfiguration.perfCounterRingBufferSize));
		}
   }

   @Bean
   @Scope("singleton")
   Timer reportingTimer()
   {
      return new HashWheelTimer(
         "reportingTimer",
         parametersConfiguration.reportTimerResolutionMillis,
         RingBufferUtils.nextSmallestPowerOf2(parametersConfiguration.reportTimerWheelSize),
         parametersConfiguration.reportTimerWaitKind.get(),
         Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory(
            "reporting-timer-run")));
   }


   @Bean( autowire=Autowire.BY_TYPE, name="perfCounterSegment" )
   @Scope("singleton")
   PerfCounterSegment perfCounterSegment()
   {
      return new PerfCounterSegment(
         parametersConfiguration.reportIntervalSeconds,
         lifecycleEventBus(),
         reportingTimer(),
         writeOutputFileSegment(),
         perfCounterProcessor(),
         eventsConfiguration.incrementCountersAllocator());
   }
}

package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.reactivestreams.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.ICounterIncrements;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Streams;


public class PerfCounterSegment
extends AbstractSegment
{
   private static final Logger LOG = LoggerFactory.getLogger(PerfCounterSegment.class);

   // private final IReusableAllocator<IncrementCounters> counterDeltaAllocator;
   // private final IReusableAllocator<PerformanceReport> performanceReportAllocator;
   // private final CounterRotation counterRotation;
//   private final long pulseInterval = 100;
//   private final TimeUnit pulseTimeUnit = TimeUnit.MILLISECONDS;
//   private final int pulseCountPerReport = 100;

   private final long reportIntervalInSeconds;
   private final Timer reportTimer;
   private final WriteOutputFileSegment writeOutputFileSegment;
   private final Processor<ICounterIncrements,ICounterIncrements> counterIncrementsProcessor;
	private final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator;

	private final CounterOverall cumulativeValues = new CounterOverall();
   private final ArrayList<IStatsProvider> bufferStatProviders = new ArrayList<>();

   public PerfCounterSegment(
      final long updateResolutionInSeconds,
      final EventBus eventBus,
      final Timer reportingTimer,
      final WriteOutputFileSegment writeOutputFileSegment,
      final Processor<ICounterIncrements, ICounterIncrements> counterIncrementsProcessor,
      final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator )
   {
      super(eventBus);
      reportIntervalInSeconds = updateResolutionInSeconds;
      reportTimer = reportingTimer;
      this.writeOutputFileSegment = writeOutputFileSegment;
      this.counterIncrementsProcessor = counterIncrementsProcessor;
		this.counterIncrementsAllocator = counterIncrementsAllocator;

      bufferStatProviders.add(
         new ResourceStatsAdapter(
            "Performance Counter Aggregation RingBufferProcessor",
            counterIncrementsProcessor));
   }


   @Override
   public int getPhase()
   {
		return 800;
   }

   /**
    * Send a modest pulse of zeros to ensure that messages reporting 0 progress continue during times of inactivity.
    *
    * @return A Control object used to stop the heartbeat during a graceful shutdown. private Control wireHeartbeat() {
    *         final int[] heartbeat = {0, 0};
    *
    *         return Streams.period(Environment.timer(), 0, 1) .consume( pulse -> {
    *         this.countingBroadcaster.onNext(heartbeat); if (LOG.isInfoEnabled()) { if (this.inputRingBuffer != null) {
    *         logRingBufferStatus("Input RingBuffer", this.inputRingBuffer); }
    *
    *         if (this.outputRingBuffer != null) { logRingBufferStatus("Output RingBuffer", this.outputRingBuffer); } }
    *         } ); }
    */

   @Override
   public Function<Event<Long>, Boolean> doStart()
   {
      LOG.info("Performance tracking segment is activating its data flow pipeline and timer");
      LOG.info("Console reporting interval is every {} seconds", Long.valueOf(reportIntervalInSeconds));

      final ReentrantLock shutdownLock = new ReentrantLock();
      final boolean[] seenOnComplete = {false, false};
      final Condition completed = shutdownLock.newCondition();

		writeOutputFileSegment.getReportCounterIncrementsStream()
			// .mergeWith(Streams.period(reportTimer, reportIntervalInSeconds)
			// .map(evt -> counterIncrementsAllocator.allocate()
			// .setDeltas(0, 0))
			// .log("pulse"))
			.process(counterIncrementsProcessor)
			.observeCancel(evt -> {
				// Toggle the first seenOnComplete flag once input to the final window is recognized by
				// observing a SHUTDOWN event being fed to the window boundary. Note that we are taking
				// advantage observeCancel()'s bug that causes it to trigger on SHUTDOWN signals rather
				// than CANCEL signals since there is no native observeShutdown() observer!
				shutdownLock.lock();
				try {
					seenOnComplete[0] = true;
					LOG.info(
						"Performance stats segment receives an end of stream signal.  No additional data will follow.");
				}
				finally {
					shutdownLock.unlock();
				}
			})
			.window(reportIntervalInSeconds, TimeUnit.SECONDS, reportTimer)
			.consume(statStream -> {
				statStream.startWith(
					Streams.just(
						counterIncrementsAllocator.allocate()
							.setDeltas(0, 0))
				).reduce(
						counterIncrementsAllocator.allocate(),
						(final ICounterIncrements prevStat, final ICounterIncrements nextStat) -> {
					nextStat.beforeRead();
					prevStat.incrementDeltas(nextStat);
					nextStat.release();
					return prevStat;
				})
					.consume(deltaSum -> {
					// First argument to format aggregates the total unique counter and returns the duration
					// since the last update. Remaining arguments are simple getters. If this is later
					// rearranged, understand that initial call to incrementUniqueValues() establishes time
					// duration for subsequent call to getTotalDuration() as well as total unique counter for
					// subsequent call to getTotalUniques(). Call to incrementUniqueValues() must therefore
					// precede either call to other two methods called out in this comment.
					String preSummary =
						String.format(
							"During the last %d seconds, %d unique 9-digit inputs were logged and %d redundant inputs were discarded.\nSince service launch (%d seconds), %d unique 9-digit inputs have been logged.\n\n",
							Long.valueOf(
								TimeUnit.NANOSECONDS.toSeconds(
									cumulativeValues.incrementUniqueValues(deltaSum.getDeltaUniques()))),
							Integer.valueOf(deltaSum.getDeltaUniques()),
							Integer.valueOf(deltaSum.getDeltaDuplicates()),
							Long.valueOf(TimeUnit.NANOSECONDS.toSeconds(cumulativeValues.getTotalDuration())),
							Integer.valueOf(cumulativeValues.getTotalUniques()));
					deltaSum.release();

					final StringBuilder strBldr = new StringBuilder().append(preSummary);
					for (final IStatsProvider statsProvider : bufferStatProviders) {
						strBldr.append("Buffer usage for ")
							.append(statsProvider.getName())
							.append(" reported as ")
							.append(statsProvider.getFreeBufferSlots())
							.append(" slots available out of a total of ")
							.append(statsProvider.getTotalBufferCapacity())
							.append(" allocated.\n");
					}

					LOG.info(strBldr.toString());
				});
			});

		LOG.info("Data collection is online");

		return evt -> {
			long nanosTimeout = evt.getData()
				.longValue();

			// terminalControl.cancel();
			shutdownLock.lock();
			try {
				while (seenOnComplete[0] == false) {
					try {
						nanosTimeout = completed.awaitNanos(nanosTimeout);
					}
					catch (final InterruptedException e) {
						LOG.error("Clean shutdown aborted by thread interruption!");
						Thread.interrupted();
						return Boolean.FALSE;
               }
            }

				LOG.info("Performance stats segment acknowledges it is prepared for a clean shutdown");
				return Boolean.TRUE;
			}
			finally {
				shutdownLock.unlock();
			}
		};
   }

   @Autowired
   public void setRingBufferResources( final Collection<IStatsProviderSupplier> suppliers ) {
      for (final IStatsProviderSupplier nextSupplier : suppliers) {
         for (final IStatsProvider nextProvider : nextSupplier.get()) {
            bufferStatProviders.add(nextProvider);
         }
      }
   }
}



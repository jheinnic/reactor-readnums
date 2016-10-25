package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Streams;
import reactor.rx.action.Control;


/**
 * Reactive consumer utility for writing batches of fixed sized byte arrays to a file with each entry followed by a
 * native line separator character.
 *
 * @author John
 */
public class BufferStatusSegment
extends AbstractSegment
implements IStatsProviderSupplier
{
	private static final Logger LOG = LoggerFactory.getLogger(BufferStatusSegment.class);

	private final long reportIntervalInSeconds;
	private final Timer ingestionTimer;
	private final ReentrantLock shutdownLock = new ReentrantLock();
   private final boolean[] seenOnComplete = {false, false};
	private final Condition completed = shutdownLock.newCondition();
	private final ArrayList<IStatsProvider> bufferStatProviders = new ArrayList<>();


	public BufferStatusSegment(
		final long reportIntervalInSeconds,
		final EventBus eventBus,
		final Timer ingestionTimer )
	{
		super(eventBus);
		this.reportIntervalInSeconds = reportIntervalInSeconds;
		this.ingestionTimer = ingestionTimer;
	}


	@Override
	public int getPhase()
	{
		return 1200;
	}


	@Override
	protected Function<Event<Long>, Boolean> doStart()
	{
		final Control terminalControl =
			Streams.period(this.ingestionTimer, this.reportIntervalInSeconds, TimeUnit.SECONDS)
			.consume(counter -> {
				final StringBuilder strBldr = new StringBuilder(2048);
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

		LOG.info("Buffer Stats reporting is online");

		return evt -> {
			long nanosTimeout = evt.getData()
				.longValue();

			shutdownLock.lock();
			try {
				if (seenOnComplete[0] == false) {
					terminalControl.cancel();
				}
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

				LOG.info("Write output file with counters segment acknowledges a clean shutdown");
				return Boolean.TRUE;
			}
			finally {
				shutdownLock.unlock();
			}
		};
	}


	@Override
	public Iterable<IStatsProvider> get()
	{
		return Collections.emptyList();
	}


	@Autowired
	public void setRingBufferResources(final Collection<IStatsProviderSupplier> suppliers)
	{
		for (final IStatsProviderSupplier nextSupplier : suppliers) {
			for (final IStatsProvider nextProvider : nextSupplier.get()) {
				bufferStatProviders.add(nextProvider);
			}
		}
	}
}

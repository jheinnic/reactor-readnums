package info.jchein.apps.nr.codetest.ingest.lifecycle;


import static info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage.UNDER_CONSTRUCTION;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import com.google.common.base.Preconditions;

import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;
import reactor.fn.Function;


public abstract class AbstractSegment
implements SmartLifecycle
{
	private static final AtomicReferenceFieldUpdater<AbstractSegment, LifecycleStage> LIFECYCLE_STAGE_UPDATER =
		AtomicReferenceFieldUpdater.newUpdater(AbstractSegment.class, LifecycleStage.class, "stage");

	private static final Event<Long> SHUTDOWN_EVENT = Event.wrap(Long.valueOf(-1));

	@SuppressWarnings("unused")
	private volatile LifecycleStage stage = UNDER_CONSTRUCTION;
   private Function<Event<Long>, Boolean> shutdownFn = null;
	private final EventBus eventBus;
	private final Logger log;


   protected AbstractSegment( final EventBus eventBus )
   {
		this.eventBus = eventBus;
		this.log = LoggerFactory.getLogger(this.getClass());
   }


	protected AbstractSegment( final EventBus eventBus, Logger log )
	{
		this.eventBus = eventBus;
		this.log = log;
   }


   protected abstract Function<Event<Long>, Boolean> doStart();


   private final LifecycleStage getLifecycleStage()
   {
		return LIFECYCLE_STAGE_UPDATER.get(this);
   }


	private boolean updateLifecycle(final LifecycleStage fromStage, final LifecycleStage toStage)
   {
		return LIFECYCLE_STAGE_UPDATER.compareAndSet(this, fromStage, toStage);
   }


   @Override
   public final boolean isRunning()
   {
      return getLifecycleStage() == LifecycleStage.ACTIVE;
   }


   @Override
   public final boolean isAutoStartup()
   {
      return true;
   }


   @Override
	public final void start()
   {
		Preconditions.checkState(
			updateLifecycle(LifecycleStage.UNDER_CONSTRUCTION, LifecycleStage.READY),
			"Could not transition from UNDER_CONSTRUCTION to READY.  stage = {}", 
			getLifecycleStage());

		try {
			shutdownFn = doStart();

			if (shutdownFn != null) {
				assert updateLifecycle(LifecycleStage.READY, LifecycleStage.ACTIVE);
			} else {
				log.error("Reactor stage returned null shutdown function at startup");
				assert updateLifecycle(LifecycleStage.READY, LifecycleStage.FAILED_TO_START);
				throw new IllegalStateException("Null shutdown function at startup");
			}
		} catch (Throwable e) {
			log.error("Reactor stage threw an exception at startup: ", e);
			assert updateLifecycle(LifecycleStage.READY, LifecycleStage.FAILED_TO_START);
			throw new IllegalStateException("Exception thrown at startup", e);
		}
   }



   @Override
	public final void stop()
   {
		final LifecycleStage initial = getLifecycleStage();
		Preconditions.checkState(
			initial == LifecycleStage.ACTIVE || initial == LifecycleStage.FAILED_TO_START,
			"Stop may only be called for a stage that is ACTIVE or FAILED_TO_START.  stage = {}", initial);
		
		log.info("stop() requested of {} from {}", this, initial);

		if (initial == LifecycleStage.FAILED_TO_START) return;
		assert updateLifecycle(LifecycleStage.ACTIVE, LifecycleStage.SHUTTING_DOWN);

		try {
			if (shutdownFn.apply(SHUTDOWN_EVENT) == Boolean.TRUE) {
				assert updateLifecycle(LifecycleStage.SHUTTING_DOWN, LifecycleStage.GRACEFULLY_SHUTDOWN);
			} else {
				log.error("Reactor stage shutdown handler returned an inabiity to shutdown");
				assert updateLifecycle(LifecycleStage.SHUTTING_DOWN, LifecycleStage.ABORTED);
				throw new IllegalStateException(
					"Reactor stage shutdown handler returned an inabiity to shutdown");
			}
		}
		catch (Throwable e) {
			assert updateLifecycle(LifecycleStage.SHUTTING_DOWN, LifecycleStage.CRASHED);
			throw new RuntimeException("Failed to shutdown");
		}
   }


   @Override
	public final void stop(final Runnable callback)
   {
		final LifecycleStage initial = getLifecycleStage();
		Preconditions.checkState(
			initial == LifecycleStage.ACTIVE || initial == LifecycleStage.FAILED_TO_START,
			"Stop may only be called for a stage that is ACTIVE or FAILED_TO_START.  stage = {}", initial);

		log.info("stop() requested of {} from {}", this, initial);

		if (initial == LifecycleStage.FAILED_TO_START) {
			callback.run();
			return;
		}
		assert updateLifecycle(LifecycleStage.ACTIVE, LifecycleStage.SHUTTING_DOWN);

		this.eventBus.receive(Selectors.object(this), (Event<Long> event) -> {
			try {
				if (this.shutdownFn.apply(event) == Boolean.TRUE) {
					return(LifecycleStage.GRACEFULLY_SHUTDOWN);
				} else {
					this.log.error("Reactor stage shutdown handler returned an inabiity to shutdown");
					return(LifecycleStage.ABORTED);
				}
			} catch (Throwable e) {
				this.log.error("Failed to shutdown");
				return(LifecycleStage.CRASHED);
			}
		});

      this.eventBus.sendAndReceive(this, SHUTDOWN_EVENT.copy(), result -> {
			assert updateLifecycle(
				LifecycleStage.SHUTTING_DOWN, (LifecycleStage) result.getData());
			callback.run();
      });
   }
}

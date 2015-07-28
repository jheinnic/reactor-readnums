package info.jchein.apps.nr.codetest.ingest.lifecycle;


import static info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage.ACTIVE;
import static info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage.CRASHED;
import static info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage.GRACEFUL_SHUTDOWN;
import static info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage.READY;
import static info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage.SHUTTING_DOWN;
import static info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage.UNDER_CONSTRUCTION;

import java.util.concurrent.locks.ReentrantLock;

import org.springframework.context.SmartLifecycle;

import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;
import reactor.fn.Function;


public abstract class AbstractSegment
implements SmartLifecycle
{
   private final ReentrantLock stageLock = new ReentrantLock();
   private final EventBus eventBus;

   private LifecycleStage stage = UNDER_CONSTRUCTION;
   private Function<Event<Long>, Boolean> shutdownFn = null;


   protected AbstractSegment( final EventBus eventBus )
   {
      this.eventBus = eventBus;
   }


   protected abstract Function<Event<Long>, Boolean> doStart();


   private final LifecycleStage getLifecycleStage()
   {
      stageLock.lock();
      try {
         return stage;
      } finally {
         stageLock.unlock();
      }
   }


   private void updateLifecycle(final LifecycleStage nextStage)
   {
      stageLock.lock();
      try {
         stage = nextStage;
      } finally {
         stageLock.unlock();
      }
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
   public void start()
   {
      stageLock.lock();
      try {
         stage = READY;
         shutdownFn = doStart();
         stage = ACTIVE;
      } finally {
         stageLock.unlock();
      }
   }


   private static final Event<Long> SHUTDOWN_EVENT = Event.wrap(Long.valueOf(-1));


   @Override
   public void stop()
   {
      stageLock.lock();
      try {
         stage = SHUTTING_DOWN;
         if (shutdownFn.apply(SHUTDOWN_EVENT) == Boolean.TRUE) {
            stage = GRACEFUL_SHUTDOWN;
         } else {
            stage = CRASHED;
            throw new RuntimeException("Failed to shutdown");
         }
      } finally {
         stageLock.unlock();
      }
   }


   @Override
   public void stop(final Runnable callback)
   {
      this.eventBus.receive(Selectors.object(this), shutdownFn);
      this.eventBus.sendAndReceive(this, SHUTDOWN_EVENT.copy(), result -> {
         if (result.getData() == Boolean.TRUE) {
            updateLifecycle(GRACEFUL_SHUTDOWN);
            callback.run();
         } else {
            updateLifecycle(CRASHED);
            throw new RuntimeException("Failed to stop!");
         }
      });
   }
}

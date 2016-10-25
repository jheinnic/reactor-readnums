package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;


import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import info.jchein.apps.nr.codetest.ingest.lifecycle.LifecycleStage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.core.processor.CancelException;
import reactor.core.support.Exceptions;
import reactor.io.net.ChannelStream;
import reactor.rx.action.Action;
import reactor.rx.action.Control;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.PushSubscription;


public class ChannelStreamController<IN>
implements Callable<Boolean>
{
	static final Logger LOG = LoggerFactory.getLogger(ChannelStreamController.class);

   private final ChannelStream<IN, ?> channelStream;
   private final ChannelPipeline pipeline;
   
	final AtomicReference<ImmutableList<EndOfStreamAction>> channelTerminations;
	final Phaser eosMonitorPhaser = new Phaser(1) {
      @Override
      protected boolean onAdvance(int phase, int registeredParties) {
         return true;
      }
   };

   private final ReentrantLock stageLock = new ReentrantLock();
   private final Condition shutdownState = stageLock.newCondition();
   private LifecycleStage stage = LifecycleStage.READY;

   
   ChannelStreamController( final ChannelStream<IN, ?> channelStream ) 
   {
      this.channelStream = channelStream;
      this.pipeline = ((Channel) channelStream.delegate()).pipeline();
		this.channelTerminations = new AtomicReference<>(
         ImmutableList.<EndOfStreamAction>builder().build()
      );
   }

   
   Control routeMessagesTo( Broadcaster<IN> messageBroadcaster )
   {
      final EndOfStreamAction eosAction =
         new EndOfStreamAction(messageBroadcaster);

      channelStream.subscribe(eosAction);
      return eosAction;
   }
   
   
   void start()
   {
      stageLock.lock();
      try {
         if (stage != LifecycleStage.READY) {
            throw new IllegalStateException("State must be at READY to start, not " + stage);
         }
         for (final EndOfStreamAction terminalPoint : channelTerminations.get()) {
            terminalPoint.requestMore(Long.MAX_VALUE);
         }
         stage = LifecycleStage.ACTIVE;
      } finally {
         stageLock.unlock();
      }
   }
   

   private void cancelSubscriptions()
   {
      List<EndOfStreamAction> terminalPoints = channelTerminations.get();
      do {
         for (final EndOfStreamAction terminalPoint : terminalPoints) {
            terminalPoint.cancel();
         }
         terminalPoints = channelTerminations.get();
      } while (terminalPoints.isEmpty() == false);
   }
   
   
   public boolean awaitShutdown( long timeoutNs ) {
      stageLock.lock();
      try {
         long timeoutLeft = (timeoutNs > 0) ? timeoutNs : 1;
			while (stage != LifecycleStage.GRACEFULLY_SHUTDOWN &&
				stage != LifecycleStage.CRASHED && timeoutLeft > 0)
			{
            if (timeoutNs > 0) {
               timeoutLeft = shutdownState.awaitNanos(timeoutLeft);
            } else {
               shutdownState.await();
            }
         }
         
			return stage == LifecycleStage.GRACEFULLY_SHUTDOWN;
      } catch (InterruptedException e) {
         LOG.error("Thread interrupted while shutting down socket for " + channelStream.remoteAddress());
         Thread.interrupted();
         return false;
      } finally {
         stageLock.unlock();
      }
   }

   
   @Override
   public Boolean call() {
      Boolean retVal = Boolean.TRUE;

      stageLock.lock();
      try {
         if (stage == LifecycleStage.ACTIVE) {
            stage = LifecycleStage.SHUTTING_DOWN;
         } else {
            LOG.warn("Can only perform shutdown from ACTIVE, not from {}.", stage);
            return Boolean.FALSE;
         }
      } finally {
         stageLock.unlock();
      }

      // Cancel the subscription to avoid race conditions on closing the connection. 
      cancelSubscriptions();
      LOG.info("Cancel signal sent to all channel subscribers");
      
      // Wait for each EndOfStreamAction to get either an onComplete or an onError call
      eosMonitorPhaser.arriveAndAwaitAdvance();
      LOG.info("All channels have received either onError or onComplete");
      
      // Disconnect the now unused pipeline
      try {
         pipeline.disconnect().syncUninterruptibly();
         LOG.info("Channel from {} has been closed successfully", channelStream.remoteAddress());
      } catch (Throwable e) {
         LOG.error(
            String.format("Failed to disconnect connection to %s due to error", channelStream.remoteAddress()), e);
         retVal = Boolean.FALSE;
      }
      
      stageLock.lock();
      try {
         if (retVal == Boolean.TRUE) { 
				stage = LifecycleStage.GRACEFULLY_SHUTDOWN;
         } else {
            stage = LifecycleStage.CRASHED;
         }
         shutdownState.signalAll();
      } finally {
         stageLock.unlock();
      }
      
      LOG.info("Any threads waiting for a shutdown complete signal have been notified");
      return retVal;
   }


   class EndOfStreamAction
   extends Action<IN, Void>
   {
      private final Broadcaster<? super IN> broadcastTo;

      EndOfStreamAction( Broadcaster<? super IN> broadcastTo )
      {
         this.capacity = Long.MAX_VALUE;
         this.broadcastTo = broadcastTo;
      }


      @Override
      public void requestMore(long n) {
         PushSubscription<IN> upstreamSubscription = this.upstreamSubscription;
         if (upstreamSubscription != null) {
            TailRecurseDispatcher.INSTANCE.dispatch(Long.valueOf(n), upstreamSubscription, null);
         } else {
            LOG.error("A call to EndOfStreamAction#requestMore(n) should not be possible before having an upstream subscription!");
         }
      }
      
      
      @Override
      protected void doOnSubscribe(Subscription sub) {
			// Add one to the number of arrivals needed before the eosMonitor will allow the
			// final arrival in callShutdown (Callable.call()) to return successfully.
         ChannelStreamController.this.eosMonitorPhaser.register();

         ImmutableList<EndOfStreamAction> previousList = null;
         ImmutableList<EndOfStreamAction> nextList = null;
         do {
            previousList = channelTerminations.get();
            ImmutableList.Builder<EndOfStreamAction> listBuilder = 
               ImmutableList.<EndOfStreamAction>builder();
            listBuilder.addAll(previousList);
            listBuilder.add(this);
            nextList = listBuilder.build();
         } while(channelTerminations.compareAndSet(previousList, nextList) == false);
         
         LOG.info(
            "onSubscribe -> {}",
            nextList.stream().map(EndOfStreamAction::toString).collect(Collectors.joining(", ")));

         this.requestMore(Long.MAX_VALUE);
      }
      
      
      @Override
      public void cancel() {
         ImmutableList<EndOfStreamAction> previousList = null;
         ImmutableList<EndOfStreamAction> nextList = null;
         do {
            previousList = channelTerminations.get();
            nextList =
               ImmutableList.<EndOfStreamAction>builder()
               .addAll(
                  previousList.stream()
                  .filter( candidate -> candidate != this )
                  .iterator())
               .build();
         } while(channelTerminations.compareAndSet(previousList, nextList) == false);

         super.cancel();
      }
      
      
      @Override
      protected void doShutdown() {
         ChannelStreamController.this.eosMonitorPhaser.arriveAndDeregister();
      }


      @Override
      protected void doNext(IN ev)
      {
         if ((this.broadcastTo != null) && (this.broadcastTo.isPublishing())) {
            try {
               this.broadcastTo.onNext(ev);
            } catch(CancelException ce){
               throw ce;
            } catch (Throwable throwable) {
               doError(Exceptions.addValueAsLastCause(throwable, ev));
            }
         } else {
            throw CancelException.get();
         }
      }


      @Override
      protected void doError(Throwable ev)
      {
         cancel();

        /* if ((this.broadcastTo != null) && (this.broadcastTo.isPublishing())) {
            this.broadcastTo.onError(ev);
         } else { */
            Exceptions.throwIfFatal(ev);
        //  }
         super.doError(ev);
      }


      @Override
      protected void doComplete()
      {
         cancel();
         /* if ((this.broadcastTo != null) && (this.broadcastTo.isPublishing())) {
            this.broadcastTo.onComplete();
         } */
         super.doComplete();
      }


      @Override
      public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity)
      {
         return false;
      }


      @Override
      public Dispatcher getDispatcher()
      {
         return SynchronousDispatcher.INSTANCE;
      }


      @Override
      public String toString()
      {
         return super.toString() + "{streamsPending=" + (eosMonitorPhaser.getUnarrivedParties()-1) + "}";
      }
   }
}   

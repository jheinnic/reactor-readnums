package info.jchein.apps.nr.codetest.ingest.app;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

import info.jchein.apps.nr.codetest.ingest.app.console.IConsole;
import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.broadcast.Broadcaster;

public class ApplicationWatchdog
implements SmartLifecycle
{
   private final ReentrantLock lock = new ReentrantLock();
   
   private final Condition appIsWatching = lock.newCondition();
   private boolean isAppThreadWatching = false;

   private final Condition terminalLifecycleEvent = lock.newCondition();
   private Runnable terminateCallback = null;

   @NotNull
   private final IConsole console;
   
   @NotNull
   private final ConfigurableApplicationContext appCtxt;

   @NotNull
   private final Consumer<Void> terminateAppConsumer;
   
   @NotNull
   private final Thread consoleManagerThread;
   
   @NotNull
   private final ConsoleManager consoleManager;
   

   public ApplicationWatchdog(
      @NotNull IConsole console,
      @NotNull Codec<Buffer, IInputMessage, IInputMessage> codec,
      @NotNull Broadcaster<?> broadcaster,
      @NotNull ConfigurableApplicationContext appCtxt
   ) {
      this.console = console;
      this.appCtxt = appCtxt;
      this.terminateAppConsumer = v -> appCtxt.close();
      this.consoleManager =
         new ConsoleManager(console, codec, broadcaster, this.terminateAppConsumer);
      this.consoleManagerThread = new Thread(this.consoleManager);
   }
   
   
   void blockUntilTerminatedOrInterrupted() {
      lock.lock();
      try {
         isAppThreadWatching = true;
         appIsWatching.signalAll();
         this.consoleManagerThread.start();

         try {
            while(terminateCallback == null) {
               terminalLifecycleEvent.await();
            }

            this.consoleManagerThread.interrupt();
            this.console.format("Application Watchdog observed graceful shutdown completion.  Acknowledging signal."); 

            this.terminateCallback.run();
         } catch (InterruptedException e) {
            this.consoleManagerThread.interrupt();
            this.console.format("Application Watchdog thread interrupted.  Executing synchronous abort."); 

            appCtxt.stop();
            Thread.interrupted();
         }
      } finally {
         lock.unlock();
      }
   }
   
   
   @Bean
   @Scope("singleton")
   @Qualifier(Constants.TERMINATE_INGEST_APPLICATION)
   public Consumer<Void> getTerminateApplicationConsumer() {
      return terminateAppConsumer;
   }


   @Override
   public int getPhase()
   {
      return 999;
   }


   @Override
   public boolean isAutoStartup()
   {
      return true;
   }


   @Override
   public boolean isRunning()
   {
      lock.lock();
      try {
         return isAppThreadWatching;
      } finally {
         lock.unlock();
      }
   }
   

   @Override
   public void start()
   {
      lock.lock();
      try {
         while(isAppThreadWatching == false) {
            appIsWatching.awaitUninterruptibly();
         }
      } finally {
         lock.unlock();
      }
   }


   @Override
   public void stop()
   {
      throw new UnsupportedOperationException("Only asynchronous stop is implemented.");
   }


   @Override
   public void stop(Runnable callback)
   {
      lock.lock();
      try {
         terminateCallback = callback;
         terminalLifecycleEvent.signalAll();
      } finally {
         lock.unlock();
      }
   }
}

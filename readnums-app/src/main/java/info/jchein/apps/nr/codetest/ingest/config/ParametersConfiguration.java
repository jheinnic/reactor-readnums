package info.jchein.apps.nr.codetest.ingest.config;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import reactor.fn.Supplier;
import reactor.fn.timer.HashWheelTimer;
import reactor.fn.timer.HashWheelTimer.WaitStrategy;

@Configuration
public class ParametersConfiguration
{
   public enum TimerWaitKind implements Supplier<HashWheelTimer.WaitStrategy>
   {
      SLEEP( () -> new HashWheelTimer.SleepWait()),
      YIELD( () -> new HashWheelTimer.YieldingWait()),
      BUSY( () -> new HashWheelTimer.BusySpinWait());

      private final Supplier<HashWheelTimer.WaitStrategy> factory;

      TimerWaitKind( final Supplier<HashWheelTimer.WaitStrategy> factory ) {
         this.factory = factory;
      }

      @Override
      public WaitStrategy get()
      {
         return factory.get();
      }
   }


   /*================================+
    | Lifecycle Event Bus Parameters |
    +================================*/

   @Value("lifecycleEventBus")
   public final String lifecycleEventBusName = null;

   @Value("1")
   public int lifecycleEventBusThreads;

	@Value("8")
   public int lifecycleEventBusBufferSize;

   /*===========================+
    | Access Segment Parameters |
    +===========================*/

   @Value("0.0.0.0")
   public String bindHost;

   @Value("4000")
   public int bindPort;

   @Value("5")
   public int maxConcurrentSockets;

	// @Value("32768")
	@Value("16777216")
   public int socketReceiveBufferBytes;

	@Value("1250")
   public int socketTimeoutMilliseconds;

	// @Value("2048")
	// public int inputMsgAllocBatchSize;

	// @Value("4000000")
	// public int peakMsgsPerSecondExpected;

	@Value("8192")
	public int socketHandoffRingBufferSize;

   /*==========================+
    | Input Segment Parameters |
    +==========================*/

	@Value("4096")
   public int fanOutRingBufferSize;

   @Value("4")
   public byte dataPartitionCount;

   @Value("3200")
   public short flushAfterNInputs;

	@Value("800")
	public short flushOverflowTolerance;

	@Value("1250")
   public long flushEveryInterval;

   @Value("MILLISECONDS")
   public TimeUnit flushEveryTimeUnits;

	@Value("128")
   public int ingestionTimerWheelSize;

	@Value("1250")
   public int ingestionTimerResolutionMillis;

	@Value("SLEEP")
   public TimerWaitKind ingestionTimerWaitKind;

	@Value("131072")
   public int peakExpectedInputsInFlight;


   /*================================+
    | Batch Input Segment Parameters |
    +================================*/

	@Value("512")
	public int peakUnconsolidatedRawBatchesExpected;

   /*===============================+
    | Log Writer Segment Parameters |
    +===============================*/

	@Value("128")
   public int writeOutputRingBufferSize;

   // @Value("#{systemProperties.myProp}:numbers.log")
   @Value("numbers.log")
   public String outputLogFilePath;

   @Value("1")
   public byte numConcurrentWriters;

   @Value("128")
   public int peakUnwrittenOutputBuffersExpected;


   /*=========================================+
    | Performance Tracking Segment Parameters |
    +=========================================*/

	// @Value("128")
	// public int reportTimerWheelSize;

	// @Value("1250")
	// public int reportTimerResolutionMillis;

	// @Value("SLEEP")
	// public TimerWaitKind reportTimerWaitKind;

   @Value("10")
   public int reportIntervalSeconds;

	@Value("64")
   public int perfCounterRingBufferSize;
}
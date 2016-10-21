package info.jchein.apps.nr.codetest.ingest.messages;


import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import info.jchein.apps.nr.codetest.ingest.config.ParametersConfiguration;
import info.jchein.apps.nr.codetest.ingest.perfdata.RingBufferUtils;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import info.jchein.apps.nr.codetest.ingest.reusable.ReusableObjectAllocator;


@Configuration
@Import(ParametersConfiguration.class)
public class EventConfiguration
{
	// private static final Logger LOG = LoggerFactory.getLogger(ParametersConfiguration.class);

   @Autowired
   ParametersConfiguration paramsConfig;


   @Bean
   @Scope("singleton")
   public IReusableAllocator<IInputMessage> inputMessageAllocator()
   {
		// return new InputMessageAllocator(2 * parametersConfiguration.peakExpectedInputsInFlight, false);
		return new ReusableObjectAllocator<IInputMessage, InputMessage>(
			2 * paramsConfig.peakExpectedInputsInFlight,
			(callback, index) -> new InputMessage(callback, index), false);
   }


	// @Bean
	// @Scope("singleton")
	// public IReusableAllocator<IRawInputBatch> rawInputBatchAllocator()
	// {
	// // return new RawInputBatchAllocator(
	// // 2 * parametersConfiguration.peakUnconsolidatedRawBatchesExpected, false,
	// // parametersConfiguration.flushAfterNInputs + parametersConfiguration.flushOverflowTolerance);
	// final int bufferMaxMsgs =
	// paramsConfig.flushAfterNInputs + paramsConfig.flushOverflowTolerance;
	// final int initialCount =
	// RingBufferUtils.nextSmallestPowerOf2(paramsConfig.peakUnconsolidatedRawBatchesExpected);
	//
	// return new ReusableObjectAllocator<IRawInputBatch, RawInputBatch>( initialCount,
	// (callback, index) -> new RawInputBatch(callback, index, bufferMaxMsgs), false);
	// }


   @Bean
   @Scope("singleton")
   public IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator()
   {
      // WriteFileBufferAllocator.BUFFER_SIZE_IN_MESSAGES = parametersConfiguration.maxMessagesPerFileBuffer;

		// return new WriteFileBufferAllocator(
		// paramsConfig.peakUnwrittenOutputBuffersExpected * 2,
		// false, paramsConfig.flushAfterNInputs);
		final int bufferMaxMsgs = paramsConfig.flushAfterNInputs + paramsConfig.flushOverflowTolerance;
		final int initialCount =
			RingBufferUtils.nextSmallestPowerOf2(paramsConfig.peakUnconsolidatedRawBatchesExpected);
		final AtomicLong nextWriteOffset = new AtomicLong(0);

		return new ReusableObjectAllocator<IWriteFileBuffer, WriteFileBuffer>(
			initialCount, 
			(callback, index) -> new WriteFileBuffer(callback, index, nextWriteOffset, bufferMaxMsgs),
			false);
   }


   @Bean
   @Scope("singleton")
   public IReusableAllocator<ICounterIncrements> incrementCountersAllocator()
   {
		// return new CounterIncrementsAllocator(
		// 2 * paramsConfig.peakUnwrittenOutputBuffersExpected, false);
		final int initialCount =
			RingBufferUtils.nextSmallestPowerOf2(paramsConfig.peakUnwrittenOutputBuffersExpected);

		return new ReusableObjectAllocator<ICounterIncrements, CounterIncrements>(
			initialCount, (callback, index) -> new CounterIncrements(callback, index), false);
   }

   // @Bean
   // @Scope("singleton")
   // public IReusableAllocator<PerformanceReport> performanceReportAllocator() {
   // return new ReusableObjectAllocator<PerformanceReport>(
   // (onReturned, poolIndex) -> new PerformanceReport(onReturned, poolIndex));
   // }
}

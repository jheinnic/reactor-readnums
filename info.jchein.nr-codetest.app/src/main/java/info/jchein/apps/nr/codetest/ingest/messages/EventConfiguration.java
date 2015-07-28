package info.jchein.apps.nr.codetest.ingest.messages;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import info.jchein.apps.nr.codetest.ingest.config.ParametersConfiguration;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;


@Configuration
@Import(ParametersConfiguration.class)
public class EventConfiguration
{
   private static final Logger LOG = LoggerFactory.getLogger(ParametersConfiguration.class);

   @Autowired
   ParametersConfiguration parametersConfiguration;


   @Bean
   @Scope("singleton")
   public IReusableAllocator<IInputMessage> inputMessageAllocator()
   {
//      final int peakMsgsPerMillisecondExpected =
//         (parametersConfiguration.peakMsgsPerSecondExpected / 1000);
//      final int timeBasedEstimate =
//         Math.toIntExact(
//            (peakMsgsPerMillisecondExpected > 0)
//            ? peakMsgsPerMillisecondExpected
//            : parametersConfiguration.flushEveryTimeUnits.toMillis(
//                 parametersConfiguration.flushEveryInterval)
//              * parametersConfiguration.dataPartitionCount * 2);
//      final int volumeBasedEstimate =
//         Math.toIntExact(
//            parametersConfiguration.dataPartitionCount
//            * parametersConfiguration.flushAfterNInputs * 2);
//      final int initialAllocationEstimate =
//         Math.max(timeBasedEstimate, volumeBasedEstimate);
//
//      LOG.info(
//         "Initial allocation for input messages of {} is the larger of a time-based flush estimate ({}) and a volume-based flush estimate ({})",
//         new Object[] {
//            Integer.valueOf(initialAllocationEstimate),
//            Integer.valueOf(timeBasedEstimate),
//            Integer.valueOf(volumeBasedEstimate)
//         });

      return new InputMessageAllocator(2 * parametersConfiguration.peakExpectedInputsInFlight, false);
   }


//   @Bean
//   @Scope("singleton")
//   public IReusableAllocator<IRawInputBatch> rawInputBatchAllocator()
//   {
//      return new RawInputBatchAllocator(
//         2 * parametersConfiguration.peakUnconsolidatedRawBatchesExpected,
//         false, parametersConfiguration.flushAfterNInputs);
//   }


   @Bean
   @Scope("singleton")
   public IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator()
   {
      // WriteFileBufferAllocator.BUFFER_SIZE_IN_MESSAGES = parametersConfiguration.maxMessagesPerFileBuffer;

      return new WriteFileBufferAllocator(
         parametersConfiguration.peakUnwrittenOutputBuffersExpected * 2,
         false, parametersConfiguration.flushAfterNInputs);
   }


   @Bean
   @Scope("singleton")
   public IReusableAllocator<ICounterIncrements> incrementCountersAllocator()
   {
      return new CounterIncrementsAllocator(
         2 * parametersConfiguration.peakUnwrittenOutputBuffersExpected, false);
   }

   // @Bean
   // @Scope("singleton")
   // public IReusableAllocator<PerformanceReport> performanceReportAllocator() {
   // return new ReusableObjectAllocator<PerformanceReport>(
   // (onReturned, poolIndex) -> new PerformanceReport(onReturned, poolIndex));
   // }
}

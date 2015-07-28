package info.jchein.apps.nr.codetest.ingest.segments.logunique;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Processor;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

public class BatchInputSegment
extends AbstractSegment
implements IStatsProviderSupplier
{
   // private static final Logger LOG = LoggerFactory.getLogger(BatchInputSegment.class);

   private final short ioCount;
   private final long ioPeriod;
   private final TimeUnit ioTimeUnit;
   private final byte numDataPartitions;
   private final Timer ingestionTimer;
   private final IUniqueMessageTrie uniqueTest;
   private final Broadcaster<Stream<IInputMessage>> streamsToMerge;
   private final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator;
   private final Processor<IInputMessage,IInputMessage>[] fanOutProcessors;

   private Stream<IWriteFileBuffer> loadedWriteFileBufferStream;

   @SuppressWarnings("unchecked")
   public BatchInputSegment(
      final short ioCount,
      final long ioPeriod,
      final TimeUnit ioTimeUnit,
      final byte numDataPartitions,
      final EventBus eventBus,
      final Timer ingestionTimer,
      final IUniqueMessageTrie uniqueTest,
      final Broadcaster<Stream<IInputMessage>> streamsToMerge,
      final IReusableAllocator<IWriteFileBuffer> writeFileBufferAllocator,
      final Processor<IInputMessage,IInputMessage>[] fanOutProcessors )
   {
      super(eventBus);
      this.ioCount = ioCount;
      this.ioPeriod = ioPeriod;
      this.ioTimeUnit = ioTimeUnit;
      this.uniqueTest = uniqueTest;
      this.ingestionTimer = ingestionTimer;
      this.numDataPartitions = numDataPartitions;
      this.streamsToMerge = streamsToMerge;
      this.writeFileBufferAllocator = writeFileBufferAllocator;
      this.fanOutProcessors = fanOutProcessors;

   }


   @Override
   public int getPhase()
   {
      return 300;
   }


   @Override
   protected Function<Event<Long>, Boolean> doStart()
   {
      loadedWriteFileBufferStream =
         streamsToMerge.startWith(
            Streams.<IInputMessage>never())
         .<IInputMessage> merge()
         .observe(evt -> evt.beforeRead())
         .filter(evt -> evt.getPartitionIndex() >= 0)
         .groupBy(evt -> Byte.valueOf(evt.getPartitionIndex()))
         .<IWriteFileBuffer> flatMap(partitionStream -> {
            final byte partitionIndex =
               partitionStream.key()
               .byteValue();

            return
            partitionStream
            .process(fanOutProcessors[partitionIndex])
            .observe(evt -> evt.beforeRead())
            .window(ioCount, ioPeriod, ioTimeUnit, ingestionTimer)
            .<IWriteFileBuffer> flatMap( nestedWindow -> {
               return
               nestedWindow
//               .log("Input")
               .reduce(
                  writeFileBufferAllocator.allocate(),
                  (final IWriteFileBuffer batch, final IInputMessage msg) -> {
                     if (uniqueTest.isUnique(msg.getPrefix(), msg.getSuffix())) {
                        batch.acceptUniqueInput(msg.getMessageBytes());
                        msg.release();
                     } else {
                        msg.release();
                        batch.trackSkippedDuplicate();
                     }

                     return batch;
                  }
               )
               .observe(writeBuf -> writeBuf.afterWrite());
            });
         });

      return evt -> Boolean.TRUE;
   }


   public Stream<IWriteFileBuffer> getBatchedRawDataStream()
   {
      return loadedWriteFileBufferStream;
   }


   @Override
   public Iterable<IStatsProvider> get()
   {
      final ArrayList<IStatsProvider> retVal =
         new ArrayList<IStatsProvider>(numDataPartitions);
      for (int ii=0; ii<numDataPartitions; ii++) {
         retVal.add(
            new ResourceStatsAdapter(
               "Partitioned Input RingBufferProcessor-" + ii,
               fanOutProcessors[ii]));
      }
      return retVal;
   }
}

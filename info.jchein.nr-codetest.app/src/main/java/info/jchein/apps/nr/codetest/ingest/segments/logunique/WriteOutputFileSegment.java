package info.jchein.apps.nr.codetest.ingest.segments.logunique;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import org.reactivestreams.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.ICounterIncrements;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProvider;
import info.jchein.apps.nr.codetest.ingest.perfdata.IStatsProviderSupplier;
import info.jchein.apps.nr.codetest.ingest.perfdata.ResourceStatsAdapter;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.rx.Stream;


/**
 * Reactive consumer utility for writing batches of fixed sized byte arrays to a file with each entry followed by a
 * native line separator character.
 *
 * @author John
 */
public class WriteOutputFileSegment
extends AbstractSegment
implements IStatsProviderSupplier
{
   private static final Logger LOG = LoggerFactory.getLogger(WriteOutputFileSegment.class);

   private final File outputLogFile;
   private final byte concurrentFileWriters;
   private final BatchInputSegment batchInputSegment;
   private final Processor<IWriteFileBuffer,IWriteFileBuffer> writeOutputFileWorkProcessor;
   private final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator;

   private FileOutputStream outputFileStream;
   private Stream<ICounterIncrements> reportCounterIncrementsStream;


   public WriteOutputFileSegment(
      final String outputFilePath,
      final byte concurrentFileWriters,
      final EventBus eventBus,
      final BatchInputSegment batchInputSegment,
      final Processor<IWriteFileBuffer,IWriteFileBuffer> writeOutputFileWorkProcessor,
      final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator )
   {
      super(eventBus);
      this.concurrentFileWriters = concurrentFileWriters;
      outputLogFile = new File(outputFilePath);
      this.batchInputSegment = batchInputSegment;
      this.writeOutputFileWorkProcessor = writeOutputFileWorkProcessor;
      this.counterIncrementsAllocator = counterIncrementsAllocator;
   }


   @Override
   public int getPhase()
   {
      return 200;
   }


   @Override
   protected Function<Event<Long>, Boolean> doStart()
   {
      LOG.info("Output writer is opening output file");
      openFile();
      LOG.info("Output file is available for writing");

      @SuppressWarnings("unchecked")
      final
      Stream<ICounterIncrements>[] partitions =
         new Stream[concurrentFileWriters];

      final Stream<IWriteFileBuffer> partitionedStreamRoot =
         batchInputSegment.getBatchedRawDataStream()
         .process(writeOutputFileWorkProcessor);

      for (byte ii=0; ii<concurrentFileWriters; ii++) {
         partitions[ii] =
            partitionedStreamRoot
            .map(writeBuffer -> processBatch(writeBuffer));
      }

      for( int nextReduction=(concurrentFileWriters/2), step=1; nextReduction >= 1; step=step * 2, nextReduction = nextReduction / 2) {
         for (int ii=0, leap=step * 2; ii<=nextReduction; ii=ii+leap) {
            partitions[ii] = partitions[ii].mergeWith(partitions[ii+step]);
         }
      }
      reportCounterIncrementsStream = partitions[0];

      return evt -> Boolean.TRUE;
   }


   public Stream<ICounterIncrements> getReportCounterIncrementsStream()
   {
      return reportCounterIncrementsStream;
   }


   void openFile()
   {
      try {
         outputFileStream = new FileOutputStream(outputLogFile, false);
         LOG.info("Output file stream to {} open for writing", outputLogFile);
      } catch (final FileNotFoundException e) {
         if (outputLogFile.exists()) throw new FailedWriteException(outputLogFile, WriteFailureType.EXISTS_NOT_WRITABLE, e);
         else
            throw new FailedWriteException(outputLogFile, WriteFailureType.CANNOT_CREATE, e);
      }
   }


   /**
    * Flush a buffer containing some fixed size entries to the output log, with a native line separator between each
    * entry.
    *
    * It is the caller's responsibility to ensure that the ByteBuffer received has been flipped or is otherwise
    * configured such that its position points at the first byte to write and its limit points at the last.
    *
    * @param accepted
    */
   ICounterIncrements processBatch(final IWriteFileBuffer batchDef)
   {
      batchDef.beforeRead();
      final ByteBuffer buf = batchDef.getBufferToDrain();
      long writeOffset = batchDef.getFileWriteOffset();

      try {
         int bytesWritten = 0;
         int passCount = 0;
         final FileChannel outputChannel = outputFileStream.getChannel();
         while ((bytesWritten >= 0) && buf.hasRemaining()) {
            bytesWritten = outputChannel.write(buf, writeOffset);
            if (bytesWritten > 0) {
               writeOffset += bytesWritten;
            }
            passCount++;
         }

         if ((passCount > 1) && LOG.isInfoEnabled()) {
            LOG.info(String.format(
               "Wrote %d bytes in %d passes, leaving %s",
               buf.position(),
               passCount,
               buf.toString()));
         }

         if ((bytesWritten < 0) || buf.hasRemaining()) throw new FailedWriteException(
            outputLogFile,
            WriteFailureType.WRITE_RETURNS_NEGATIVE,
            writeOffset,
            buf.position(),
            buf.remaining());

         if (LOG.isDebugEnabled()) {
            LOG.debug("Drained buffer to output file and recycled WriteFileBuffer message.", buf);
         }

         final ICounterIncrements retVal =
            batchDef.loadCounterDeltas(
               counterIncrementsAllocator.allocate());
         retVal.afterWrite();
         return retVal;
      } catch (final IOException e) {
         if (LOG.isWarnEnabled()) {
            LOG.warn(String.format(
               "Failed to drain buffer to output file %s at offset %d.  Event still recyled.",
               outputLogFile,
               writeOffset), e);
         }
         throw new FailedWriteException(
            outputLogFile,
            WriteFailureType.IO_EXCEPTION_ON_WRITE,
            writeOffset,
            buf.position(),
            buf.remaining(),
            e);
      } finally {
         batchDef.release();
      }
   }


   void close()
   {
      try {
         outputFileStream.close();
      } catch (final IOException e) {
         LOG.error(String.format(
            "Exception thrown on closing %s while cleaning up resources to shutdown.",
            outputLogFile), e);
         throw new FailedWriteException(outputLogFile, WriteFailureType.IO_EXCEPTION_ON_CLOSE, e);
      }
   }


   @Override
   public Iterable<IStatsProvider> get()
   {
      final ArrayList<IStatsProvider> retVal =
         new ArrayList<IStatsProvider>(1);
      retVal.add(
         new ResourceStatsAdapter(
            "Output File Writers' RingBufferWorkProcessor",
            writeOutputFileWorkProcessor));
      return retVal;
   }
}
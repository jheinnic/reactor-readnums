package info.jchein.apps.nr.codetest.ingest.segments.logunique;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.lifecycle.AbstractSegment;
import info.jchein.apps.nr.codetest.ingest.messages.IRawInputBatch;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.rx.Stream;

public class FillWriteBuffersSegment
extends AbstractSegment
{
   private static final Logger LOG = LoggerFactory.getLogger(FillWriteBuffersSegment.class);

   private final BatchInputSegment batchedInputSegment;
   private final IReusableAllocator<IWriteFileBuffer> writeBufferAllocator;

   private Stream<IWriteFileBuffer> filledBufferStream;

   public FillWriteBuffersSegment(
      final EventBus eventBus,
      final BatchInputSegment batchedInputSegment,
      final IReusableAllocator<IWriteFileBuffer> writeBufferAllocator )
   {
      super(eventBus);

      this.batchedInputSegment = batchedInputSegment;
      this.writeBufferAllocator = writeBufferAllocator;
   }


   @Override
   public int getPhase()
   {
		return 400;
   }


   @Override
   public Function<Event<Long>,Boolean> doStart()
   {
      LOG.info("Activating pipeline segment for consolidating input batches as larger buffers for writing output file");

      // Assuming that all calls to an instance of this action originate on a common thread...
		IWriteFileBuffer[] activeBufferRef = {
			writeBufferAllocator.allocate()
		};

      filledBufferStream =
         batchedInputSegment.getLoadedWriteFileBufferStream()
			.cast(IRawInputBatch.class) // TODO: If this class goes away, it won't matter that this is just silencing
												 // errors!
         .map( evt -> {
            final IWriteFileBuffer retVal;
				if (evt.transferToFileBuffer(activeBufferRef[0])) {
               retVal = null;
            } else {
               // Writing would have overflowed the current buffer.  Its time to emit this event and
               // allocate the next.  Keep the current WriteLogBatch so it can be the first write to
               // the next buffer, since none of it was in the one about to be sealed and sent.

               // activeBuffer.setFileWriteOffset(nextFileWriteOffset);
               // nextFileWriteOffset += activeBuffer.getByteCapacity() - capacityAfter;
					activeBufferRef[0].afterWrite();
					retVal = activeBufferRef[0];
					activeBufferRef[0] = writeBufferAllocator.allocate();
					boolean newWrite = evt.transferToFileBuffer(activeBufferRef[0]);
					assert newWrite;
					// TODO: Subclass a specific Exception class!
					// throw new RuntimeException("Could not populate newly allocated file buffer with initial batch!");
            }

            evt.release();
            return retVal;
         })
         .filter( evt -> evt != null );

      return evt -> Boolean.TRUE;
   }


   Stream<IWriteFileBuffer> getFilledBufferStream() {
      return filledBufferStream;
   }
}

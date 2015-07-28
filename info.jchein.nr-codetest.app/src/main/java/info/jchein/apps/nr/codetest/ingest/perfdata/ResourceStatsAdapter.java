package info.jchein.apps.nr.codetest.ingest.perfdata;

import reactor.jarjar.com.lmax.disruptor.RingBuffer;

/**
 * Adapts a Resource object to expose methods for querying the buffer statistics of its
 * internal RingBuffer.
 *  
 * @author John Heinnickel
 *
 * @param <R> The concrete sub-type of an adapted Resource.
 */
public class ResourceStatsAdapter
implements IStatsProvider
{
   private final String resourceName;
   private final RingBuffer<?> ringBuffer;

   public ResourceStatsAdapter( String resourceName, Object resource )
   {
      this.resourceName = resourceName;
      this.ringBuffer = RingBufferUtils.extractRingBuffer(
         this.resourceName, resource);
      return;
   }
   
   
   @Override
   public String getName()
   {
      return this.resourceName;
   }


   @Override
   public final long getFreeBufferSlots()
   {
      return this.ringBuffer.remainingCapacity();
   }


   @Override
   public final int getTotalBufferCapacity()
   {
      return this.ringBuffer.getBufferSize();
   }


   /*
   @Override
   public final boolean isAlive()
   {
      return this.resource.alive();
   }
   */
}

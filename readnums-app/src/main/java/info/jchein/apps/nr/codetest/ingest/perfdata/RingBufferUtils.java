package info.jchein.apps.nr.codetest.ingest.perfdata;

import java.lang.reflect.Field;

import org.springframework.util.StringUtils;

import com.google.common.base.Preconditions;

import reactor.jarjar.com.lmax.disruptor.RingBuffer;

public final class RingBufferUtils
{
   // private static final Logger LOG = LoggerFactory.getLogger(RingBufferUtils.class);

   private RingBufferUtils() { }
   
   /**
    * Ring Buffers appear in RingBufferProcessor, RingBufferWorkProcessor, RingBufferDispatcher, and WorkQueue.
    * They can also be found in the HashTimer, but the use there seems sufficiently different that we currently
    * exclude it from coverage here.
    * 
    * The sole intended purpose for unwrapping such a tender internal implementation detail from such crucial
    * objects is to access their buffer capacity statistics and report on utilization.  No other interactions are
    * anticipated, encouraged, or met with any demeanor besides panic and horror.
    * 
    * @param role Used to refer to the passed object by some name in log messages.
    * @param dispatchPowered An object that caller is one of the four supported types listed above.  IF so, this
    * method should return its RingBuffer with little problem.  Otherwise, a message will be logged and a null value
    * returned.
    * @return A RingBuffer if one could be found in the supplied object at the expected location.  Otherwise,
    * a null value is returned.  Thrown Exceptions are avoided in favor of null, but a log event with any trapped
    * Exception is created at warning severity.
    * @throws This utility will through NullPointerException on null input.
    */
   static RingBuffer<?> extractRingBuffer( String name, Object dispatchPowered ) {
      Preconditions.checkNotNull(name, "Name may not be null");
      Preconditions.checkArgument(StringUtils.hasText(name), "Name may not be blank");

      if (dispatchPowered == null) {
         return null;
      }

      try {
         final Field ringBufferField;
         ringBufferField = dispatchPowered.getClass().getDeclaredField("ringBuffer");
         ringBufferField.setAccessible(true);
         return (RingBuffer<?>) ringBufferField.get(dispatchPowered);
      } catch (NoSuchFieldException | SecurityException e1) {
         // LOG.warn("Could not find the ringBuffer field for " + role, e1);
      } catch (IllegalArgumentException | IllegalAccessException e2) {
         // LOG.warn("Could not access the ringBuffer field for " + role, e2);
      }
      
      return null;
   }
   
   // Ring buffers must be sized by a power of two.
   private static final double LN_TWO = Math.log(2);
   public static int nextSmallestPowerOf2(int goalValue)
   {
      final int retVal =  Math.round((float) Math.pow(2, Math.ceil(Math.log(goalValue) / LN_TWO)));
      // LOG.info("{} is the smallest power of 2 at least as large as {}", retVal, ioCount);
      return retVal;
   }

}

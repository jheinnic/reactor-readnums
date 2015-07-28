package info.jchein.apps.nr.codetest.ingest.messages;


import info.jchein.apps.nr.codetest.ingest.reusable.ReusableObjectAllocator;

public class RawInputBatchAllocator
extends ReusableObjectAllocator<IRawInputBatch, RawInputBatch>
{
   private static final int DEFAULT_INITIAL_POOL_SIZE = 512;
   private static final boolean DEFAULT_IS_POOL_FIXED = false;


   public RawInputBatchAllocator( final int maxInputsPerFlush )
   {
      this(DEFAULT_INITIAL_POOL_SIZE, DEFAULT_IS_POOL_FIXED, maxInputsPerFlush);
   }


   public RawInputBatchAllocator( final int initialPoolSize, final int maxInputsPerFlush )
   {
      this(initialPoolSize, DEFAULT_IS_POOL_FIXED, maxInputsPerFlush);
   }


   public RawInputBatchAllocator( final boolean poolSizeFixed, final int maxInputsPerFlush )
   {
      this(DEFAULT_INITIAL_POOL_SIZE, poolSizeFixed, maxInputsPerFlush);
   }


   public RawInputBatchAllocator(
      final int initialSize, final boolean poolSizeFixed, final int maxInputsPerFlush )
   {
      super(
         initialSize,
         (onReturned, poolIndex) -> new RawInputBatch(onReturned, poolIndex, maxInputsPerFlush),
         poolSizeFixed);
      }
}

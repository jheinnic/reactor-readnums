package info.jchein.apps.nr.codetest.ingest.messages;


import info.jchein.apps.nr.codetest.ingest.reusable.ReusableObjectAllocator;

public class CounterIncrementsAllocator
extends ReusableObjectAllocator<ICounterIncrements, CounterIncrements>
{
   private static final int DEFAULT_INITIAL_POOL_SIZE = 65536;
   private static final boolean DEFAULT_IS_POOL_FIXED = false;


   public CounterIncrementsAllocator()
   {
      this(DEFAULT_INITIAL_POOL_SIZE, DEFAULT_IS_POOL_FIXED);
   }


   public CounterIncrementsAllocator( final int initialPoolSize )
   {
      this(initialPoolSize, DEFAULT_IS_POOL_FIXED);
   }


   public CounterIncrementsAllocator( final boolean poolSizeFixed )
   {
      this(DEFAULT_INITIAL_POOL_SIZE, poolSizeFixed);
   }


   public CounterIncrementsAllocator( final int initialSize, final boolean poolSizeFixed )
   {
      super(
         initialSize,
         (onReturned, poolIndex) -> new CounterIncrements(onReturned, poolIndex),
         poolSizeFixed);
      }
}

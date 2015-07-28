package info.jchein.apps.nr.codetest.ingest.messages;


import info.jchein.apps.nr.codetest.ingest.reusable.ReusableObjectAllocator;

public class InputMessageAllocator
extends ReusableObjectAllocator<IInputMessage,InputMessage>
{
   private static final int DEFAULT_INITIAL_POOL_SIZE = 262144;
   private static final boolean DEFAULT_POOL_SIZE_FIXED = false;


   public InputMessageAllocator()
   {
      this(DEFAULT_INITIAL_POOL_SIZE, DEFAULT_POOL_SIZE_FIXED);
   }


   public InputMessageAllocator( final int initialPoolSize )
   {
      this(initialPoolSize, DEFAULT_POOL_SIZE_FIXED);
   }


   public InputMessageAllocator( final boolean poolSizeFixed )
   {
      this(DEFAULT_INITIAL_POOL_SIZE, poolSizeFixed);
   }


   public InputMessageAllocator( final int initialSize, final boolean poolSizeFixed )
   {
      super(
         initialSize,
         (onReturned, poolIndex) -> new InputMessage(onReturned, poolIndex),
         poolSizeFixed);
      }
}

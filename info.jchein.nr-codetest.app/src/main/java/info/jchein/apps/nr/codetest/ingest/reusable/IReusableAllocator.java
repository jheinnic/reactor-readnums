package info.jchein.apps.nr.codetest.ingest.reusable;

import java.util.ArrayList;

public interface IReusableAllocator<I extends IReusable>
{
   /**
    * Allocate an object from the internal pool.
    *
    * @return a {@link T} that can be retained and released.
    */
   I allocate();

   /**
    * Allocate a batch of objects all at once.
    *
    * @param size the number of objects to allocate
    *
    * @return a {@code List} of allocated {@link T objects}
    */
   void allocateBatch( int size, ArrayList<I> container );
}

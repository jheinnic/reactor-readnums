package info.jchein.apps.nr.codetest.ingest.reusable;

import java.util.ArrayList;

public interface IReusableAllocator<I extends IReusable>
{
   /**
	 * Reserve a reusable object from the internal pool by acquiring its lease
	 *
	 * @return a leased {@link I object} that has been reserved for use in a blank writable state with one reference to
	 *         release before being recycled. Additional references can be retained until the last is released.
	 */
   I allocate();

   
	/**
	 * Reserve a batch of reusable objects by acquiring each one's lease.
	 *
	 * @param size
	 *           number of objects to reserve
	 *
	 * @return a {@link ArrayList<I> list} of leased {@link I objects}, each reserved for use in a blank writeable state
	 *         with one reference to release before being recycled. Additional references can be retained until the last
	 *         is released.
	 */
   void allocateBatch( int size, ArrayList<I> container );
}

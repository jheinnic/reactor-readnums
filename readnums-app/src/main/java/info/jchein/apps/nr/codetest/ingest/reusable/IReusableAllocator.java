package info.jchein.apps.nr.codetest.ingest.reusable;


@FunctionalInterface
public interface IReusableAllocator<I extends IReusable>
{
   /**
	 * Reserve a reusable object from the internal pool by acquiring its lease
	 *
	 * @return a leased {@link I object} that has been reserved for use in a blank writable state with one reference to
	 *         release before being recycled. Additional references can be retained until the last is released.
	 */
   I allocate();
}

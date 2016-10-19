package info.jchein.apps.nr.codetest.ingest.reusable;

/**
 * Public interface for a classes whose instances are always pre-constructed by some subclass-defined
 * {@link ReusableAllocator} implementation to create a pool of leased objects.  Instances are acquired by
 * calling {@link ReusabableAllocator}#allocate()} and returned by reducing their reference count to 0 using
 * {@link #release()} and/or {@link #release(int)}.  Instances initially have a reference count of 1 when first
 * returned by {@link ReusableAllocator#allocate()}.  Reference counts are increased by {@link #retain()} and
 * {@link #retain(int)}, and they are decreased by {@link #release()} and {@link #release(int)}.
 *
 * It is left as an implementation-defined detail as to whether {@link #retain()} and {@link #retain(int)}
 * may be used and whether or not an object's own interface is affected by loss of a lease.  This interface
 * does not provide a means of identifying the specific lease allocation an object was acquired through,
 * but neither does it preclude concrete implementations from providing such a facility by extension.
 *
 * @author John Heinnickel
 */
public abstract interface IReusable
{
   int getReferenceCount();

   void retain();

   void retain(int incr);

   void release();

   void release(int decr);

	// IReusable afterWrite();

	// IReusable beforeRead();
}

package info.jchein.apps.nr.codetest.ingest.reusable;

import reactor.core.alloc.Allocator;
import reactor.core.alloc.Recyclable;
import reactor.core.alloc.Reference;

/**
 * Non-public implementation glue interface for root of the AbstractReusableObject hierarchy, as part of the
 * info.jchein.apps.nr.codetest.reusable.object package implementation of the abstract interfaces provided by
 * info.jchein.apps.nr.codetest.reusable package.
 * 
 * Public interface for an object that:
 * -- Is constructed by an associated {@link ReusableObjectFactory<T>} that passes {@link OnReturnCallback} and
 * <code>pooledObjetIndex</code> to each {@link IReusable<T>} object it constructs.
 * -- Implements * {@link #retain(int)} and {@link #release(int)} as Atomic operations.
 *    -- The reference count may not be increased once it reaches 0.  Throws IllegalStateException on any attempt
 *       to increment the reference count with {@link #retain(int)} or {@link #retain()} once initial reference
 *       count is 0 or less.
 *    -- Recognizes which call to {@link #release(int)} decrements reference count to 0, and calls both
 *       injected {@link OnReturnCallback} exactly one time when that happens.
 *    -- Implements additional methods from {@link ReusableObjectFactory<T>}'s non-public glue interface for 
 *       object pool members, {@link IReusableObjectInternal<T>}.
 * 
 * NOTE: Reducing the reference count to a value less than zero indicates an implementation bug such that an
 * insufficient number of references was set through {@link Reference#retain(int)} and/or {@link Reference#retain()} 
 * to accurately enforce actual object utilization.  Violations will be tolerated on a best-effort basis and logged at 
 * warning severity.  Bugs that manifest as a consequence of this error will appear as unexpected reads of 
 * as-initialized values and/or values that change while working with an IReusable before calling {@link #release()}.
 * The former error will start to occur just after the last under-allocated {@link #release()} and the latter may
 * manifest once the object has been re-allocated and used for another task.
 * 
 * @author John Heinnickel
 */
public interface IReusableObjectInternal<I extends IReusable>
extends Recyclable, IReusable
{
   I castToInterface();
   
   /**
    * Atomically increment the reservation from zero to one to begin a new reservation. This method is only intended for
    * use by a {@link Allocator} managing the object pool an implementing instance came from. It is needed because the
    * described contract for {@link #retain(int)} may not increase the reference count once it reaches zero. That is a
    * valid rule to enforce for application consumers, but the object pool origin needs an Exception to that rule in
    * order to initialize a new Object lease with a reference count of 1 after it was previously expired at 0.
    * 
    * @throws IllegalStateException
    *            if the reference count was not initially 0.
    */
   int reserve();

  /**
   * 
   */
   void vacate();
   
   /**
    * Every reusable object is assigned an integer index value on creation. That value is injected through the
    * constructor and this interface method enables a Reusable object pool to retrieve that index back to discern the
    * object's identity any time it returns home.
    * 
    * @return This Reusable object's Origin pool-assigned index value, which is unique among all other objects
    *         constructed by that object pool and facilitates identification on return from a reservation.
    */
   int getPoolIndex();

   long getInception();
}
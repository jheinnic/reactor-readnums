package info.jchein.apps.nr.codetest.ingest.reusable;


/**
 * Factory instance used by a {@link ReusableAllocator<?>} to populate its pool of reusable object.  It is meant
 * to be implemented by developers to provide any specialized construction semantics needed to satisfy their
 * reusable artifact's template requirements, if any.
 * 
 * An example use involves creating payload objects that contain some variable sized resource, such as a BitSet
 * or ArrayList, that must be initialized with the same sizing parameters in each instance of a reusable object
 * pool.  In this example, a developer would store sizing parameters inside a ReusableFactory and supply them
 * to the Reusable object's constructor from {@link IReusableObjectFactory#apply(OnReturnCallback, int)}.
 *
 * @param <T> Subclass of Reusable<T> that implementation yields when {@link #apply(OnReturnCallback, int)}
 *             is called. 
  
 * @author John Heinnickel
 */
@FunctionalInterface
public interface IReusableObjectFactory<T extends AbstractReusableObject<?, T>>
{
   /**
    * Accepts an on-release callback and pooled object ID from its parent{@link StrippedReusableObjectAllocator}
    * and returns a new instance prepared for allocation from that pool.
    *
    * <p>This is a <a href="package-summary.html">functional interface</a>
    * whose functional method is {@link #apply(OnReleaseCallback, int)}.
    *
    * @param <T> Subclass of Reusable<T> that implementation yields when {@link #apply(OnReleaseCallback, int)}
    *             is called. 
    * @returns A new instance of T that has been wired with the arguments as a member of caller's managed
    *           reusable object pool.
    *             
    * @see ReusableObjectFactory<T>
    */
    T apply(OnReturnCallback onReturnCallback, int pooledObjectIndex);
}

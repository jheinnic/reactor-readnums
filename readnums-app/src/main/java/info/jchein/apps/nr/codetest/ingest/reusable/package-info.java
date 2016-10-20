/**
 * The Reusables package compromises a tradeoff between separation of concerns and availability of Reactor's object
 * pooling implementation to situations not compatible with exposing that functionality as an object wrapper.
 * 
 * It combines default {@link reactor.core.alloc.ReferenceCountingAllocator}'s implementation of a non-static inner
 * implementation of {@link Reference} to connect each wrapper to its object pool of origin. The only point where this
 * link back to the point of origin is required is when the Reference must broker the one-way transition from a reserved
 * state to an available state as the object's last reference is released and it is recycled for a future reservation.
 * 
 * The Reusables package alternative is to inject each pooled object with a non-static inner implementation of
 * {@link info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback} under a contract that requires calling
 * {@link info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback#get()} once the object determines it is no
 * longer actively referenced.
 * 
 * appropriate one-way transition moment from Reserved to Free by a pooled object's own implementation of the Reference
 * <?> interface. The ReusableFactory interface exists to codify that concept of callback injection and allows the same
 * callback to be used no matter how many allocate()/release() cycles an Object passes through.
 * 
 * The Reusable package provides a facility for developers to opt in the compromise of relaxing full separation of
 * concerns as a tradeoff for compatibility with situations like the ArrayList<? extends AbstractPayload> scenario. It
 * works by letting an object implement the {@link IReusableObject<?>} contract for its own reservation management
 * needs.
 * 
 * @author John
 *
 */
package info.jchein.apps.nr.codetest.ingest.reusable;


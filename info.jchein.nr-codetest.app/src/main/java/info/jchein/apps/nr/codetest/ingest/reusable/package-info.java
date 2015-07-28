/**
 * The Reusables package compromises a tradeoff between separation of concerns and availability of Reactor's object
 * pooling implementation to situations not compatible with exposing that functionality as an object wrapper.
 * 
 * It augments the default {@link reactor.core.alloc.ReferenceCountingAllocator}'s implementation of a non-static 
 * inner implementation of {@link Reference} to connect each wrapper to its object pool of origin.  The only point
 * where this link back to the point of origin is required is when the Reference must broker the one-way transition
 * from a reserved state to an available state as the object's last reference is released and it is recycled for
 * a future reservation.
 * 
 * The Reusables package alternative is to inject each pooled object with a non-static inner implementation of
 * {@link info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback} under a contract that requires calling
 * {@link info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback#get()} once the object determines it is
 * no longer actively referenced.
 * 
 * appropriate one-way transition moment from Reserved to Free by a pooled object's own implementation of the
 * Reference<?> interface.  The ReusableFactory interface exists to codify that concept of callback injection and
 * allows the same callback to be used no matter how many allocate()/release() cycles an Object passes through.
 * 
 * A common trait among the various Reusable object types is that they are self-referencing.  {@link IReusableAllocator}
 * extends Reactor's {@link Allocator} interface, which returns objects wrapped in a separate {@link Reference}
 * object.  This is a healthy separation of concerns, but can only be used in templated scenarios where the object 
 * type is unconstrained.  For example, a List<? extends Object> contract can be satisfied by List<Reference<Payload>>
 * just as well as a List<Payload>.  However, a contract calling for List<? extends AbstractPayload> cannot 
 * co-exist with a Reactor {@link Allocator} because Reference is not compatible with AbstractPayload.
 * 
 * The Reusable package provides a facility for developers to opt in the compromise of relaxing full separation of
 * concerns as a tradeoff for compatibility with situations like the ArrayList<? extends AbstractPayload> scenario.
 * It works by letting an object implement the {@link Reference<?>} contract for its own reservation management
 * needs.
 * @author John
 *
 */
package info.jchein.apps.nr.codetest.ingest.reusable;


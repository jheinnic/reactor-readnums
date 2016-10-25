/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package info.jchein.apps.nr.codetest.ingest.reusable;


import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;

import reactor.core.support.Recyclable;


/**
 * {@link Recyclable} and {@link Reusable} implementation that is intended for use through a single write/read cycle and
 * then returned to the {@link ReusableObjectAllocator} pool from which it was previously reserved once the read phase
 * of its use lifecycle is complete.
 *
 * When used as an event or messaging payload, it is the allocator's responsibility to ensure that reference counting
 * will only reach 0 after the last recipient has finished their work. In general, this means that the reusable packages
 * are not yet appropriate for event/message use cases where the number of actual recipients is unknown when the message
 * is sent.
 *
 * When the number of event/message recipients is known a priori, and is greater than zero, the sender should deduct one
 * from the number of recipients, call {@link #retain(int)} with that sum, and then skip calling {@link #release()}
 * after sending the message due to the -1 optimization. This presumes that delivery is reliable and each recipient can
 * be trusted to call {@link #release()} once for its own use of the message.
 *
 * Future enhancements should expand the range of scenarios where the reusable package is a good fit by tapping into the
 * EventBus's consumer registry and providing an option to utilize that access to set a Reusable's reference count
 * dynamically rather than requiring the sender have appropriate knowledge about recipient(s) and/or routing behavior of
 * the EventBus itself.
 *
 * @param <T>
 *           Every concrete subclass should its own class so {@link Reference<T>} can self resolve.
 * @see ReusableEventAllocator#get(Class)
 */
public abstract class AbstractReusableObject<I extends IReusable, C extends AbstractReusableObject<? super C, C>>
implements IReusableObjectInternal<I>, IReusable, Recyclable
{
	// private static final Logger LOG = LoggerFactory.getLogger(AbstractReusableObject.class);

   // Atomic updater providing atomic access guarantees for reference counter variable, but not responsible for
   // providing a sufficient memory barrier to share subclass state.
	@SuppressWarnings("rawtypes")
	private static AtomicIntegerFieldUpdater<AbstractReusableObject> ATOMIC_REF_COUNT_UPDATE =
		AtomicIntegerFieldUpdater.newUpdater(AbstractReusableObject.class, "refCnt");

	@SuppressWarnings("rawtypes")
	private static AtomicIntegerFieldUpdater<AbstractReusableObject> ATOMIC_SYNC_BIT_UPDATE =
		AtomicIntegerFieldUpdater.newUpdater(AbstractReusableObject.class, "syncBit");

   // An atomic updater for supporting public interface methods that provide concrete subclasses an interface driven
   // memory barrier without resorting to heavier mutual exclusion locks.  The tradeoff is that it is developer's
   // responsibility to ensure all write access is single-threaded.  For use cases where a message is written before
   // being unicast or multicast to reader recipients, this constraint already fits the inherent use pattern.
	// @SuppressWarnings("rawtypes")
	// private static AtomicIntegerFieldUpdater<AbstractReusableObject> ATOMIC_SYNC_BIT_UPDATE =
	// AtomicIntegerFieldUpdater.newUpdater(AbstractReusableObject.class, "syncBit");

	// private final OnReturnCallback releaseCallback;
   private final int poolIndex;
   private final long inception;

   // Reference counter used by public IReusable interface methods to identify the moment it is safe for an instance
   // to recycle itself back to its original object pool.
	private volatile int refCnt = 0;

   // volatile bit solely for the purpose of ensuring memory visibility.  Reference count is not always changed and
   // read along a pattern that is sufficient to support required memory barrier semantics, but it is possible to
   // satisfy the necessity of memory barrier semantics when maintaining the reference counter.  Take the example of
   // a one-to-one exchange.  The sender may be legitimately expected to call afterWrite() and the recipient to
   // call beforeRead(), but it is unlikely that the sender will call "retain(1); release(1);" to increment the
   // retention by one to account for the recipient and then decrement it back to the original value to release its
   // own interest in the object.  Both operations would satisfy the need for a write barrier, but involves an
   // unnecessary identity calculation of 1 + 1 - 1 = 1 and two writes where only one is needed to satisfy
   // memory barrier preconditions.
   //
	// NOTE: The actual value of this variable is completely meaningless, but by convention it stores the value 1
	// when an object is initially reserved. The value changes to 0 when the last write of its current
	// lease is committed by {@link #afterWrite()}, and {@link #beforeRead()} uses this convention to indicate
	// whether or not anything has been written. To ensure that reserved objects are received in a visible
	// and initialized state, {@link #recycle()} will write 1, and {@link #reserve()} will expect to read
	// 1.
   private volatile int syncBit = 1;


   protected AbstractReusableObject( final OnReturnCallback releaseCallback, final int poolIndex )
   {
      super();
		// this.releaseCallback = releaseCallback;
		this.poolIndex = poolIndex;
      this.inception = System.nanoTime();
   }


	protected abstract String getTypeName();


   /*
    * =============================================+ | IReusableObjectInternal Implementation Glue |
    * +=============================================
    */

   @Override
   public final long getInception()
   {
      // return TimeUtils.approxCurrentTimeMillis() - inception;
      return inception;
   }


   @Override
   public final int getPoolIndex()
   {
      return poolIndex;
   }


	@Override
	public final int reserve()
	{
		return this.reserve(1);
	}

   @Override
	public final int reserve(int incr)
   {
		boolean updated = ATOMIC_REF_COUNT_UPDATE.compareAndSet(this, 0, incr);
		while ((updated == false) && (ATOMIC_REF_COUNT_UPDATE.get(this) == 0)) {
			updated = ATOMIC_REF_COUNT_UPDATE.compareAndSet(this, 0, incr);
      }

      Verify.verify(
         updated,
         "Reference count for %s became non-zero while handling reserve(), but it was not updated by thread calling reserve().  Lease age may no longer be accurate.",
         this);
      
		Verify.verify(
			ATOMIC_SYNC_BIT_UPDATE.compareAndSet(this, 1, 2),
			"reserve() was called on a reserved object of type %s with index %s.  Sync bit was %s, expected 1.",
			this.getTypeName(), this.getPoolIndex(), this.syncBit);

		return incr;
   }



   /*
    * ============================+ | IReusable Public Interface | +============================
    */

   @Override
   public final int getReferenceCount()
   {
		return ATOMIC_REF_COUNT_UPDATE.get(this);
   }


   @Override
   public final void retain()
   {
      retain(1);
   }


   @Override
   public final void retain(final int incr)
   {
      Preconditions.checkArgument(incr > 0, "Retention increments must always be positive");
		Preconditions.checkArgument(incr < Integer.MAX_VALUE, "Retention increments may not overflow int");

		// Heuristic avoidance of a volatile read. If the true reference count is not one, the
		// cost is a failed write attempt before we read the actual reference count, but most of
		// the time the value will be 1, and so risking a potential extra failed write pays off.
		int origRefCnt = 1;
		int newRefCnt = incr + 1;
		while ((origRefCnt > 0) && ATOMIC_REF_COUNT_UPDATE.weakCompareAndSet(this, origRefCnt, newRefCnt)) {
			origRefCnt = ATOMIC_REF_COUNT_UPDATE.get(this);
         newRefCnt = origRefCnt + incr;
			Preconditions
				.checkState(newRefCnt > origRefCnt, "Reference counts may not overflow an integer");
      }

      Preconditions.checkState(
			origRefCnt > 0, "Could not increase reference count by %s after it already decreased to %s",
			incr, origRefCnt);
   }


   @Override
   public final void release()
   {
      release(1);
   }


   @Override
   public final void release(final int decr)
   {
      Preconditions.checkArgument(
         decr > 0,
         "Release() can ony decrement a ReusableObject's reference count by a positive number.");

		int origRefCnt = decr;
		int newRefCnt = 0;
		while ((origRefCnt >= decr) &&
			(!ATOMIC_REF_COUNT_UPDATE.weakCompareAndSet(this, origRefCnt, newRefCnt)))
		{
			origRefCnt = ATOMIC_REF_COUNT_UPDATE.get(this);
			newRefCnt = origRefCnt - decr;
         Preconditions.checkState(newRefCnt < origRefCnt, "Reference counts may not overflow to an increased value");
      }

      // Recycle and release if this call successfully updated reference counter from a positive value to 0.
		Preconditions.checkState(
			origRefCnt >= decr, "Cannot decrement ref counter by %s; was %s", decr, origRefCnt);
      if (origRefCnt == decr) {
			// Implementations of recycle() that need to read any object state in order to reset themselves to an
			// initial state are highly discouraged. If such an implementation is unavoidable, implementor is
			// responsible for ensuring reading from a volatile field that was updated after the last state change
			// required.
			this.recycle();
			Preconditions.checkState(
				ATOMIC_SYNC_BIT_UPDATE.compareAndSet(this, 4, 1),
				"release() recycled an object that was not reserved, written, and acknowledged for read.  Sync bit was %s, expected 4.",
				this.syncBit
			);
      }
   }


   //-------------- Public and inherited interface for memory barrier effects, constrained solely to one instance of this object.

   
	/**
	 * Just before sharing an IReusable with another thread, invoke this apparent no-op method to commit any changes
	 * you've made since your last call to this method on this object to memory. Any thread that calls
	 * {@link #beforeRead()} will thereafter be guaranteed to see the latest state change made before most recent call to
	 * {@link #afterWrite()}.
	 *
	 * @see #beforeRead()
	 */
	@SuppressWarnings("unchecked")
	protected C afterWrite()
	{
		Verify.verify(
			ATOMIC_SYNC_BIT_UPDATE.compareAndSet(this, 2, 3),
			"afterWrite() called on an object that was not reserved and uncommitted for write.  Sync bit was %s, expected 2.",
			this.syncBit);
		return (C) this;
	}

   /**
	 * Just after receiving an IReusable from another thread, a call to this method will ensure any changes made before
	 * other thread called {@link #afterWrite()} are visible.
	 *
	 * The actual return value of {@link #beforeRead()} has no meaning, it only exists to provide a Load Memory Barrier
	 * for a concrete implementation's defined state.
	 *
	 * @return Nothing of any significance whatsoever.
	 * @see #afterWrite()
	 */
	@SuppressWarnings("unchecked")
	protected C beforeRead()
	{
		Verify.verify(
			ATOMIC_SYNC_BIT_UPDATE.compareAndSet(this, 3, 4),
			"beforeRead() called on an object that was not reserved and write committed.  Sync bit was %s, expected 3.",
			this.syncBit);
		return (C) this;
	}


   @Override
   public final String toString()
	{
		return new StringBuilder().append("ReusableObject{refCnt=")
			.append(refCnt)
			.append(", syncBit=")
			.append(syncBit)
			.append(", poolIndex=")
			.append(poolIndex)
			.append(", inception=")
			.append(inception)
			.append(", obj=")
			.append(innerToString())
			.append('}')
			.toString();
	}


   protected abstract String innerToString();
}

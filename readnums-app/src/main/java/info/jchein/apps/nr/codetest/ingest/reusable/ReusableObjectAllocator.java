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


import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;


/**
 * An implementation of {@link ReusableAllocator} that uses a reference-counting base class that returns itself to its
 * origin pool for recycling when its reference count drops to 0 after having been previously allocated.
 *
 * @author John Heinnickel
 */
public class ReusableObjectAllocator<I extends IReusable, T extends AbstractReusableObject<I, ?>>
implements IReusableAllocator<I>
{
	private static final Logger LOG = LoggerFactory.getLogger(ReusableObjectAllocator.class);
	private static final int DEFAULT_SIZE = 2048;

	private final BitSet leaseMask;
	private final boolean poolSizeFixed;
	private final ReentrantLock leaseLock;
	private final ArrayList<IReusableObjectInternal<I>> availablePool;
	private final IReusableObjectFactory<I> factory;
	private final OnReturnCallback onReturnCallback;
	private int nextLeasedIndex;
	private boolean upwardSweep;


	public ReusableObjectAllocator( final IReusableObjectFactory<I> factory )
	{
		this(DEFAULT_SIZE, factory, false);
	}


	public ReusableObjectAllocator( final int initialSize, final IReusableObjectFactory<I> factory )
	{
		this(initialSize, factory, false);
	}


	public ReusableObjectAllocator( final int initialSize, final IReusableObjectFactory<I> factory,
		final boolean poolSizeFixed )
	{
		this.factory = factory;
		this.poolSizeFixed = poolSizeFixed;
		this.leaseMask = new BitSet(initialSize);
		this.leaseLock = new ReentrantLock();
		this.availablePool = new ArrayList<>(initialSize);
		this.onReturnCallback = (returnedObject) -> {
			assert returnedObject != null;
			final int index = returnedObject.getPoolIndex();
			leaseLock.lock();
			try {
				assert leaseMask.get(index);
				assert returnedObject.getReferenceCount() == 0;
				leaseMask.clear(index);
			}
			finally {
				leaseLock.unlock();
			}
		};

		this.leaseLock.lock();
		try {
			this.expand(initialSize);
		}
		finally {
			this.leaseLock.unlock();
		}
	}


	@Override
	public I allocate()
	{
		final IReusableObjectInternal<I> retVal =
			this.factory.apply(onReturnCallback, nextLeasedIndex++);
		retVal.reserve();
		return retVal.castToInterface();
	}


	@Override
	public void allocateBatch(final int batchSize, final ArrayList<I> container)
	{
		Preconditions.checkArgument(batchSize > 0, "Allocation batch size must be greater than zero.");
		Preconditions.checkNotNull(
			container,
			"A non-null initially empty container is required for sending your reserved objects back to you.");
		Preconditions.checkArgument(
			container.isEmpty(),
			"The container you send us for recieving your pre-allocated objects must be sent initially empty because we are not yet prepared for what liabilities would apply..");

		leaseLock.lock();
		try {
			// Make sure we've got current taste.
			// if(this.syncBit) { } else { }

			// First ensure there is adequate capacity. If capacity is fixed and the request cannot be fulfilled,
			// its better to fail fast rather than have to undo some number of reservations because we cannot
			// deliver the rest of the lot. And if we can handle dynamic expansion, checking for exhaustion potential
			// only once, before beginning, is more efficient than having to check after every reservation.
			final int initialReserved = leaseMask.cardinality();
			if ((this.availablePool.size() - initialReserved) < batchSize) {
				if (this.poolSizeFixed) throw new IllegalStateException(
					String.format(
						"Cannot allocate a batch of size %d from a fixed size pool of size %d with %d reserved",
						batchSize, this.availablePool.size(), initialReserved));
				else {
					expand(Math.max(this.availablePool.size(), batchSize));
				}
			}

			// Now update lease bits and fill the return list by scanning progressively for runs of free objects
			// until enough have been culled.
			int needed;
			final BitSet pending = new BitSet();
			if (this.upwardSweep) {
				needed = doBulkUpwardSweep(container, batchSize, pending);
				if (needed > 0) {
					needed = doBulkDownwardSweep(container, needed, pending);
				}
			} else {
				needed = doBulkDownwardSweep(container, batchSize, pending);
				if (needed > 0) {
					needed = doBulkUpwardSweep(container, needed, pending);
				}
			}
			if (needed > 0) {
				throw new IllegalStateException("Exhausted bitmask supply despite sufficient capacity");
			}

			// We have a handle on an object set that satisfies the requirements. Now, commit the state changes,
			// release the lock, and do the per-object initialization that no longer touches shared state.
			this.leaseMask.or(pending);

			// TODO: Convert these to asserts
			final int deltaReserved = pending.cardinality();
			final int finalReserved = leaseMask.cardinality();
			Verify.verify(
				((finalReserved - initialReserved) == batchSize), "Cardinality mishmash %s - %s = %s",
				finalReserved, initialReserved, batchSize);
			Verify
				.verify(deltaReserved == batchSize, "Pending mishmash %s == %s", deltaReserved, batchSize);
		}
		finally {
			leaseLock.unlock();
		}
	}


	private int doBulkDownwardSweep(final ArrayList<I> container, int needed, final BitSet pending)
	{
		final int initialNext = this.nextLeasedIndex;
		while ((!this.upwardSweep) && (needed > 0)) {
			int last = this.leaseMask.previousClearBit(this.nextLeasedIndex);
			if (last > 0) {
				int next = this.leaseMask.previousSetBit(last);
				final int found = Math.min(last - next, needed);

				needed = needed - found;
				this.nextLeasedIndex = last - found;
				last = last + 1;
				next = last - found;

				reserveIndexRange(container, next, last, pending);
			} else {
				this.nextLeasedIndex = initialNext + 1;
				this.upwardSweep = true;
			}
		}
		checkForSweepInversion();
		return needed;
	}


	private int doBulkUpwardSweep(final ArrayList<I> container, int needed, final BitSet pending)
	{
		final int initialNext = this.nextLeasedIndex;
		final int len = this.availablePool.size();
		while (this.upwardSweep && (needed > 0)) {
			final int next = this.leaseMask.nextClearBit(this.nextLeasedIndex);

			if (next < len) {
				final int found;
				int last = leaseMask.nextSetBit(next);
				if (last < 0) {
					// A concrete lower bound and a non-existent upper bound means infinite zeroes in the undefined
					// direction. It looks like a non-existent negative range, but that's deceptive. I think the
					// will let us take advantage of it as long as we treat it as such, which means it doesn't matter
					// how much we've asked for, it can all be satisfied` at once. Let's see if that works...
					found = Math.min(len - next, needed);
				} else {
					found = Math.min(last - next, needed);
				}

				needed = needed - found;
				this.nextLeasedIndex = next + found;
				last = this.nextLeasedIndex;
				pending.set(next, last);

				reserveIndexRange(container, next, last, pending);
			} else {
				this.nextLeasedIndex = initialNext - 1;
				this.upwardSweep = false;
			}
		}

		checkForSweepInversion();
		return needed;
	}


	private void checkForSweepInversion()
	{
		// Handle the boundary cases where a full upward sweep turns out to be exhaustive.
		if (this.upwardSweep) {
			final int len = this.availablePool.size();
			if (this.nextLeasedIndex >= len) {
				this.upwardSweep = false;
				this.nextLeasedIndex = len - 1;
			}
		} else {
			if (this.nextLeasedIndex < 0) {
				this.upwardSweep = true;
				this.nextLeasedIndex = 0;
			}
		}
	}


	private void
	reserveIndexRange(final ArrayList<I> container, int next, int last, final BitSet pending)
	{
		// Reserve each entry in the reservation state and verify its critical
		// details are correct. Cross a LOAD based memory barrier specific to
		// the object at hand to be sure its loaded.
		pending.set(next, last);
		availablePool.subList(next, last)
			.stream()
			.map(src -> {
				src.reserve();
				return src.castToInterface();
			})
			.collect(() -> container, ArrayList::add, ArrayList::addAll);
	}


	protected void releaseBatch(final Iterable<T> batch)
	{
		if (batch != null) {
			// Prepare a BitSet that can be applies to the lease BitSet as an "andNot" mask to clear
			// any bit set corresponding to an object in the batch we're processing for return.
			// Allocate a blank BitSet up front to take advantage of a small/sparse input domain potentially.
			final BitSet releaseMask = new BitSet();
			final BitSet checkMask = new BitSet();
			for (final T returned : batch) {
				releaseMask.set(returned.getPoolIndex());
			}
			checkMask.or(releaseMask);

			this.leaseLock.lock();
			try {
				checkMask.andNot(this.leaseMask);
				this.leaseMask.andNot(releaseMask);
				Preconditions
					.checkArgument(checkMask.isEmpty(), "Batch included un-reserved objects: {}", checkMask);
			}
			finally {
				this.leaseLock.unlock();
			}
		}
	}


	/**
	 * Expands the number of available objects in the pool by {@link num}. This method is only meant to be called while
	 * holding the ReusableAllocator's Reentrant Lock, and then only from within a try{ } block ensuring the lock gets
	 * released once the flow control leaves that try block.
	 *
	 * Earlier implementations replaced the BitSet, but that is no longer necessary in later versions of Java, where
	 * BitSet is just as capable of dynamically resizing itself on demand as ArrayList. Just make sure it happens with
	 * the visibility guarantees of a visibility-preserving memory barrier when the BitSet is shared.
	 */
	private void expand(final int num)
	{
		assert num > 0;
		assert this.leaseLock.isHeldByCurrentThread();

		// Verify.verify(
		// this.leaseLock.isHeldByCurrentThread(),
		// "It is only legal to call ReusableObjectAllocator#expand(int) while holding the leaseLock from within a try
		// block that guarantees leaseLock#unlock) will get called on leaving that block.");

		this.upwardSweep = true;
		this.nextLeasedIndex = this.availablePool.size();
		final int newLen = this.nextLeasedIndex + num;
		if (this.nextLeasedIndex > 0) {
	      LOG.warn(
	      	"ReusableObjectAllocator is about to expand available pool size from {} to {}.  Sample pool member: {}",
	      	new Object[] { this.nextLeasedIndex, newLen, availablePool.get(0) });
		} else {
	      LOG.warn(
	      	"ReusableObjectAllocator is about to initialize available pool size at {}.",
	      	new Object[] { newLen });
		}

		this.availablePool.ensureCapacity(newLen);
		for (int ii = this.availablePool.size(); ii < newLen; ii++) {
			this.availablePool.add(constructNext(ii));
		}
	}


	private IReusableObjectInternal<I> constructNext(final int index)
	{
		return this.factory.apply(this.onReturnCallback, index);
	}
}

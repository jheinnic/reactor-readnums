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
import com.google.common.base.VerifyException;


/**
 * An implementation of {@link ReusableAllocator} that uses a reference-counting base class that returns itself to its
 * origin pool for recycling when its reference count drops to 0 after having been previously allocated.
 *
 * @author John Heinnickel
 */
public class ReusableObjectAllocator<I extends IReusable, T extends AbstractReusableObject<I>>
implements IReusableAllocator<I>
{
   private static final Logger LOG = LoggerFactory.getLogger(ReusableObjectAllocator.class);
   private static final int DEFAULT_SIZE = 2048;

   private final BitSet leaseMask;
   // private volatile boolean syncBit;

   private final boolean poolSizeFixed;
   private final ReentrantLock leaseLock;
   private final ArrayList<T> availablePool;
   private final IReusableObjectFactory<T> factory;


   public ReusableObjectAllocator( final IReusableObjectFactory<T> factory )
   {
      this(DEFAULT_SIZE, factory, false);
   }


   public ReusableObjectAllocator( final int initialSize, final IReusableObjectFactory<T> factory )
   {
      this(initialSize, factory, false);
   }


   public ReusableObjectAllocator( final int initialSize, final IReusableObjectFactory<T> factory,
      final boolean poolSizeFixed )
   {
      this.factory = factory;
      this.poolSizeFixed = poolSizeFixed;
      this.leaseMask = new BitSet(initialSize);
      this.leaseLock = new ReentrantLock();
      this.availablePool = new ArrayList<T>(initialSize);

      this.leaseLock.lock();
      try {
         this.expand(initialSize);
      } finally {
         this.leaseLock.unlock();
      }
   }


   @Override
   public I allocate()
   {
      T ref;
      int next;

      this.leaseLock.lock();
      try {
         // if(this.syncBit) { } else { }
         final int len = this.availablePool.size();
         next = this.leaseMask.nextClearBit(0);
         if (next >= len) {
            if (this.poolSizeFixed)
               throw new IllegalStateException("ReusableObjectAllocator's object supply is exhausted!");
            else {
               expand(len);
            }
         }
         this.leaseMask.set(next);
         ref = this.availablePool.get(next);

         // Mark the object as reserved in its own encapsulated state. The boolean value returned is just to
         // avoid JIT hotspot from compiling away the volatile sync involved. IF this method encounters a real
         // problem, it will throw an Exception, not return false.  Read the object's volatile sync bit before
         // returning it, just in case the recipient doesn't take the same precaution.
         ref.reserve();
         // this.syncBit = false;


         // We have completed reservation as far as the common data structures are concerned. We can initialize
         // the served object outside of the shared locks because it neither consumes nor produces shared state.
         return ref.castToInterface();
      } finally {
         this.leaseLock.unlock();
      }
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

      int next = 0;
      int last = 0;

      leaseLock.lock();
      try {
          // Make sure we've got current taste.
         // if(this.syncBit) { } else { }

         // First ensure there is adequate capacity. If capacity is fixed and the request cannot be fulfilled,
         // its better to fail fast rather than have to undo some number of reservations because we cannot
         // deliver the rest of the lot. And if we can handle dynamic expansion, checking for exhaustion potential
         // only once, before beginning, is more efficient than having to check after every reservation.
         final int len = availablePool.size();
         final int numFree = len - leaseMask.cardinality();
         if (numFree < batchSize) {
            if (this.poolSizeFixed)
               throw new IllegalStateException(
                  String.format(
                     "Cannot allocate a batch of size %d from a fixed size pool with only %d free",
                     Integer.valueOf(batchSize), Integer.valueOf(numFree)));
            else {
               expand(len + batchSize);
            }
         }

         // Now update lease bits and fill the return list by scanning progressively for runs of free objects
         // until enough have been culled.
         int found = 0;
         int needed = batchSize;
         final int cardinalityb4 = leaseMask.cardinality();
         final BitSet pending = new BitSet();
         while (needed > 0) {
            next = leaseMask.nextClearBit(last);
            last = leaseMask.nextSetBit(next);

            if ((next >= 0) && (last < 0)) {
               // A concrete lower bound and a non-existent upper bound means infinite zeroes in the undefined
               // direction. It looks like a non-existent negative range, but that's deceptive. I think the
               // will let us take advantage of it as long as we treat it as such, which means it doesn't matter
               // how much we've asked for, it can all be satisfied at once. Let's see if that works...
               last = next + needed;
               needed = 0;
               pending.set(next, last);
            } else {
               found = last - next;
               if (found < needed) {
                  needed -= found;
               } else {
                  last = next + needed;
                  needed = 0;
               }
               pending.set(next, last);
            }

            // Reserve each entry in the reservation state and verify its critical
            // details are correct.  Cross a LOAD based memory barrier specific to
            // the object at hand to be sure its loaded.
            availablePool.subList(next, last)
            .stream()
            .map(src -> {
               src.reserve();
               return src.castToInterface();
            })
            .collect( ()->container, ArrayList::add, ArrayList::addAll );
         }

         // We have a handle on an object set that satisfies the requirements. Now, commit the state changes,
         // release the lock, and do the per-object initialization that no longer touches shared state.
         this.leaseMask.or(pending);
         // this.syncBit = true;

         try {
            Verify.verify(
               ((leaseMask.cardinality() - cardinalityb4) == batchSize),
               "Cardinality mishmash %s - %s = %s",
               Integer.toString(leaseMask.cardinality()),
               Integer.toString(cardinalityb4),
               Integer.toString(batchSize));
            Verify.verify(
               pending.cardinality() == batchSize,
               "Pending mishmash %s == %s",
               Integer.toString(pending.cardinality()),
               Integer.toString(batchSize));
         } catch (final VerifyException e) {
            e.printStackTrace();

            // LOG.error("B4 Lease Mask: {}, {}", leaseB4, leaseCard);
            // LOG.error("B4 Pending Mask: {}, {}", pendingB4, pendingCard);

            // LOG.error("Goal: {}", batchSize);
            // LOG.error("After Lease Mask: {}", this.leaseMask);
            // LOG.error("After Pending Mask: {}", pending);
            // LOG.error("Container: {}", container);
         }
      } finally {
         leaseLock.unlock();
      }
   }


   protected void releaseBatch(final Iterable<T> batch)
   {
      if ((null != batch) && batch.iterator().hasNext()) {
         // Prepare a BitSet that can be applies to the lease BitSet as an "andNot" mask to clear
         // any bit set corresponding to an object in the batch we're processing for return.
         // Allocate a blank BitSet up front to take advantage of a small/sparse input domain potentially.
         // TODO: Also verify that every bit in the mask is previously set so we don't return anything not leased!
         final BitSet releaseMask = new BitSet();
         for (final T returned : batch) {
            releaseMask.set(returned.getPoolIndex());
         }

         this.leaseLock.lock();
         try {
            this.leaseMask.andNot(releaseMask);
         } finally {
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
      Verify.verify(
         this.leaseLock.isHeldByCurrentThread(),
         "It is only legal to call ReusableObjectAllocator#expand(int) while holding the leaseLock from within a try block that guarantees leaseLock#unlock) will get called on leaving that block.");

      final int len = this.availablePool.size();
      final int newLen = len + num;
      if (len > 0) {
         LOG.warn("ReusableObjectAllocator is about to expand available pool size to {}.  Sample pool member: {}", Integer.valueOf(newLen), availablePool.get(0));
      }

      this.availablePool.ensureCapacity(newLen);
      for (int i = len; i < newLen; i++) {
         this.availablePool.add(constructNext(i));
      }
   }


   private T constructNext(final int index)
   {
      // Each constructed object in the pool is injected with a unique release callback function that
      // embeds its allocator-assigned release index and also keeps the allocator's leaseLock and
      // leaseMask in scope.
      //
      // Constructed Reusable objects are responsible for implementing retain() and release() methods in
      // an atomic fashion so they can identify which operation makes the transition from a positive
      // reference count to zero and only call this method once from that transition.
      final OnReturnCallback onReturnCallback =
         (inception) -> {
            leaseLock.lock();
            try {
               Verify.verify(
                  leaseMask.get(index),
                  "Bit %s in lease mask is already false on return!",
                  Integer.valueOf(index));
               final T returnedObject = this.availablePool.get(index);
               Verify.verify(
                  returnedObject.getReferenceCount() == 0,
                  "Object pool entry at %s has a non-zero reference count!",
                  Integer.valueOf(index));
               Verify.verify(
                  returnedObject.getInception() == inception,
                  "Object pool entry at %s has inception %s, not %s",
                  Integer.toString(index),
                  Long.toString(
                     returnedObject.getInception()),
                  Long.toString(inception));

               // Engage a memory barrier and assure a clean sweep.
               // boolean readSync = this.syncBit;
               returnedObject.vacate();
               leaseMask.clear(index);
               // this.syncBit = false;
            } finally {
               leaseLock.unlock();
            }
         };

      return this.factory.apply(onReturnCallback, index);
   }
}

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

package info.jchein.apps.nr.codetest.ingest.messages;


import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;
import info.jchein.apps.nr.codetest.ingest.reusable.ReusableObjectAllocator;


/**
 * An subclass of {@link ReusableObjectAllocator} that is meant to be used by a single thread in a Reactor-driven
 * program to acquire File I/O sized ByteBuffers and populate with a consolidation of the per-partition
 * {@link RawBatchInput} messages produced during a single input gathering window.
 *
 * An input gathering window is bounded by either the maximum number of entries that can fit into the configured
 * File I/O buffer's byte size or the configured maximum collection inter-window latency interval.  Whichever of
 * either window closing events occurs first is the one that is acted upon--it is not necessary for both conditions
 * to be met, just one of them.
 *
 * To support concurrent output file writing (an optional feature), this singleton class has a file offset counter
 * that is read to set each allocated {@link WriteFileBuffer}
 *
 * @author John Heinnickel
 */
@Component
public final class V1WriteFileBufferAllocator
extends ReusableObjectAllocator<IWriteFileBuffer, WriteFileBuffer>
{
   private static final int DEFAULT_INITIAL_POOL_SIZE = 64;
   private static final boolean DEFAULT_IS_POOL_FIXED = false;

   // To configure these parameters of the singleton during any runtime, change these static variables and then
   // call #getInstance() __on the same thread__ and __before any other thread calls #getInstance()__.
   //
   // Once first call has been made to #getInstance(), changing these values has __no effect whatsoever__.
   public static int INITIAL_POOL_SIZE = DEFAULT_INITIAL_POOL_SIZE;
   public static boolean POOL_SIZE_FIXED = DEFAULT_IS_POOL_FIXED;
   public static int BUFFER_SIZE_IN_MESSAGES = Constants.DEFAULT_MAX_MESSAGES_PER_FLUSH;

	private final AtomicLong nextFileWriteOffset;

   public static V1WriteFileBufferAllocator getInstance() {
      return WriteFileBufferAllocatorHolder.INSTANCE;
   }


   private static final class WriteFileBufferAllocatorHolder {
		static final int BUFFER_SIZE_IN_MESSAGES = V1WriteFileBufferAllocator.BUFFER_SIZE_IN_MESSAGES;

		static final V1WriteFileBufferAllocator INSTANCE =
			new V1WriteFileBufferAllocator(INITIAL_POOL_SIZE, POOL_SIZE_FIXED, new AtomicLong(0));
   }


	// This would be private if private scope did not cause static inner classes to require a
	// synthetic access method. Favoring performance and exposing package scope instead.
	V1WriteFileBufferAllocator( final int initialPoolSize, final boolean poolSizeFixed,
		final AtomicLong nextFileWriteOffset )
   {
      super(
         initialPoolSize,
         (final OnReturnCallback onReturnCallback, final int pooledObjectIndex) ->
            new WriteFileBuffer(
               onReturnCallback,
               pooledObjectIndex,
				// V1WriteFileBufferAllocator.getInstance(),
				nextFileWriteOffset, V1WriteFileBufferAllocator.getBufferSizeInMessages()),
         poolSizeFixed);

		assert(nextFileWriteOffset != null);
		this.nextFileWriteOffset = nextFileWriteOffset;
   }


   private static int getBufferSizeInMessages()
   {
      return WriteFileBufferAllocatorHolder.BUFFER_SIZE_IN_MESSAGES;
   }


	long increaseNextFileWriteOffset(final int numBytes)
   {
		return nextFileWriteOffset.addAndGet(numBytes);
   }


   long getNextFileWriteOffset()
   {
		return nextFileWriteOffset.get();
   }
}

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
public final class WriteFileBufferAllocator
extends ReusableObjectAllocator<IWriteFileBuffer, WriteFileBuffer>
{
   private static final int DEFAULT_INITIAL_POOL_SIZE = 64;
   private static final boolean DEFAULT_POOL_SIZE_FIXED = false;


	// public static int BUFFER_SIZE_IN_MESSAGES = Constants.DEFAULT_MAX_MESSAGES_PER_FLUSH;

   public WriteFileBufferAllocator( final int bufferSizeInMessages )
   {
      this( DEFAULT_INITIAL_POOL_SIZE, DEFAULT_POOL_SIZE_FIXED, bufferSizeInMessages, new AtomicLong(0) );
   }

   public WriteFileBufferAllocator( final int initialPoolSize, final int bufferSizeInMessages )
   {
      this( initialPoolSize, DEFAULT_POOL_SIZE_FIXED, bufferSizeInMessages, new AtomicLong(0) );
   }

   public WriteFileBufferAllocator( final boolean poolSizeFixed, final int bufferSizeInMessages )
   {
      this( DEFAULT_INITIAL_POOL_SIZE, poolSizeFixed, bufferSizeInMessages, new AtomicLong(0) );
   }

   public WriteFileBufferAllocator( final int initialPoolSize, final boolean poolSizeFixed, final int bufferSizeInMessages )
   {
      this( initialPoolSize, poolSizeFixed, bufferSizeInMessages, new AtomicLong(0) );
   }

   public WriteFileBufferAllocator(
      final int initialPoolSize,
      final boolean poolSizeFixed,
      final int bufferSizeInMessages,
      final AtomicLong nextWriteFileOffset )
   {
      super(
         initialPoolSize,
         (final OnReturnCallback onReturnCallback, final int pooledObjectIndex) ->
            new WriteFileBuffer(
               onReturnCallback,
               pooledObjectIndex,
               nextWriteFileOffset,
               bufferSizeInMessages),
         poolSizeFixed);
   }
}

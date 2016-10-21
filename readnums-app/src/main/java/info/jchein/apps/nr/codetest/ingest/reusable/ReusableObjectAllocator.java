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


/**
 * An implementation of {@link ReusableAllocator} that uses a reference-counting base class that returns itself to its
 * origin pool for recycling when its reference count drops to 0 after having been previously allocated.
 *
 * @author John Heinnickel
 */
public class ReusableObjectAllocator<I extends IReusable, T extends AbstractReusableObject<I, ?>>
implements IReusableAllocator<I>
{
	// private static final Logger LOG = LoggerFactory.getLogger(ReusableObjectAllocator.class);

	private final IReusableObjectFactory<I> factory;
	private final OnReturnCallback onReturnCallback;

	@SuppressWarnings("unused")
	private volatile int nextLeasedIndex = 0;


	@SuppressWarnings("rawtypes")
	private static final AtomicIntegerFieldUpdater<ReusableObjectAllocator> ATOMIC_NEXT_INDEX_UPDATER =
		AtomicIntegerFieldUpdater.newUpdater(ReusableObjectAllocator.class, "nextLeasedIndex");


	public ReusableObjectAllocator( final int poolSize, final IReusableObjectFactory<I> factory )
	{
		this.factory = factory;
		this.onReturnCallback = (returnedObject) -> {
			return;
		};
	}


	@Override
	public I allocate()
	{
		final IReusableObjectInternal<I> retVal =
			this.factory.apply(onReturnCallback, ATOMIC_NEXT_INDEX_UPDATER.getAndIncrement(this));
		retVal.reserve();
		return retVal.castToInterface();
	}
}

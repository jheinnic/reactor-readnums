package info.jchein.apps.nr.codetest.ingest.messages;

import info.jchein.apps.nr.codetest.ingest.reusable.IReusable;


public interface ICounterIncrements
extends IReusable
{
	/**
	 * Sets and commits unique and duplicate counters for this object's current lease.
	 * 
	 * @param fromWriteBuf
	 *           Source for counter values
	 */
	public ICounterIncrements setDeltas(IWriteFileBuffer fromWriteBuf);


	/**
	 * Sets and commits unique and duplicate counters for this object's current lease.
	 * 
	 * @param deltaUniques
	 *           Source for unique counter value
	 * @param deltaDuplicates
	 *           Source for duplicate counter value
	 */
	public ICounterIncrements setDeltas(int deltaUniques, int deltaDuplicates);


	/**
	 * Increments, but does not commit, unique and duplicate counters.
	 * 
	 * @param deltaUniques
	 * @param deltaDuplicates
	 * @return
	 */
   public ICounterIncrements incrementDeltas(int deltaUniques, int deltaDuplicates);


	/**
	 * Increments, but does not commit, unique and duplicate counters.
	 * 
	 * @param absorbedCounters
	 *           Source for unique and duplicate counters. {@link #beforeRead()} is called on this parameter
	 *           to ensure values are visible before absorbed.
	 * @return
	 */
   public ICounterIncrements incrementDeltas(ICounterIncrements absorbedCounters);


	/**
	 * Returns unique counter for this object.
	 * 
	 * If this object was allocated by the same thread, the value returned reflects the most recent value set or the
	 * current incremental sum, regardless of whether or not any call has been made to {@link #beforeRead()}. If not, the
	 * returned value is only defined if it was assigned by previous call to either {@link #setDeltas(IWriteFileBuffer)}
	 * or {@link #setDeltas(int, int)} followed by a call on the current thread to {@link #beforeRead()}.
	 * 
	 * Unique counter values accumulated through calls to either {@link #incrementDeltas(int, int)} or
	 * {@link #incrementDeltas(ICounterIncrements)} are not presently guaranteed visible to any other threads.
	 * 
	 * @see ICounterIncrements#beforeRead();
	 * @return The currently visible unique values counter for this instance.
	 */
   public int getDeltaUniques();


	/**
	 * Returns duplicate counter for this object.
	 * 
	 * If this object was allocated by the same thread, the value returned reflects the most recent value set or the
	 * current incremental sum, regardless of whether or not any call has been made to {@link #beforeRead()}. If not, the
	 * returned value is only defined if it was assigned by previous call to either {@link #setDeltas(IWriteFileBuffer)}
	 * or {@link #setDeltas(int, int)} followed by a call on the current thread to {@link #beforeRead()}.
	 * 
	 * Unique counter values accumulated through calls to either {@link #incrementDeltas(int, int)} or
	 * {@link #incrementDeltas(ICounterIncrements)} are not presently guaranteed visible to any other threads.
	 * 
	 * @see ICounterIncrements#beforeRead();
	 * @return The currently visible duplicate values counter for this instance.
	 */
   public int getDeltaDuplicates();


	/**
	 * Half synchronization method required to ensure visibility of counter values committed by another thread by prior
	 * call to {@link #setDeltas(IWriteFileBuffer)} or {@link #setDeltas(int, int)}.
	 * 
	 * @return
	 */
	public ICounterIncrements beforeRead();


	public ICounterIncrements finalizeAggregate();
}

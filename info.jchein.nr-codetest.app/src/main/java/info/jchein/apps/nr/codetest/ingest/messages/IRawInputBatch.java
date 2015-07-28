package info.jchein.apps.nr.codetest.ingest.messages;

import java.nio.ByteBuffer;

import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusable;


public interface IRawInputBatch
extends IReusable
{
   public void acceptUniqueInput(int message);


   public void trackSkippedDuplicate();

   /**
    * Note that this does not override {@link AbstractReusableObject#afterWrite()}, but its implementation will and
    * must comply with both signatures. What that really means is that every AbstractReusableObject subclass must
    * also implement an interface that extends IReusable whenever it wants to expose the fluent API chaining feature
    * of {@link AbstractReusableObject#afterWrite()} and {@link AbstractReusableObject#beforeRead()} through an
    * implementation detail-hiding Interface, such as {@link IRawInputBatch} itself.
    *
    * @see #beforeRead()
    * @see AbstractReusableObject#afterWrite()
    * @see AbstractReusableObject#beforeRead()
    */
   @Override
   public IRawInputBatch afterWrite();


   /**
    * The first method a Thread consuming any content encapsulated by an implementation of this interface must call
    * before using other methods in order to ensure memory visibility effects are handled and also to trigger any
    * "on receipt" side effects defined by the implementation.
    *
    * Note that this does not override {@link AbstractReusableObject#beforeRead()}, but its implementation will and
    * must comply with both signatures. What that really means is that every AbstractReusableObject subclass must
    * also implement an interface that extends IReusable whenever it wants to expose the fluent API chaining feature
    * of {@link AbstractReusableObject#afterWrite()} and {@link AbstractReusableObject#beforeRead()} through an
    * implementation detail-hiding Interface, such as {@link IRawInputBatch} itself.
    *
    * @see #afterWrite()
    * @see AbstractReusableObject#afterWrite()
    * @see AbstractReusableObject#beforeRead()
    */
   @Override
   public IRawInputBatch beforeRead();


   /**
    * Transfers the content of implementing instance to parameter <code>buf</code>'s encapsulated {@link ByteBuffer}.
    *
    * It is extremely important to call {@link #beforeRead()} to ensure visibility and sanity checking have occurred
    * BEFORE calling this method!!
    *
    * @param buf {@link IWriteFileBuffer} whose {@link ByteBuffer} will be loaded with the transformed content of
    *             implementing instance's int[] batch contents.
    * @return True if contents of the implementing instance's int array were successfully converted to bytes and
    * loaded into <code>buf</code>'s {@link ByteBuffer}, or false if there was insufficient capacity left to
    * attempt requested transfer.
    *
    * @see #beforeRead()
    */
   public boolean transferToFileBuffer(IWriteFileBuffer buf);


   /**
    * Adds the content of accepted/skipped counters stored in an instance of this interface to corresponding counter
    * values encapsulated by argument <code>deltaCounters</code>
    *
    * It is extremely important to call {@link #beforeRead()} to ensure visibility and sanity checking have occurred
    * BEFORE calling this method!!
    *
    * @param deltaContainer
    *
    * @see #beforeRead()
    */
   public void loadCounterDeltas(ICounterIncrements deltaCounters);


   /**
    *
    */
   public int getEntryCount();

}

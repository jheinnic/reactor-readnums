package info.jchein.apps.nr.codetest.ingest.messages;

import info.jchein.apps.nr.codetest.ingest.reusable.AbstractReusableObject;
import info.jchein.apps.nr.codetest.ingest.reusable.OnReturnCallback;

public class CounterIncrements
extends AbstractReusableObject<ICounterIncrements, CounterIncrements>
implements ICounterIncrements
{
   public int deltaUniques;
   public int deltaDuplicates;


   public CounterIncrements(final OnReturnCallback onReleased, final int poolIndex) {
      super(onReleased, poolIndex);
      deltaUniques = 0;
      deltaDuplicates = 0;
   }


   @Override
   public final ICounterIncrements castToInterface() {
      return this;
   }


   @Override
	public final CounterIncrements setDeltas(final IWriteFileBuffer fromBuffer)
	{
		assert fromBuffer != null;
		fromBuffer.loadCounterDeltas(this);
		return this.afterWrite();
	}


	@Override
	public final CounterIncrements setDeltas(final int deltaUniques, final int deltaDuplicates)
	{
      this.deltaUniques = deltaUniques;
      this.deltaDuplicates = deltaDuplicates;
		return this.afterWrite();
   }


   @Override
   public final CounterIncrements incrementDeltas( final int deltaUniques, final int deltaDuplicates ) {
      this.deltaUniques += deltaUniques;
      this.deltaDuplicates += deltaDuplicates;

      return this;
   }


   @Override
   public CounterIncrements incrementDeltas(final ICounterIncrements absorbedCounters)
   {
		absorbedCounters.beforeRead();
      deltaUniques += absorbedCounters.getDeltaUniques();
      deltaDuplicates += absorbedCounters.getDeltaDuplicates();

      return this;
   }


   @Override
   public final int getDeltaUniques()
   {
      return deltaUniques;
   }


   @Override
   public final int getDeltaDuplicates()
   {
      return deltaDuplicates;
   }


   @Override
   public void recycle()
   {
      deltaUniques = 0;
      deltaDuplicates = 0;
   }


   @Override
   protected String innerToString()
   {
      return new StringBuilder()
      .append("IncrementCounters [deltaUniques=")
      .append(deltaUniques)
      .append(", deltaDuplicates=")
      .append(deltaDuplicates)
      .append("]")
      .toString();
   }


	@Override
	public CounterIncrements beforeRead()
	{
		return super.beforeRead();
	}
}

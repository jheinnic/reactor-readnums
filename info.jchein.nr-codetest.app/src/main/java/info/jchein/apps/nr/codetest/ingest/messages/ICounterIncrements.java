package info.jchein.apps.nr.codetest.ingest.messages;

import info.jchein.apps.nr.codetest.ingest.reusable.IReusable;


public interface ICounterIncrements
extends IReusable
{
   public ICounterIncrements setDeltas(int deltaUniques, int deltaDuplicates);


   public ICounterIncrements incrementDeltas(int deltaUniques, int deltaDuplicates);


   public ICounterIncrements incrementDeltas(ICounterIncrements absorbedCounters);


   public int getDeltaUniques();


   public int getDeltaDuplicates();
}

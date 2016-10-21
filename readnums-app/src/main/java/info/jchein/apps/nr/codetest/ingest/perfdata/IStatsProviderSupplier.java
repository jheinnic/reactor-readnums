package info.jchein.apps.nr.codetest.ingest.perfdata;


import reactor.fn.Supplier;


public interface IStatsProviderSupplier
extends Supplier<Iterable<IStatsProvider>>
{
	// This interface just names a particular type parameterization
	// of Supplier.
}

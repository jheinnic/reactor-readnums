package info.jchein.apps.nr.codetest.ingest.reusable;


@FunctionalInterface
public interface OnReturnCallback 
{
	public void accept(IReusableObjectInternal<?> returned);
}

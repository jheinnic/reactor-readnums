package info.jchein.apps.nr.codetest.ingest.segments.logunique;


public interface IUniqueMessageTrie
{
   boolean isUnique(int prefix, short suffix);
}

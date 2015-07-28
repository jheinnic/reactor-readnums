package info.jchein.apps.nr.codetest.ingest.segments.logunique;

public class CounterOverall
{
   private final long startupTime;
   private long lastUpdatedAt;
   private int totalUniques;
   
   public CounterOverall() {
      this.startupTime = System.nanoTime();
      this.lastUpdatedAt = this.startupTime;
      this.totalUniques = 0;
   }


   public long incrementUniqueValues(int deltaUniques)
   {
      final long previousTime = lastUpdatedAt;
      totalUniques += deltaUniques;
      lastUpdatedAt = System.nanoTime();
      return lastUpdatedAt - previousTime;
   }
   
   
   public final int getTotalUniques()
   {
      return totalUniques;
   }
   
   
   public long getTotalDuration() {
      return lastUpdatedAt - startupTime;
   }


   public final long getStartupTime()
   {
      return startupTime;
   }


   public final long getLastUpdatedAt()
   {
      return lastUpdatedAt;
   }
}

package info.jchein.apps.nr.codetest.ingest.perfdata;

/**
 * Interface implemented by any object that can be queried for statistics about a named RingBuffer.
 *
 * @author John
 */
public interface IStatsProvider
{
   /**
    * Returns name to identify the described RingBuffer.
    * 
    * @return Name to used when identifying described RingBuffer.
    */
   public String getName();
   
   /**
    * Returns how many buffer slots from the total capacity are presently unused.
    * 
    * NOTE: Yes, the RingBuffer interface exposes this as a long, but total capacity as an int.
    * 
    * @return Currently available RingBuffer slots if {@link #isAllive()} == true, otherwise not
    *          defined.
    */
   public long getFreeBufferSlots();

   /**
    * Returns how many buffer slots are allocated by described RingBuffer.
    * 
    * NOTE: Yes, the RingBuffer interface exposes this as an int, but available slots as a long.
    * 
    * @return Total allocated RingBuffer slots if {@link #isAllive()} == true, otherwise not
    *          defined.
    */
   public int getTotalBufferCapacity();

   /*
    * If true, then the RingBuffer is online and both {@link #getFreeBufferSlots()} and
    * {@link #getTotalBufferCapacity()} are defined.
    * 
    * @return True if the RingBuffer is online and buffer size methods have defined behavior.
   public boolean isAlive();
    */
}

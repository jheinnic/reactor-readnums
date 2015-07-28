package info.jchein.apps.nr.codetest.ingest.segments.logunique;

import java.util.BitSet;

public class UniqueMessageTrie implements IUniqueMessageTrie
{
   // private final byte[][][] partitionModTable;
   private final BitSet[] partitionedTrie;
   // private byte partitionCount;

   public UniqueMessageTrie( /*final byte partitionCount*/ ) {
      /*
      this.partitionCount = partitionCount;
      this.partitionModTable = new byte[10][10][10];

      byte modCounter = 0;
      byte[] nextValue = new byte[partitionCount];
      while(modCounter < partitionCount) {
         nextValue[modCounter] = ++modCounter;
      };
      nextValue[modCounter-1] = 0;
      modCounter = 0;
      
      for(int hundreds=0; hundreds < 10; hundreds++ ) {
         for (int tens=0; tens < 10; tens++) {
            for (int ones=0; ones<10; ones++) {
               partitionModTable[hundreds][tens][ones] = modCounter;
               modCounter = nextValue[modCounter];
            }
         }
      }
      */
      this.partitionedTrie = new BitSet[1000];
      for(int ii=0; ii<1000; ii++) {
         this.partitionedTrie[ii] = new BitSet(1000000);
      }
   }

   /*
   public byte fooMod(short key)
   {
      return (byte) (key % partitionCount);
   }

   public byte lookupPartitionMod( byte[] abc ) {
      return this.partitionModTable[abc[0]][abc[1]][abc[2]];
   }
   */

   @Override
   public boolean isUnique(final int prefix, final short suffix) {
      final BitSet msgBitSet = this.partitionedTrie[suffix];
      
      final boolean retVal;
      if (msgBitSet.get(prefix)) {
         retVal = false;
      } else {
         msgBitSet.set(prefix);
         retVal = true;
      }
      
      return retVal;
   }
}

package info.jchein.apps.nr.codetest.ingest.messages;

import info.jchein.apps.nr.codetest.ingest.reusable.IReusable;




public interface IInputMessage extends IReusable
{
   public static enum MessageKind
   {
      NINE_DIGITS,
      TERMINATE_CMD,
      INVALID_INPUT;
   }


   public void setMessagePayload(byte[] msgBytes, int prefix, short suffix, byte dataPartitionCount);


   public IInputMessage.MessageKind getKind();


   public byte[] getMessageBytes();


   public int getMessage();


   public int getPrefix();


   public short getSuffix();


   public byte getPartitionIndex();
}

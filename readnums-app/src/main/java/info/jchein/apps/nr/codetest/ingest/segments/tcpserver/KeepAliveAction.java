package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import reactor.fn.Supplier;
import reactor.rx.action.Action;

public class KeepAliveAction<T>
extends Action<T, T>
{
   KeepAliveAction() {
   }
   
   public static <T> Supplier<Action<T,T>> supply() {
		return () -> new KeepAliveAction<>();
   }

   @Override
   protected void doNext(T ev)
   {
      broadcastNext(ev);
   }
   
   @Override
   public void cancel()
   {
      // IGNORE
   }

}

package info.jchein.apps.nr.codetest.ingest.segments.tcpserver;

import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.rx.action.Action;

import com.google.common.base.Verify;

public class OnCancelAction<T>
extends Action<T, T>
{
   private Consumer<Void> onCancel;

   OnCancelAction(Consumer<Void> onCancel) {
      Verify.verifyNotNull(onCancel);
      this.onCancel = onCancel;
   }
   
   public static <T> Supplier<Action<T,T>> supply(Consumer<Void> onCancel) {
      return () -> new OnCancelAction<T>(onCancel);
   }

   @Override
   protected void doNext(T ev)
   {
      broadcastNext(ev);
   }
   
   @Override
   public void cancel()
   {
      this.onCancel.accept(null);
      super.cancel();
   }

}

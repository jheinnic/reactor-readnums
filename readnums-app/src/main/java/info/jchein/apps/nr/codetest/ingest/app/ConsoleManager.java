package info.jchein.apps.nr.codetest.ingest.app;

import javax.validation.constraints.NotNull;

import org.reactivestreams.Subscriber;
import org.springframework.beans.factory.annotation.Qualifier;

import info.jchein.apps.nr.codetest.ingest.app.console.IConsole;
import info.jchein.apps.nr.codetest.ingest.config.Constants;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;

public class ConsoleManager
implements Runnable
{
   @NotNull
   private final IConsole console;

   @NotNull
   private final Codec<Buffer, IInputMessage, IInputMessage> codec;

   @NotNull
	private final Subscriber<?> inputBroadcaster;

   @NotNull
   private final Consumer<Void> terminateAppConsumer;
   
   
   ConsoleManager(
      @NotNull IConsole console,
      @NotNull Codec<Buffer,IInputMessage,IInputMessage> codec,
		@NotNull Subscriber<?> inputBroadcaster,
      @NotNull @Qualifier(Constants.TERMINATE_INGEST_APPLICATION) Consumer<Void> terminateAppConsumer
   ) {
      this.console = console;
      this.codec = codec;
      this.inputBroadcaster = inputBroadcaster;
      this.terminateAppConsumer = terminateAppConsumer;
   }


   @Override
   public void run()
   {
      boolean quitting = false;
      while (quitting == false) {
         final String input = 
            console.readLine("Ingestion application is running.  To exit, type 'terminate' and then <ENTER>");
         final IInputMessage msg = 
            codec.decoder().apply(Buffer.wrap(input));

         switch (msg.getKind()) {
            case TERMINATE_CMD: {
               console.format("Ingestion application is shutting down.  Please wait...");
               terminateAppConsumer.accept(null);
               quitting = true;
               break;
            }
            case NINE_DIGITS: {
               console.format("Forwarding %s as though it had been read from an open socket\n", input);
					// TODO: REPAIR THIS
					// inputBroadcaster.onNext(msg);
               quitting = false;
            }
            case INVALID_INPUT: {
               console.format("Input (%s) not recognized.  Please try again.\n");
               quitting = false;
            }
         }
      }
   }
}

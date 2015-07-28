package info.jchein.apps.nr.codetest.ingest.app;

import info.jchein.apps.nr.codetest.ingest.config.IngestConfiguration;
import info.jchein.apps.nr.codetest.ingest.config.ParametersConfiguration;
import info.jchein.apps.nr.codetest.ingest.messages.EventConfiguration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

public class SpringMain
implements CommandLineRunner
{
   // private static final Logger LOG = LoggerFactory.getLogger(SpringMain.class);

   
   public static void main(String[] args) throws InterruptedException {
      SpringApplication app =
         new SpringApplicationBuilder(
            SpringMain.class, IngestConfiguration.class, EventConfiguration.class, ParametersConfiguration.class)
         .headless(true)
         .main(SpringMain.class).application();

      app.run(args);
   }
      
   @Autowired
   ApplicationWatchdog applicationWatchdog;
   
   @Autowired
   ConfigurableApplicationContext applicationContext;
   
      // CTXT.registerShutdownHook();
      // Thread.sleep(2000);
      // CTXT.stop();

   @Override
   public void run(String... args) throws Exception {
      applicationContext.registerShutdownHook();
      applicationContext.start();
      applicationWatchdog.blockUntilTerminatedOrInterrupted();
   }

}
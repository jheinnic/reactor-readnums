package info.jchein.apps.nr.codetest.ingest.config;

public final class Constants
{
   // public static final long DEFAULT_SHUTDOWN_TOLERANCE_IN_SECONDS = 90;
   // public static final String DEFAULT_SOCKET_DISPATCHER_FACTORY_NAME = "socketDispatcherGroup";
   public static final int MAX_CONCURRENT_SOCKETS = 512;
   public static final int DEFAULT_MAX_MESSAGES_PER_FLUSH = 6000;

   public static final String TERMINATE_MSG = "terminate";

   public static final int VALID_MESSAGE_SIZE = 9;
   public static final int WIN_ALT_VALID_MESSAGE_SIZE = 10;
   public static final int UNIQUE_POSSIBLE_MESSAGE_COUNT = 1000000000;

   public static final byte[] DELIMITER_BYTES = System.lineSeparator().getBytes();
   public static final int DELIMITER_SIZE = DELIMITER_BYTES.length;
   public static final int FILE_ENTRY_SIZE = VALID_MESSAGE_SIZE + DELIMITER_SIZE;

//   public static final int STARTUP_TOLERANCE_IN_SECONDS = 60;
//   public static final long ENV_SHUTDOWN_TOLERANCE_IN_SECONDS = 90;
//   public static final String PROPERTIES_RESOURCE_PATH = "META-INF/info.jchein/unique-numbers.properties";

   // public static final String EVENT_BUS_DISPATCHER_NAME = "eventBusDispatcher";

   public static final String TERMINATE_INGEST_APPLICATION = "terminateIngestApplication";
}

package info.jchein.apps.nr.codetest.ingest.app.console;

public final class ConsoleFactory
{
    private ConsoleFactory() {}
    
    public static final IConsole getConsole()
    {
       return ConsoleFactoryHolder.INSTANCE;
    }
    
    static final class ConsoleFactoryHolder
    {
       static final IConsole INSTANCE;
       
       static {
           java.io.Console systemConsole = System.console();
           if (systemConsole != null) {
              INSTANCE = new SystemConsoleWrapper(systemConsole);
           } else {
              INSTANCE = ImitationConsole.getConsole();
           }
       }
    }
}

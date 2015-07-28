package info.jchein.apps.nr.codetest.ingest.app.console;

import java.io.Console;
import java.io.PrintWriter;
import java.io.Reader;

public class SystemConsoleWrapper
implements IConsole
{
   private final Console systemConsole;


   public SystemConsoleWrapper( Console systemConsole )
   {
      this.systemConsole = systemConsole;
   }


   @Override
   public PrintWriter writer()
   {
      return systemConsole.writer();
   }


   @Override
   public Reader reader()
   {
      return systemConsole.reader();
   }


   @Override
   public IConsole format(String fmt, Object... args)
   {
      systemConsole.format(fmt, args);
      return this;
   }


   @Override
   public IConsole printf(String format, Object... args)
   {
      systemConsole.printf(format, args);
      return this;
   }


   @Override
   public String readLine(String fmt, Object... args)
   {
      return systemConsole.readLine(fmt, args);
   }


   @Override
   public String readLine()
   {
      return systemConsole.readLine();
   }


   @Override
   public char[] readPassword(String fmt, Object... args)
   {
      return systemConsole.readPassword(fmt, args);
   }


   @Override
   public char[] readPassword()
   {
      return systemConsole.readPassword();
   }


   @Override
   public void flush()
   {
      systemConsole.flush();
   }
}

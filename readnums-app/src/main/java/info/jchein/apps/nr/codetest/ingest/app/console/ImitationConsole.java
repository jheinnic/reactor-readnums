package info.jchein.apps.nr.codetest.ingest.app.console;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Formatter;
import java.util.IllegalFormatException;

import sun.nio.cs.StreamDecoder;
import sun.nio.cs.StreamEncoder;

@SuppressWarnings("restriction")
class ImitationConsole implements IConsole
{
   static IConsole getConsole()
   {
      return new ImitationConsole();
   }

   
	/**
	 * Retrieves the unique {@link java.io.PrintWriter PrintWriter} object associated with this console.
	 *
	 * @return The print writer associated with this console
	 */
    @Override
	public PrintWriter writer() {
        return pw;
    }

   /**
    * Retrieves the unique {@link java.io.Reader Reader} object associated
    * with this console.
    * <p>
    * This method is intended to be used by sophisticated applications, for
    * example, a {@link java.util.Scanner} object which utilizes the rich
    * parsing/scanning functionality provided by the <tt>Scanner</tt>:
    * <blockquote><pre>
    * Console con = System.console();
    * if (con != null) {
    *     Scanner sc = new Scanner(con.reader());
    *     ...
    * }
    * </pre></blockquote>
    * <p>
    * For simple applications requiring only line-oriented reading, use
    * <tt>{@link #readLine}</tt>.
    * <p>
    * The bulk read operations {@link java.io.Reader#read(char[]) read(char[]) },
    * {@link java.io.Reader#read(char[], int, int) read(char[], int, int) } and
    * {@link java.io.Reader#read(java.nio.CharBuffer) read(java.nio.CharBuffer)}
    * on the returned object will not read in characters beyond the line
    * bound for each invocation, even if the destination buffer has space for
    * more characters. The {@code Reader}'s {@code read} methods may block if a
    * line bound has not been entered or reached on the console's input device.
    * A line bound is considered to be any one of a line feed (<tt>'\n'</tt>),
    * a carriage return (<tt>'\r'</tt>), a carriage return followed immediately
    * by a linefeed, or an end of stream.
    *
    * @return  The reader associated with this console
    */
    @Override
	public Reader reader() {
        return reader;
    }

   /**
    * Writes a formatted string to this console's output stream using
    * the specified format string and arguments.
    *
    * @param  fmt
    *         A format string as described in <a
    *         href="../util/Formatter.html#syntax">Format string syntax</a>
    *
    * @param  args
    *         Arguments referenced by the format specifiers in the format
    *         string.  If there are more arguments than format specifiers, the
    *         extra arguments are ignored.  The number of arguments is
    *         variable and may be zero.  The maximum number of arguments is
    *         limited by the maximum dimension of a Java array as defined by
    *         <cite>The Java&trade; Virtual Machine Specification</cite>.
    *         The behaviour on a
    *         <tt>null</tt> argument depends on the <a
    *         href="../util/Formatter.html#syntax">conversion</a>.
    *
    * @throws  IllegalFormatException
    *          If a format string contains an illegal syntax, a format
    *          specifier that is incompatible with the given arguments,
    *          insufficient arguments given the format string, or other
    *          illegal conditions.  For specification of all possible
    *          formatting errors, see the <a
    *          href="../util/Formatter.html#detail">Details</a> section
    *          of the formatter class specification.
    *
    * @return  This console
    */
    @Override
	public ImitationConsole format(String fmt, Object ...args) {
        formatter.format(fmt, args).flush();
        return this;
    }

   /**
    * A convenience method to write a formatted string to this console's
    * output stream using the specified format string and arguments.
    *
    * <p> An invocation of this method of the form <tt>con.printf(format,
    * args)</tt> behaves in exactly the same way as the invocation of
    * <pre>con.format(format, args)</pre>.
    *
    * @param  format
    *         A format string as described in <a
    *         href="../util/Formatter.html#syntax">Format string syntax</a>.
    *
    * @param  args
    *         Arguments referenced by the format specifiers in the format
    *         string.  If there are more arguments than format specifiers, the
    *         extra arguments are ignored.  The number of arguments is
    *         variable and may be zero.  The maximum number of arguments is
    *         limited by the maximum dimension of a Java array as defined by
    *         <cite>The Java&trade; Virtual Machine Specification</cite>.
    *         The behaviour on a
    *         <tt>null</tt> argument depends on the <a
    *         href="../util/Formatter.html#syntax">conversion</a>.
    *
    * @throws  IllegalFormatException
    *          If a format string contains an illegal syntax, a format
    *          specifier that is incompatible with the given arguments,
    *          insufficient arguments given the format string, or other
    *          illegal conditions.  For specification of all possible
    *          formatting errors, see the <a
    *          href="../util/Formatter.html#detail">Details</a> section of the
    *          formatter class specification.
    *
    * @return  This console
    */
    @Override
	public ImitationConsole printf(String format, Object ... args) {
        return format(format, args);
    }

   /**
    * Provides a formatted prompt, then reads a single line of text from the
    * console.
    *
    * @param  fmt
    *         A format string as described in <a
    *         href="../util/Formatter.html#syntax">Format string syntax</a>.
    *
    * @param  args
    *         Arguments referenced by the format specifiers in the format
    *         string.  If there are more arguments than format specifiers, the
    *         extra arguments are ignored.  The maximum number of arguments is
    *         limited by the maximum dimension of a Java array as defined by
    *         <cite>The Java&trade; Virtual Machine Specification</cite>.
    *
    * @throws  IllegalFormatException
    *          If a format string contains an illegal syntax, a format
    *          specifier that is incompatible with the given arguments,
    *          insufficient arguments given the format string, or other
    *          illegal conditions.  For specification of all possible
    *          formatting errors, see the <a
    *          href="../util/Formatter.html#detail">Details</a> section
    *          of the formatter class specification.
    *
    * @throws IOError
    *         If an I/O error occurs.
    *
    * @return  A string containing the line read from the console, not
    *          including any line-termination characters, or <tt>null</tt>
    *          if an end of stream has been reached.
    */
    @Override
	public String readLine(String fmt, Object ... args) {
        String line = null;
        synchronized (writeLock) {
            synchronized(readLock) {
                if (fmt.length() != 0)
                    pw.format(fmt, args);
                try {
                    char[] ca = readline(false);
                    if (ca != null)
                        line = new String(ca);
                } catch (IOException x) {
                    throw new IOError(x);
                }
            }
        }
        return line;
    }

   /**
    * Reads a single line of text from the console.
    *
    * @throws IOError
    *         If an I/O error occurs.
    *
    * @return  A string containing the line read from the console, not
    *          including any line-termination characters, or <tt>null</tt>
    *          if an end of stream has been reached.
    */
    @Override
	public String readLine() {
        return readLine("");
    }

   /**
    * Provides a formatted prompt, then reads a password or passphrase from
    * the console with echoing disabled.
    *
    * @param  fmt
    *         A format string as described in <a
    *         href="../util/Formatter.html#syntax">Format string syntax</a>
    *         for the prompt text.
    *
    * @param  args
    *         Arguments referenced by the format specifiers in the format
    *         string.  If there are more arguments than format specifiers, the
    *         extra arguments are ignored.  The maximum number of arguments is
    *         limited by the maximum dimension of a Java array as defined by
    *         <cite>The Java&trade; Virtual Machine Specification</cite>.
    *
    * @throws  IllegalFormatException
    *          If a format string contains an illegal syntax, a format
    *          specifier that is incompatible with the given arguments,
    *          insufficient arguments given the format string, or other
    *          illegal conditions.  For specification of all possible
    *          formatting errors, see the <a
    *          href="../util/Formatter.html#detail">Details</a>
    *          section of the formatter class specification.
    *
    * @throws IOError
    *         If an I/O error occurs.
    *
    * @return  A character array containing the password or passphrase read
    *          from the console, not including any line-termination characters,
    *          or <tt>null</tt> if an end of stream has been reached.
    */
    @Override
	public char[] readPassword(String fmt, Object ... args) {
        char[] passwd = null;
        synchronized (writeLock) {
            synchronized(readLock) {
//                 try {
//                     echoOff = echo(false);
//                 } catch (IOException x) {
//                     throw new IOError(x);
//                 }
                IOError ioe = null;
                try {
                    if (fmt.length() != 0)
                        pw.format(fmt, args);
                    passwd = readline(true);
                } catch (IOException x) {
                    ioe = new IOError(x);
                } finally {
//                    try {
//                        echoOff = echo(true);
//                    } catch (IOException x) {
//                        if (ioe == null)
//                            ioe = new IOError(x);
//                        else
//                            ioe.addSuppressed(x);
//                    }
                    if (ioe != null)
                        throw ioe;
                }
                pw.println();
            }
        }
        return passwd;
    }

   /**
    * Reads a password or passphrase from the console with echoing disabled
    *
    * @throws IOError
    *         If an I/O error occurs.
    *
    * @return  A character array containing the password or passphrase read
    *          from the console, not including any line-termination characters,
    *          or <tt>null</tt> if an end of stream has been reached.
    */
    @Override
	public char[] readPassword() {
        return readPassword("");
    }

    /**
     * Flushes the console and forces any buffered output to be written
     * immediately .
     */
    @Override
	public void flush() {
        pw.flush();
    }


	Object readLock;
	Object writeLock;
    private Reader reader;
    private Writer out;
    private PrintWriter pw;
    private Formatter formatter;
    private Charset cs;
	char[] rcb;
    // private static native String encoding();
    // private static native boolean echo(boolean on) throws IOException;
    // private static boolean echoOff;

    private char[] readline(boolean zeroOut) throws IOException {
        int len = reader.read(rcb, 0, rcb.length);
        if (len < 0)
            return null;  //EOL
        if (rcb[len-1] == '\r')
            len--;        //remove CR at end;
        else if (rcb[len-1] == '\n') {
            len--;        //remove LF at end;
            if (len > 0 && rcb[len-1] == '\r')
                len--;    //remove the CR, if there is one
        }
        char[] b = new char[len];
        if (len > 0) {
            System.arraycopy(rcb, 0, b, 0, len);
            if (zeroOut) {
                Arrays.fill(rcb, 0, len, ' ');
            }
        }
        return b;
    }

	char[] grow()
	{
        assert Thread.holdsLock(readLock);
        char[] t = new char[rcb.length * 2];
        System.arraycopy(rcb, 0, t, 0, rcb.length);
        rcb = t;
        return rcb;
    }

    class LineReader extends Reader {
        private Reader in;
        private char[] cb;
        private int nChars, nextChar;
        boolean leftoverLF;
        LineReader(Reader in) {
            this.in = in;
            cb = new char[1024];
            nextChar = nChars = 0;
            leftoverLF = false;
        }


		@Override
		public void close()
		{
			// No Operation
		}


		@Override
		public boolean ready() throws IOException
		{
			// in.ready synchronizes on readLock already
			return in.ready();
		}


		@Override
		public int read(char cbuf[], int offset, int length) throws IOException
		{
            int off = offset;
            int end = offset + length;
            if (offset < 0 || offset > cbuf.length || length < 0 ||
                end < 0 || end > cbuf.length) {
                throw new IndexOutOfBoundsException();
            }
            synchronized(readLock) {
                boolean eof = false;
                char c = 0;
                for (;;) {
                    if (nextChar >= nChars) {   //fill
                        int n = 0;
                        do {
                            n = in.read(cb, 0, cb.length);
                        } while (n == 0);
                        if (n > 0) {
                            nChars = n;
                            nextChar = 0;
                            if (n < cb.length &&
                                cb[n-1] != '\n' && cb[n-1] != '\r') {
                                /*
                                 * we're in canonical mode so each "fill" should
                                 * come back with an eol. if there no lf or nl at
                                 * the end of returned bytes we reached an eof.
                                 */
                                eof = true;
                            }
                        } else { /*EOF*/
                            if (off - offset == 0)
                                return -1;
                            return off - offset;
                        }
                    }
                    if (leftoverLF && cbuf == rcb && cb[nextChar] == '\n') {
                        /*
                         * if invoked by our readline, skip the leftover, otherwise
                         * return the LF.
                         */
                        nextChar++;
                    }
                    leftoverLF = false;
                    while (nextChar < nChars) {
                        c = cbuf[off++] = cb[nextChar];
                        cb[nextChar++] = 0;
                        if (c == '\n') {
                            return off - offset;
                        } else if (c == '\r') {
                            if (off == end) {
                                /* no space left even the next is LF, so return
                                 * whatever we have if the invoker is not our
                                 * readLine()
                                 */
                                if (cbuf == rcb) {
                                    cbuf = grow();
                                    end = cbuf.length;
                                } else {
                                    leftoverLF = true;
                                    return off - offset;
                                }
                            }
                            if (nextChar == nChars && in.ready()) {
                                /*
                                 * we have a CR and we reached the end of
                                 * the read in buffer, fill to make sure we
                                 * don't miss a LF, if there is one, it's possible
                                 * that it got cut off during last round reading
                                 * simply because the read in buffer was full.
                                 */
                                nChars = in.read(cb, 0, cb.length);
                                nextChar = 0;
                            }
                            if (nextChar < nChars && cb[nextChar] == '\n') {
                                cbuf[off++] = '\n';
                                nextChar++;
                            }
                            return off - offset;
                        } else if (off == end) {
                           if (cbuf == rcb) {
                                cbuf = grow();
                                end = cbuf.length;
                           } else {
                               return off - offset;
                           }
                        }
                    }
                    if (eof)
                        return off - offset;
                }
            }
        }
    }


	ImitationConsole()
	{
		readLock = new Object();
		writeLock = new Object();
		// String csname = encoding();
		// if (csname != null) {
		// try {
		// cs = Charset.forName(csname);
		// } catch (Exception x) {}
		// }
		// if (cs == null)
		cs = Charset.defaultCharset();
		out =
			StreamEncoder.forOutputStreamWriter(new FileOutputStream(FileDescriptor.out), writeLock, cs);
		pw = new PrintWriter(out, true) {
			@Override
			public void close()
			{
				// No Operation
			}
		};
		formatter = new Formatter(out);
		reader =
			new LineReader(
				StreamDecoder.forInputStreamReader(new FileInputStream(FileDescriptor.in), readLock, cs));
		rcb = new char[1024];
	}
}

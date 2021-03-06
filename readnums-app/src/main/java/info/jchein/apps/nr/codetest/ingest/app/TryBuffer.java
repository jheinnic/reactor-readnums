package info.jchein.apps.nr.codetest.ingest.app;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.jchein.apps.nr.codetest.ingest.messages.CounterIncrements;
import info.jchein.apps.nr.codetest.ingest.messages.ICounterIncrements;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.messages.MessageInput;
import info.jchein.apps.nr.codetest.ingest.messages.MessageInput;
import info.jchein.apps.nr.codetest.ingest.messages.WriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
import info.jchein.apps.nr.codetest.ingest.reusable.ReusableObjectAllocator;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.FailedWriteException;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.IUniqueMessageTrie;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.UniqueMessageTrie;
import info.jchein.apps.nr.codetest.ingest.segments.logunique.WriteFailureType;
import info.jchein.apps.nr.codetest.ingest.segments.tcpserver.InputMessageCodec;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import reactor.Environment;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.timer.HashWheelTimer;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.DelimitedCodec;
import reactor.io.net.ChannelStream;
import reactor.io.net.NetStreams;
import reactor.io.net.impl.netty.NettyChannelHandlerBridge;
import reactor.io.net.impl.netty.tcp.NettyTcpServer;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.PushSubscription;


public class TryBuffer
{
   private static final Logger LOG = LoggerFactory.getLogger(TryBuffer.class);


   public static void main(final String[] args) throws InterruptedException
   {
      final byte numDataPartitions = (byte) 4;

      final Environment reactorEnv =
         Environment.initializeIfEmpty()
         .assignErrorJournal(err -> {
            LOG.error("Envrionment was asked to route unhandled exception: ", err);
            err.printStackTrace();
         });

		final ReusableObjectAllocator<IWriteFileBuffer, WriteFileBuffer> writeBufAlloc =
			new ReusableObjectAllocator<>(4096, null);

		final ReusableObjectAllocator<ICounterIncrements, CounterIncrements> counterAlloc =
			new ReusableObjectAllocator<>(2048, null);

		final Codec<Buffer, MessageInput, Object> msgCodec =
			new DelimitedCodec<>(
				true, new InputMessageCodec(numDataPartitions));

		final TcpServer<MessageInput, Object> tcpServer =
			NetStreams.<MessageInput, Object> tcpServer(
            NettyTcpServer.class,
            aSpec -> {
               return aSpec.env(
                  Environment.initializeIfEmpty())
               .codec(msgCodec)
               .dispatcher(Environment.newDispatcher("serverDispatcher", 8192, 1))
               .listen("127.0.0.1", 5000);
            }
         );

      final Broadcaster<Stream<MessageInput>> streamsToMerge =
         Broadcaster.create(reactorEnv, SynchronousDispatcher.INSTANCE);

      final IUniqueMessageTrie uniqueTrie = new UniqueMessageTrie();
      final File fileObject = new File("tryBuffer.dat");
      final FileChannel fileChannel = openFile(fileObject);

      final Timer workTimer = new HashWheelTimer(
         "work-timer",
         20,
         512,
         new HashWheelTimer.SleepWait(),
         Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("work-timer-run")));

      @SuppressWarnings("rawtypes")
      final Processor[] fanOutProcessors = new Processor[4];
      for (int ii = 0; ii < 4; ii++) {
         fanOutProcessors[ii] = RingBufferWorkProcessor.share("fanOutProcessor-" + ii, 4096, true);
      }

      final Processor<IWriteFileBuffer, IWriteFileBuffer> writeOutputProcessor =
         RingBufferWorkProcessor.share("writeFileWorkProcessor", 4096, true);

      final Timer counterTimer = new HashWheelTimer(
         "counter-timer",
         250,
         512,
         new HashWheelTimer.SleepWait(),
         Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("counter-timer-run")));
      final Processor<ICounterIncrements, ICounterIncrements> counterIncrementsProcessor =
         RingBufferWorkProcessor.share("counterIncrementsProcessor", 4096, true);

      final long[] lastUpdateNanos = new long[] { System.nanoTime() };



      @SuppressWarnings("unchecked")
      final Stream<IWriteFileBuffer> computeStream =
         streamsToMerge.startWith(
            Streams.<MessageInput>never())
         .<MessageInput> merge()
         .groupBy(evt -> Byte.valueOf(evt.getPartitionIndex()))
//         .retry( t -> {
//            LOG.error("Retrying from channel handler stream after ", t);
//            return true;
//         })
         .<IWriteFileBuffer> flatMap(partitionStream -> {
            final byte partitionIndex = partitionStream.key().byteValue();

            return partitionStream.process((Processor<MessageInput, MessageInput>) fanOutProcessors[partitionIndex])
	            .window(3072, 6000, TimeUnit.MILLISECONDS, workTimer)
	            .<IWriteFileBuffer> flatMap( nestedWindow -> {
						return nestedWindow.reduce(
	                  writeBufAlloc.allocate(),
	                  (final IWriteFileBuffer batch, final MessageInput msg) -> {
							if (uniqueTrie.isUnique(
								InputMessageCodec.parsePrefix(msg.getMessageBytes()), msg.getSuffix()))
					{
	                        batch.acceptUniqueInput(msg.getMessageBytes());
	                     } else {
									// Duplicate tracking really requires an intermediate stage to
									// accumulate counters.
									batch.trackSkippedDuplicates(1);
	                     }
	
	                     return batch;
	                  }
	               ).map(writeBuf -> writeBuf.afterWrite());
//         .retry( t -> {
//            LOG.error("Retrying from compute stream after ", t);
//            return true;
//         });
	            });
				});

         final Stream<ICounterIncrements> writeResultStream =
            computeStream.process(writeOutputProcessor)
            .map(writeBuf -> {
				// writeBuf.beforeRead();
               final ICounterIncrements countIncr =
                  processBatch(writeBuf, fileChannel, fileObject, counterAlloc);

				// countIncr.afterWrite();
               return countIncr;
            })
            .retry( t -> {
               LOG.error("Retrying from write output processor stream after ", t);
               return true;
            });

         	// TODO: This is outdated logic.  See production impl for a better code path.
            writeResultStream.process(counterIncrementsProcessor)
	            .mergeWith(Streams.period(counterTimer, 1)
	               .map(evt -> counterAlloc.allocate()
	                  .setDeltas(0, 0)))
	            .window(5, TimeUnit.SECONDS, counterTimer)
	            .flatMap(statStream -> {
	               return statStream.reduce(
	                  counterAlloc.allocate(),
	                  (final ICounterIncrements prevStat, final ICounterIncrements nextStat) -> {
	                     nextStat.beforeRead();
	                     nextStat.incrementDeltas(prevStat);
	                     prevStat.release();
	                     return nextStat;
	               });
	            })
	            .retry( t -> {
	               LOG.error("Retrying from counter incrementing processor stream after ", t);
	               return true;
	            })
	            .consume(evt -> {
	               final int deltaUniques = evt.getDeltaUniques();
	               final int deltaDuplicates = evt.getDeltaDuplicates();
	               evt.release();
	
	               final long currentNanos = System.nanoTime();
	               LOG.info(
	                  "Over the past {} seconds, we accepted {} records and rejected {} duplicates.",
	                  new Object[] {
	                     Long.valueOf(
	                        TimeUnit.NANOSECONDS.toSeconds(currentNanos - lastUpdateNanos[0])),
	                     Integer.valueOf(deltaUniques), Integer.valueOf(deltaDuplicates) }
	               );
	               lastUpdateNanos[0] = currentNanos;
	            });

      tcpServer.start(channelStream -> {
         LOG.info("In connection handler");

			final ChannelStream<MessageInput, Object> theChannelStream = channelStream;

         streamsToMerge.onNext(
            theChannelStream.filter( evt -> {
               switch (evt.getKind()) {
                  case NINE_DIGITS: {
                     return true;
                  }
                  case TERMINATE_CMD: {
                     // TODO!!
                     closeChannel(channelStream);
                     break;
                  }
                  case INVALID_INPUT: {
                     closeChannel(channelStream);
                  }
               }

               return false;
            })
         );

         LOG.info("Addressing write stream and returning...");
         final Stream<MessageInput> noData = Streams.empty();
         noData.consume();
         return channelStream.writeWith(noData);
      });


      while (true) {
         synchronized (tcpServer) {
            tcpServer.wait();
         }
      }
   }

   private static void closeChannel(final ChannelStream<?, ?> channelStream)
   {
      final ChannelPipeline pipeline = ((Channel) channelStream.delegate()).pipeline();
      final NettyChannelHandlerBridge<?, ?> nettyHandlerBridge =
         pipeline.get(NettyChannelHandlerBridge.class);
      if (nettyHandlerBridge == null) {
         LOG.warn(
            "There is no NettyChannelHandlerBridge installed on this Channel.  It looks like Reactor has already abandoned it.  Nothing more to do--if it hasn't closed yet it is proably in the act of being closed.");
         return;
      }
      final PushSubscription<?> subscription = nettyHandlerBridge.subscription();

      if (LOG.isInfoEnabled()) {
         if (subscription != null) {
            LOG.error(
               "Ending NettyChannelHandlerBridge's downstream subscription before closing Netty Channel to avoid potential race conditions");
            subscription.terminate();
         } else {
            LOG
            .error("Reactor has no downstream subscription to terminate before closing Netty Channel.");
         }
      } else if (subscription != null) {
         subscription.terminate();
      }
      pipeline.disconnect();
   };


   @SuppressWarnings("resource")
   static FileChannel openFile(final File outputLogFile)
   {
      final FileOutputStream outputFileStream;
      try {
         outputFileStream = new FileOutputStream(outputLogFile, false);
      } catch (final FileNotFoundException e) {
         if (outputLogFile.exists())
            throw new FailedWriteException(outputLogFile, WriteFailureType.EXISTS_NOT_WRITABLE, e);
         else
            throw new FailedWriteException(outputLogFile, WriteFailureType.CANNOT_CREATE, e);
      }

      LOG.info("Output file stream to {} open for writing", outputLogFile);
      return outputFileStream.getChannel();
   }


   /**
    * Flush a buffer containing some fixed size entries to the output log, with a native line separator between each
    * entry.
    *
    * It is the caller's responsibility to ensure that the ByteBuffer received has been flipped or is otherwise
    * configured such that its position points at the first byte to write and its limit points at the last.
    *
    * @param accepted
    */
   static ICounterIncrements processBatch(final IWriteFileBuffer batchDef,
      final FileChannel outputChannel, final File outputLogFile,
      final IReusableAllocator<ICounterIncrements> counterIncrementsAllocator)
   {
      final ByteBuffer buf = batchDef.getByteBufferToFlush();
      long writeOffset = batchDef.getFileWriteOffset();

      try {
         int passCount = 0;
         while (buf.hasRemaining()) {
            final int bytesWritten = outputChannel.write(buf, writeOffset);

            if (bytesWritten < 0)
               throw new FailedWriteException(
                  outputLogFile,
                  WriteFailureType.WRITE_RETURNS_NEGATIVE,
                  writeOffset,
                  buf.position(),
                  buf.remaining());
            else {
               writeOffset += bytesWritten;
            }

            passCount++;
         }

         if ((passCount > 1) && LOG.isInfoEnabled()) {
            LOG.info(
               String.format(
                  "Wrote %d bytes in %d passes, leaving %s",
                  Integer.toString(buf.position()),
                  Integer.toString(passCount),
                  buf.toString()));
         }

         if (LOG.isDebugEnabled()) {
            LOG.debug("Drained buffer to output file and recycled WriteFileBuffer message.", buf);
         }

         return batchDef.loadCounterDeltas(
            counterIncrementsAllocator.allocate());
      } catch (final IOException e) {
         if (LOG.isWarnEnabled()) {
            LOG.warn(
               String.format(
                  "Failed to drain buffer to output file %s at offset %d.  Event still recyled.",
                  outputLogFile,
                  Long.toString(writeOffset)),
               e);
         }

         try {
            outputChannel.close();
         } catch (final IOException e1) {}

         throw new FailedWriteException(
            outputLogFile,
            WriteFailureType.IO_EXCEPTION_ON_WRITE,
            writeOffset,
            buf.position(),
            buf.remaining(),
            e);
      } finally {
         batchDef.release();
      }
   }
}

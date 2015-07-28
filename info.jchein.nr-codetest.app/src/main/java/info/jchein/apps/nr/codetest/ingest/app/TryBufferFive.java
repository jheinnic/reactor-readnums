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

import info.jchein.apps.nr.codetest.ingest.messages.CounterIncrementsAllocator;
import info.jchein.apps.nr.codetest.ingest.messages.ICounterIncrements;
import info.jchein.apps.nr.codetest.ingest.messages.IInputMessage;
import info.jchein.apps.nr.codetest.ingest.messages.IRawInputBatch;
import info.jchein.apps.nr.codetest.ingest.messages.IWriteFileBuffer;
import info.jchein.apps.nr.codetest.ingest.messages.InputMessageAllocator;
import info.jchein.apps.nr.codetest.ingest.messages.RawInputBatchAllocator;
import info.jchein.apps.nr.codetest.ingest.messages.WriteFileBufferAllocator;
import info.jchein.apps.nr.codetest.ingest.reusable.IReusableAllocator;
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
import reactor.rx.action.Control;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.PushSubscription;


public class TryBufferFive
{
   private static final Logger LOG = LoggerFactory.getLogger(TryBufferFive.class);


   public static void main(final String[] args) throws InterruptedException
   {
      final byte numDataPartitions = (byte) 4;

      @SuppressWarnings("unused")
      final Environment reactorEnv = Environment.initializeIfEmpty()
      .assignErrorJournal(err ->
      {
         LOG.error("Envrionment was asked to route unhandled exception: ", err);
         err.printStackTrace();
      });

      final IReusableAllocator<IInputMessage> msgAlloc = new InputMessageAllocator(2097152, false);

      final IReusableAllocator<IRawInputBatch> batchAlloc =
         new RawInputBatchAllocator(1024, false, 3000);

      final WriteFileBufferAllocator writeBufAlloc =
         new WriteFileBufferAllocator(4096, false, 3072, new long[] { 0 });

      final IReusableAllocator<ICounterIncrements> counterAlloc =
         new CounterIncrementsAllocator(2048, false);

      final Codec<Buffer, IInputMessage, IInputMessage> msgCodec =
         new DelimitedCodec<IInputMessage, IInputMessage>(
            true,
            new InputMessageCodec(numDataPartitions, 16384, msgAlloc));

      final TcpServer<IInputMessage, IInputMessage> tcpServer =
         NetStreams.<IInputMessage,
         IInputMessage> tcpServer(
            NettyTcpServer.class,
            aSpec -> {
               return aSpec
               .env(
                  Environment.initializeIfEmpty())
               .codec(msgCodec)
               // .dispatcher(Environment.sharedDispatcher())
               .dispatcher(Environment.newDispatcher("serverDispatcher", 32768, 1))
               .listen("127.0.0.1", 5000);
            }
         );

      final Broadcaster<Stream<IInputMessage>> streamsToMerge =
         Broadcaster.create(reactorEnv, SynchronousDispatcher.INSTANCE);

      final IUniqueMessageTrie uniqueTrie = new UniqueMessageTrie();
      final File fileObject = new File("tryBuffer.dat");
      final FileChannel fileChannel = openFile(fileObject);
      // final AgileWaitingStrategy waitStrategy = new AgileWaitingStrategy();

      final Timer workTimer = new HashWheelTimer(
         "work-timer",
         20,
         1024,
         new HashWheelTimer.SleepWait(),
         Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("work-timer-run")));

      @SuppressWarnings("rawtypes")
      final Processor[] fanOutProcessors = new Processor[4];
      // final Broadcaster[] heartbeatBroadcaster = new Broadcaster[4];
      for (int ii = 0; ii < 4; ii++) {
         fanOutProcessors[ii] = RingBufferWorkProcessor.share("fanOutProcessor-" + ii, 4096, true);
         // heartbeatBroadcaster[ii] = Broadcaster.create();
      }

      final Processor<IRawInputBatch, IRawInputBatch> writeOutputProcessor =
         RingBufferWorkProcessor.share("writeFileWorkProcessor", 4096, true);

      final Timer counterTimer = new HashWheelTimer(
         "counter-timer",
         250,
         1024,
         new HashWheelTimer.SleepWait(),
         Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("counter-timer-run")));
      final Processor<ICounterIncrements, ICounterIncrements> counterIncrementsProcessor =
         RingBufferWorkProcessor.share("counterIncrementsProcessor", 4096, true);

      final long[] lastUpdateNanos = new long[] { System.nanoTime() };
      final int maxBatchesPerFileBuffer = 3072 / 64;



      final Stream<IRawInputBatch> computeStream =
         streamsToMerge.startWith(
            Streams.<IInputMessage>never())
         .<IInputMessage> merge()
         .groupBy(evt -> Byte.valueOf(evt.getPartitionIndex()))
         .retry( t -> {
            LOG.error("Retrying from channel handler stream after ", t);
            return true;
         })
         .<IRawInputBatch> flatMap(partitionStream -> {
            final byte partitionIndex =
               partitionStream.key().byteValue();

            return
            partitionStream
//                  .log("rawInputFromStream")
            .process((Processor<IInputMessage, IInputMessage>) fanOutProcessors[partitionIndex])
//                  .log("rawInputFromProcessor")
            .window(64, 6000, TimeUnit.MILLISECONDS, workTimer)
            .<IRawInputBatch> flatMap( nestedWindow -> {
               return
               nestedWindow
//                     .log("rawInputInWindow")
               .reduce(
                  batchAlloc.allocate(),
                  (final IRawInputBatch batch, final IInputMessage msg) -> {
                     msg.beforeRead();
                     final int prefix = msg.getPrefix();
                     final short suffix = msg.getSuffix();
                     final int message = msg.getMessage();
                     msg.release();

                     if (uniqueTrie.isUnique(prefix, suffix)) {
                        batch.acceptUniqueInput(message);
                     } else {
                        batch.trackSkippedDuplicate();
                     }

                     return batch;
                  }
               );
            });
         })
         .retry( t -> {
            LOG.error("Retrying from compute stream after ", t);
            return true;
         });

 //           final Stream<IWriteFileBuffer> writeFileBufferStream =
//            final Stream<ICounterIncrements> fileIOStream =
         final Control statsReport =
            computeStream
            .process(writeOutputProcessor)
//                  .log("batchFromProcessor")
            .window(maxBatchesPerFileBuffer, 3, TimeUnit.SECONDS)
            .<IWriteFileBuffer> flatMap(
               rawBatch -> {
                  return rawBatch
//                        .log("batchInIOWindow")
                  .<IWriteFileBuffer>scan(
                        writeBufAlloc.allocate(),
                        (nextBuffer, nextBatch) -> {
                           while (nextBatch.transferToFileBuffer(nextBuffer) == false) {
//                                    nextBuffer.afterWrite();
//                                    LOG.info("Room was not available in {}", nextBuffer);
                              nextBuffer = writeBufAlloc.allocate();
                           }
                           nextBatch.release();

//                                 LOG.info("Room available in {}", nextBuffer);
                           return nextBuffer;
                        }
                  )
//                        .log("next stats from IO")
                  .distinctUntilChanged()
                  .buffer(2)
                  .<IWriteFileBuffer>split()
                  .elementAt(0);
//                        .log("final stats from IO");
//                        .filter(writeBuffer -> (writeBuffer != null));
               }
            )
//               .log("writing")
            .map(writeBuf -> {
//                     writeBuf.beforeRead();
               final ICounterIncrements countIncr =
                  processBatch(writeBuf, fileChannel, fileObject, counterAlloc);

               countIncr.afterWrite();
               return countIncr;
            })
//               .log("preoutput")
            .retry( t -> {
               LOG.error("Retrying from write output processor stream after ", t);
               return true;
            })
            .process(counterIncrementsProcessor)
//               .log("compute")
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

         final ChannelStream<IInputMessage,IInputMessage> theChannelStream = channelStream;

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
         final Stream<IInputMessage> noData = Streams.empty();
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
      final ByteBuffer buf = batchDef.getBufferToDrain();
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

         return counterIncrementsAllocator.allocate()
         .setDeltas(batchDef);
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

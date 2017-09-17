package vertx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.NetSocketInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import vertx.model.Message;

import java.io.File;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class WebServer extends AbstractVerticle implements Handler<NetSocket> {

  private static Logger logger = LoggerFactory.getLogger(WebServer.class.getName());

  private static Message newMsg() {
    return new Message("Hello, World!");
  }

  private static int jsonLen() {
    try {
      return newMapper().writeValueAsBytes(newMsg()).length;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static final byte[] STATIC_PLAINTEXT = "Hello, World!".getBytes(CharsetUtil.UTF_8);
  private static final int STATIC_PLAINTEXT_LEN = STATIC_PLAINTEXT.length;
  private static final ByteBuf PLAINTEXT_CONTENT_BUFFER = Unpooled.unreleasableBuffer(Unpooled.directBuffer().writeBytes(STATIC_PLAINTEXT));
  private static final CharSequence PLAINTEXT_CLHEADER_VALUE = new AsciiString(String.valueOf(STATIC_PLAINTEXT_LEN));
  private static final CharSequence JSON_CLHEADER_VALUE = new AsciiString(String.valueOf(jsonLen()));

  private static final CharSequence TYPE_PLAIN = new AsciiString("text/plain");
  private static final CharSequence TYPE_JSON = new AsciiString("application/json");
  private static final CharSequence SERVER_NAME = new AsciiString("Netty");
  private static final CharSequence CONTENT_TYPE_ENTITY = HttpHeaderNames.CONTENT_TYPE;
  private static final CharSequence DATE_ENTITY = HttpHeaderNames.DATE;
  private static final CharSequence CONTENT_LENGTH_ENTITY = HttpHeaderNames.CONTENT_LENGTH;
  private static final CharSequence SERVER_ENTITY = HttpHeaderNames.SERVER;
  private static final ObjectMapper MAPPER = newMapper();

  private final DateFormat FORMAT = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
  private AsciiString date = computeDate();
  private NetServer server;

  @Override
  public void start() throws Exception {
    int port = 8080;
    NetServerOptions options = new NetServerOptions();
    options.setAcceptBacklog(8192);
    options.setReuseAddress(true);
    server = vertx.createNetServer(options);
    server.connectHandler(WebServer.this).listen(port);
    vertx.setPeriodic(1000, handler -> date = computeDate());
  }

  private AsciiString computeDate() {
    return new AsciiString(FORMAT.format(new Date()));
  }

  private static final AtomicBoolean dumped = new AtomicBoolean();

  private static void dumpChannelConfig(ChannelConfig config) {
    if (dumped.compareAndSet(false, true)) {
      new Exception().printStackTrace();
      System.out.println("WriteSpinCount: " + config.getWriteSpinCount());
      System.out.println("Allocator: " + config.getAllocator());
      System.out.println("ConnectTimeoutMillis: " + config.getConnectTimeoutMillis());
      System.out.println("MessageSizeEstimator: " + config.getMessageSizeEstimator());
      System.out.println("WriteBufferHighWaterMark: " + config.getWriteBufferHighWaterMark());
      System.out.println("WriteBufferLowWaterMark: " + config.getWriteBufferLowWaterMark());
      System.out.println("getMaxMessagesPerRead: " + config.getMaxMessagesPerRead());
      System.out.println("getRecvByteBufAllocator: " + config.getRecvByteBufAllocator());
      Map<ChannelOption<?>, Object> options = config.getOptions();
      options.forEach((key, value) -> {
        System.out.println(key.name() + ": " + value);
      });
    }
  }

  @Override
  public void handle(NetSocket so) {
    NetSocketInternal internal = (NetSocketInternal) so;
    ChannelHandlerContext chctx = internal.channelHandlerContext();
    ChannelConfig cfg = chctx.channel().config();
    ChannelPipeline pipeline = chctx.pipeline();
    pipeline.addBefore("handler","encoder", new HttpResponseEncoder());
    pipeline.addBefore("handler","decoder", new HttpRequestDecoder(4096, 8192, 8192, false));
    pipeline.addBefore("handler","app", new HelloServerHandler(((ContextInternal) context).nettyEventLoop()));
/*
    internal.messageHandler(msg -> {
      if (msg instanceof HttpRequest) {
        try {
          HttpRequest request = (HttpRequest) msg;
          process(internal, request);
        } catch (Exception e) {
          so.close();
        } finally {
          ReferenceCountUtil.release(msg);
        }
      }
    });
*/
  }

  private void process(NetSocketInternal so, HttpRequest request) throws Exception {
    String uri = request.uri();
    switch (uri) {
      case "/plaintext":
        writePlainResponse(so, PLAINTEXT_CONTENT_BUFFER.duplicate());
        return;
      case "/json":
        byte[] json = MAPPER.writeValueAsBytes(newMsg());
        writeJsonResponse(so, Unpooled.wrappedBuffer(json));
        return;
    }
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND, Unpooled.EMPTY_BUFFER, false);
    so.writeMessage(response);
    so.close();
  }

  private void writePlainResponse(NetSocketInternal so, ByteBuf buf) {
    so.writeMessage(makeResponse(buf, TYPE_PLAIN, PLAINTEXT_CLHEADER_VALUE));
  }

  private void writeJsonResponse(NetSocketInternal so, ByteBuf buf) {
    so.writeMessage(makeResponse(buf, TYPE_JSON, JSON_CLHEADER_VALUE));
  }

  private FullHttpResponse makeResponse(ByteBuf buf, CharSequence contentType, CharSequence contentLength) {
    final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, buf, false);
    response.headers()
        .set(CONTENT_TYPE_ENTITY, contentType)
        .set(SERVER_ENTITY, SERVER_NAME)
        .set(DATE_ENTITY, date)
        .set(CONTENT_LENGTH_ENTITY, contentLength);
    return response;
  }

  @Override
  public void stop() {
    if (server != null) server.close();
  }

  private static ObjectMapper newMapper() {
    ObjectMapper m = new ObjectMapper();
    // m.registerModule(new AfterburnerModule());
    return m;
  }

  /**
   * Returns a random integer that is a suitable value for both the {@code id}
   * and {@code randomNumber} properties of a world object.
   *
   * @return a random world number
   */
  private static int randomWorld() {
    return 1 + ThreadLocalRandom.current().nextInt(10000);
  }

  public static void main(String[] args) throws Exception {
    JsonObject config = new JsonObject(new String(Files.readAllBytes(new File(args[0]).toPath())));
    int procs = Runtime.getRuntime().availableProcessors();
    Vertx vertx = Vertx.vertx();
    vertx.exceptionHandler(err -> {
      err.printStackTrace();
    });
    vertx.deployVerticle(WebServer.class.getName(),
        new DeploymentOptions().setInstances(procs * 2).setConfig(config), event -> {
          if (event.succeeded()) {
            System.out.println("Native: " + VertxOptions.DEFAULT_NATIVE_TRANSPORT);
            logger.debug("Your Vert.x application is started!");
          } else {
            logger.error("Unable to start your application", event.cause());
          }
        });
  }
}

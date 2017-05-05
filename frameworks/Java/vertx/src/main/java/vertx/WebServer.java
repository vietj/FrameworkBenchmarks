package vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PostgresClient;
import io.vertx.pgclient.PostgresClientOptions;
import io.vertx.pgclient.PostgresConnectionPool;
import io.vertx.pgclient.Result;
import io.vertx.pgclient.Row;
import vertx.model.World;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class WebServer extends AbstractVerticle implements Handler<HttpServerRequest> {

  static Logger logger = LoggerFactory.getLogger(WebServer.class.getName());

  static int getIntEnv(String name, int def) {
    try {
      return Integer.parseInt(System.getenv(name));
    } catch (Exception e) {
      return def;
    }
  }

  static boolean getBooleanEnv(String name, boolean def) {
    try {
      return Boolean.parseBoolean(System.getenv(name));
    } catch (Exception e) {
      return def;
    }
  }

  private static final int PSQL_DB_POOL_SIZE = getIntEnv("PSQL_DB_POOL_SIZE", 1);
  private static final int PSQL_DB_PIPELINING_LIMIT = getIntEnv("PSQL_DB_PIPELINING_LIMIT", 256);

  private static final String PATH_PLAINTEXT = "/plaintext";
  private static final String PATH_JSON = "/json";
  private static final String PATH_PSQL_DB = "/psql/db";
  private static final String PATH_INFO = "/info";

  private static final CharSequence RESPONSE_TYPE_PLAIN = HttpHeaders.createOptimized("text/plain");
  private static final CharSequence RESPONSE_TYPE_JSON = HttpHeaders.createOptimized("application/json");

  private static final String TEXT_MESSAGE = "message";
  private static final String HELLO_WORLD = "Hello, world!";
  private static final Buffer HELLO_WORLD_BUFFER = Buffer.buffer(HELLO_WORLD);

  private static final CharSequence HEADER_SERVER = HttpHeaders.createOptimized("server");
  private static final CharSequence HEADER_DATE = HttpHeaders.createOptimized("date");
  private static final CharSequence HEADER_CONTENT_TYPE = HttpHeaders.createOptimized("content-type");
  private static final CharSequence HEADER_CONTENT_LENGTH = HttpHeaders.createOptimized("content-length");

  private static final CharSequence HELLO_WORLD_LENGTH = HttpHeaders.createOptimized("" + HELLO_WORLD.length());
  private static final CharSequence SERVER = HttpHeaders.createOptimized("vert.x");

  private CharSequence dateString;

  private HttpServer server;

  private Benchmark pg;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    int port = 8080;
    server = vertx.createHttpServer(new HttpServerOptions());
    server.requestHandler(WebServer.this).listen(port);
    dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    pg = new Benchmark(vertx, PSQL_DB_POOL_SIZE, config());
    vertx.setPeriodic(1000, handler -> {
      dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    });
    pg.start(startFuture);
  }

  @Override
  public void handle(HttpServerRequest request) {
    switch (request.path()) {
      case PATH_PLAINTEXT:
        handlePlainText(request);
        break;
      case PATH_JSON:
        handleJson(request);
        break;
      case PATH_PSQL_DB:
        pg.handle(request);
        break;
      case PATH_INFO:
        handleInfo(request);
        break;
      default:
        request.response().setStatusCode(404);
        request.response().end();
        break;
    }
  }

  @Override
  public void stop() {
    if (server != null) server.close();
  }

  private void handlePlainText(HttpServerRequest request) {
    HttpServerResponse response = request.response();
    MultiMap headers = response.headers();
    headers
        .add(HEADER_CONTENT_TYPE, RESPONSE_TYPE_PLAIN)
        .add(HEADER_SERVER, SERVER)
        .add(HEADER_DATE, dateString)
        .add(HEADER_CONTENT_LENGTH, HELLO_WORLD_LENGTH);
    response.end(HELLO_WORLD_BUFFER);
  }

  private void handleJson(HttpServerRequest request) {
    Buffer buff = Buffer.buffer(Json.encode(Collections.singletonMap(TEXT_MESSAGE, HELLO_WORLD)));
    HttpServerResponse response = request.response();
    MultiMap headers = response.headers();
    headers
        .add(HEADER_CONTENT_TYPE, RESPONSE_TYPE_JSON)
        .add(HEADER_SERVER, SERVER)
        .add(HEADER_DATE, dateString);
    response.end(buff);
  }

  private void handleInfo(HttpServerRequest request) {
    HttpServerResponse response = request.response();
    MultiMap headers = response.headers();
    headers.add(HEADER_CONTENT_TYPE, RESPONSE_TYPE_JSON);
    response.end(new JsonObject()
        .put("PSQL_DB_POOL_SIZE", PSQL_DB_POOL_SIZE)
        .put("config", config())
        .encode());
  }

  private class Benchmark {

    /**
     * Returns a random integer that is a suitable value for both the {@code id}
     * and {@code randomNumber} properties of a world object.
     *
     * @return a random world number
     */
    int randomWorld() {
      return 1 + ThreadLocalRandom.current().nextInt(10000);
    }

    private final PostgresClient database;
    private final int poolSize;
    private PostgresConnectionPool pool;

    public Benchmark(Vertx vertx, int poolSize, JsonObject config) {
      PostgresClientOptions options = new PostgresClientOptions();
      options.setDatabase(config.getString("database"));
      options.setHost(config.getString("host"));
      options.setUsername(config.getString("username"));
      options.setPassword(config.getString("password"));
      options.setPipeliningLimit(PSQL_DB_PIPELINING_LIMIT);
      this.database = PostgresClient.create(vertx, options);
      this.poolSize = poolSize;
    }

    void start(Handler<AsyncResult<Void>> completionHandler) {
      database.createPool(poolSize, ar -> {
        if (ar.succeeded()) {
          pool = ar.result();
        }
        completionHandler.handle(ar.mapEmpty());
      });
    }

    final void handle(HttpServerRequest req) {
      HttpServerResponse resp = req.response();
      pool.execute("SELECT id, randomnumber from WORLD where id = " + randomWorld(), res -> {
        if (res.succeeded()) {
          Result resultSet = res.result();
          if (resultSet == null || resultSet.size() == 0) {
            resp.setStatusCode(404).end();
            return;
          }
          Row row = resultSet.get(0);
          resp
              .putHeader(HttpHeaders.SERVER, SERVER)
              .putHeader(HttpHeaders.DATE, dateString)
              .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
              .end(Json.encode(new World((Integer)row.get(0), (Integer) row.get(1))));
        } else {
          logger.error(res.cause());
          resp.setStatusCode(500).end(res.cause().getMessage());
        }
      });
    }
  }

  public static void main(String[] args) throws Exception {
    JsonObject config = new JsonObject(new String(Files.readAllBytes(new File(args[0]).toPath())));
    int procs = Runtime.getRuntime().availableProcessors();
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(WebServer.class.getName(),
        new DeploymentOptions().setInstances(procs * 2).setConfig(config), event -> {
          if (event.succeeded()) {
            logger.debug("Your Vert.x application is started!");
          } else {
            logger.error("Unable to start your application", event.cause());
          }
        });
  }
}

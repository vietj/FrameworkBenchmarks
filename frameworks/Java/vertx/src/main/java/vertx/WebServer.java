package vertx;

import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.HttpServerInternal;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgClient;
import io.vertx.pgclient.PgClientOptions;
import vertx.model.World;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class WebServer extends AbstractVerticle implements Function<HttpServerRequest, Future<HttpServerResponse>> {

  static Logger logger = LoggerFactory.getLogger(WebServer.class.getName());

  private static int getIntEnv(String name, int def) {
    try {
      return Integer.parseInt(System.getenv(name));
    } catch (Exception e) {
      return def;
    }
  }

  private static final int PSQL_DB_POOL_SIZE = getIntEnv("PSQL_DB_POOL_SIZE", 4);
  private static final int SERVER_CONCURRENCY = getIntEnv("SERVER_CONCURRENCY", 4);

  private static final String PATH_PLAINTEXT = "/plaintext";
  private static final String PATH_JSON = "/json";
  private static final String PATH_PSQL_DB = "/psql/db";

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

  private HttpServerInternal server;

  private PostgresClient pg;

  @Override
  public void start() {
    int port = 8080;
    server = (HttpServerInternal) vertx.createHttpServer(new HttpServerOptions());
    server.requestHandler(WebServer.this).setPipeliningLimit(SERVER_CONCURRENCY).listen(port);
    dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    pg = new PostgresClient(vertx, PSQL_DB_POOL_SIZE, config());
    vertx.setPeriodic(1000, handler -> {
      dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    });
  }

  @Override
  public Future<HttpServerResponse> apply(HttpServerRequest request) {
    switch (request.path()) {
      case PATH_PLAINTEXT:
        handlePlainText(request);
        return null;
      case PATH_JSON:
        handleJson(request);
        return null;
      case PATH_PSQL_DB:
        return pg.handle(request);
      default:
        request.response().setStatusCode(404);
        request.response().end();
        return null;
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

  private class PostgresClient {

    /**
     * Returns a random integer that is a suitable value for both the {@code id}
     * and {@code randomNumber} properties of a world object.
     *
     * @return a random world number
     */
    int randomWorld() {
      return 1 + ThreadLocalRandom.current().nextInt(10000);
    }

    private final PgClient database;

    public PostgresClient(Vertx vertx, int poolSize, JsonObject config) {
      PgClientOptions options = new PgClientOptions();
      options.setPoolsize(poolSize);
      options.setDatabase(config.getString("database"));
      options.setHost(config.getString("host"));
      options.setUsername(config.getString("username"));
      options.setPassword(config.getString("password"));
      this.database = PgClient.create(vertx, options);
    }

    final Future<HttpServerResponse> handle(HttpServerRequest req) {
      Future<HttpServerResponse> respFut = Future.future();
      Future<ResultSet> resFut = Future.future();
      respFut.setHandler(ar1 -> {
        if (ar1.succeeded()) {
          HttpServerResponse resp = req.response();
          resFut.setHandler(ar2 -> {
            if (ar2.succeeded()) {
              com.github.pgasync.ResultSet resultSet = resFut.result();
              if (resultSet == null || resultSet.size() == 0) {
                resp.setStatusCode(404).end();
                return;
              }
              Row row = resultSet.row(0);
              resp
                  .putHeader(HttpHeaders.SERVER, SERVER)
                  .putHeader(HttpHeaders.DATE, dateString)
                  .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                  .end(Json.encode(new World(row.getInt(0), row.getInt(1))));
            } else {
              resp.setStatusCode(500).end(ar2.cause().getMessage());
            }
          });
        }
      });
      database.query("SELECT id, randomnumber from WORLD where id = " + randomWorld(), resFut);
      return respFut;
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

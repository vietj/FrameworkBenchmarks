package vertx;

import io.vertx.core.AbstractVerticle;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PostgresClient;
import io.vertx.pgclient.PostgresClientOptions;
import io.vertx.pgclient.PostgresConnection;
import io.vertx.pgclient.PostgresConnectionPool;
import io.vertx.pgclient.Result;
import io.vertx.pgclient.Row;
import vertx.model.World;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class WebServer extends AbstractVerticle implements Handler<HttpServerRequest> {

  /**
   * Returns the value of the "queries" getRequest parameter, which is an integer
   * bound between 1 and 500 with a default value of 1.
   *
   * @param request the current HTTP request
   * @return the value of the "queries" parameter
   */
  static int getQueries(HttpServerRequest request) {
    String param = request.getParam("queries");

    if (param == null) {
      return 1;
    }
    try {
      int parsedValue = Integer.parseInt(param);
      return Math.min(500, Math.max(1, parsedValue));
    } catch (NumberFormatException e) {
      return 1;
    }
  }

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
  private static final String PATH_DB = "/db";
  private static final String PATH_QUERIES = "/queries";
  private static final String PATH_UPDATE = "/update";
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

  private PostgresClient client;
  private PostgresConnectionPool pool;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    int port = 8080;
    server = vertx.createHttpServer(new HttpServerOptions());
    server.requestHandler(WebServer.this).listen(port);
    dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    JsonObject config = config();
    vertx.setPeriodic(1000, handler -> {
      dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    });
    PostgresClientOptions options = new PostgresClientOptions();
    options.setDatabase(config.getString("database"));
    options.setHost(config.getString("host"));
    options.setUsername(config.getString("username"));
    options.setPassword(config.getString("password"));
    options.setPipeliningLimit(PSQL_DB_PIPELINING_LIMIT);
    client = PostgresClient.create(vertx, options);
    pool = client.createPool(PSQL_DB_POOL_SIZE);
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
      case PATH_DB:
        handleDb(request);
        break;
      case PATH_QUERIES:
        new Queries().handle(request);
        break;
      case PATH_UPDATE:
        new Update().handle(request);
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

  /**
   * Returns a random integer that is a suitable value for both the {@code id}
   * and {@code randomNumber} properties of a world object.
   *
   * @return a random world number
   */
  private static int randomWorld() {
    return 1 + ThreadLocalRandom.current().nextInt(10000);
  }

  private void handleDb(HttpServerRequest req) {
    HttpServerResponse resp = req.response();
    pool.getConnection(ar -> {
      if (ar.succeeded()) {
        PostgresConnection conn = ar.result();
        conn.execute("SELECT id, randomnumber from WORLD where id = " + randomWorld(), res -> {
          if (res.succeeded()) {
            Result resultSet = res.result();
            if (resultSet == null || resultSet.size() == 0) {
              resp.setStatusCode(404).end();
              conn.close();
              return;
            }
            Row row = resultSet.get(0);
            conn.close();
            resp
                .putHeader(HttpHeaders.SERVER, SERVER)
                .putHeader(HttpHeaders.DATE, dateString)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(Json.encode(new World((Integer)row.get(0), (Integer) row.get(1))));
          } else {
            logger.error(res.cause());
            conn.close();
            resp.setStatusCode(500).end(res.cause().getMessage());
          }
        });
      } else {
        resp.setStatusCode(500).end(ar.cause().getMessage());
      }
    });
  }

  class Queries {

    boolean failed;
    JsonArray worlds = new JsonArray();

    public void handle(HttpServerRequest req) {
      HttpServerResponse resp = req.response();
      final int queries = getQueries(req);

      pool.getConnection(ar -> {
        if (ar.succeeded()) {
          PostgresConnection conn = ar.result();
          for (int i = 0; i < queries; i++) {
            conn.execute("SELECT id, randomnumber from WORLD where id = " + randomWorld(), query -> {
              if (!failed) {
                if (query.failed()) {
                  failed = true;
                  resp.setStatusCode(500).end(query.cause().getMessage());
                  conn.close();
                  return;
                }

                // we need a final reference
                final Row row = query.result().get(0);
                worlds.add(new JsonObject().put("id", "" + row.get(0)).put("randomNumber", "" + row.get(1)));

                // stop condition
                if (worlds.size() == queries) {
                  resp
                      .putHeader(HttpHeaders.SERVER, SERVER)
                      .putHeader(HttpHeaders.DATE, dateString)
                      .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                      .end(Json.encode(worlds.encode()));
                  conn.close();
                }
              }
            });
          }
        } else {
          resp.setStatusCode(500).end(ar.cause().getMessage());
        }
      });
    }

  }

  class Update {

    boolean failed;
    JsonArray worlds = new JsonArray();

    public void handle(HttpServerRequest req) {
      HttpServerResponse resp = req.response();
      final int queries = getQueries(req);

      pool.getConnection(ar -> {
        if (ar.succeeded()) {
          PostgresConnection conn = ar.result();
          for (int i = 0; i < queries; i++) {
            int id = randomWorld();
            conn.execute("SELECT id, randomnumber from WORLD where id = " + id, query -> {
              if (!failed) {
                if (query.failed()) {
                  failed = true;
                  resp.setStatusCode(500).end(query.cause().getMessage());
                  conn.close();
                  return;
                }

                int randomNumber = randomWorld();

                conn.execute("UPDATE world SET randomnumber = " + randomNumber + " WHERE id = " + id, update -> {
                  if (!failed) {
                    if (update.failed()) {
                      failed = true;
                      resp.setStatusCode(500).end(query.cause().getMessage());
                      conn.close();
                      return;
                    }
                  }

                  // we need a final reference
                  Row row = query.result().get(0);
                  worlds.add(new JsonObject().put("id", "" + row.get(0)).put("randomNumber", "" + randomNumber));

                  // stop condition
                  if (worlds.size() == queries) {
                    conn.close();
                    resp
                        .putHeader(HttpHeaders.SERVER, SERVER)
                        .putHeader(HttpHeaders.DATE, dateString)
                        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                        .end(Json.encode(worlds.encode()));
                  }
                });
              }
            });
          }
        } else {
          resp.setStatusCode(500).end(ar.cause().getMessage());
        }
      });
    }
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

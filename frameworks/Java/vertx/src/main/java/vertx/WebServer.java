package vertx;

import com.julienviet.pgclient.PgBatch;
import com.julienviet.pgclient.PgClient;
import com.julienviet.pgclient.PgClientOptions;
import com.julienviet.pgclient.PgConnection;
import com.julienviet.pgclient.PgPreparedStatement;
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
import vertx.model.World;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
  private static final String PATH_UPDATES = "/updates";
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

  private PgClient client;
  private PgConnection conn;
  private PgPreparedStatement worldUpdate;
  private PgPreparedStatement worldSelect;

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
    PgClientOptions options = new PgClientOptions();
    options.setDatabase(config.getString("database"));
    options.setHost(config.getString("host"));
    options.setPort(config.getInteger("port", 5432));
    options.setUsername(config.getString("username"));
    options.setPassword(config.getString("password"));
    options.setPipeliningLimit(PSQL_DB_PIPELINING_LIMIT);
    client = PgClient.create(vertx, options);
    client.connect(ar -> {
      if (ar.succeeded()) {
        conn = ar.result();
        worldUpdate = conn.prepare("UPDATE world SET randomnumber=$1 WHERE id=$2");
        worldSelect = conn.prepare("SELECT id, randomnumber from WORLD where id=$1");
        startFuture.complete();
      } else {
        startFuture.fail(ar.cause());
      }
    });
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
      case PATH_UPDATES:
        new Update(request).handle();
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
    worldSelect.query(randomWorld()).execute(res -> {
      if (res.succeeded()) {
        List<JsonArray> resultSet = res.result().getResults();
        if (resultSet.isEmpty()) {
          resp.setStatusCode(404).end();
          return;
        }
        JsonArray row = resultSet.get(0);
        resp
            .putHeader(HttpHeaders.SERVER, SERVER)
            .putHeader(HttpHeaders.DATE, dateString)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .end(Json.encode(new World(row.getInteger(0), row.getInteger(1))));
      } else {
        logger.error(res.cause());
        resp.setStatusCode(500).end(res.cause().getMessage());
      }
    });
  }

  class Queries {

    boolean failed;
    JsonArray worlds = new JsonArray();

    public void handle(HttpServerRequest req) {
      HttpServerResponse resp = req.response();
      final int queries = getQueries(req);
      for (int i = 0; i < queries; i++) {
        worldSelect.query(randomWorld()).execute(ar -> {
          if (!failed) {
            if (ar.failed()) {
              failed = true;
              resp.setStatusCode(500).end(ar.cause().getMessage());
              return;
            }

            // we need a final reference
            final JsonArray row = ar.result().getResults().get(0);
            worlds.add(new JsonObject().put("id", "" + row.getInteger(0)).put("randomNumber", "" + row.getInteger(1)));

            // stop condition
            if (worlds.size() == queries) {
              resp
                  .putHeader(HttpHeaders.SERVER, SERVER)
                  .putHeader(HttpHeaders.DATE, dateString)
                  .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                  .end(Json.encode(worlds.encode()));
            }
          }
        });
      }
    }

  }

  class Update {

    final HttpServerRequest request;
    boolean failed;
    int queryCount;
    final World[] worlds;

    public Update(HttpServerRequest request) {
      final int queries = getQueries(request);
      this.request = request;
      this.worlds = new World[queries];
    }

    public void handle() {
      for (int i = 0; i < worlds.length; i++) {
        int id = randomWorld();
        int index = i;
        worldSelect.query(id).execute(ar2 -> {
          if (!failed) {
            if (ar2.failed()) {
              failed = true;
              sendError(ar2.cause());
              return;
            }
            worlds[index] = new World(ar2.result().getResults().get(0).getInteger(0), randomWorld());
            if (++queryCount == worlds.length) {
              handleUpdates();
            }
          }
        });
      }
    }

    void handleUpdates() {
      Arrays.sort(worlds);
      PgBatch batch = worldUpdate.batch();
      JsonArray json = new JsonArray();
      for (int i = 0;i < worlds.length;i++) {
        World world = worlds[i];
        batch.add(world.getRandomNumber(), world.getId());
      }
      batch.execute(ar3 -> {
        if (!failed) {
          if (ar3.failed()) {
            failed = true;
            sendError(ar3.cause());
            return;
          }
          for (int i = 0;i < worlds.length;i++) {
            World world = worlds[i];
            json.add(new JsonObject().put("id", "" + world.getId()).put("randomNumber", "" + world.getRandomNumber()));
          }
          request.response()
              .putHeader(HttpHeaders.SERVER, SERVER)
              .putHeader(HttpHeaders.DATE, dateString)
              .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
              .end(Json.encode(json.encode()));
        }
      });
    }

    void sendError(Throwable err) {
      failed = true;
      logger.error("", err);
      request.response().setStatusCode(500).end(err.getMessage());
    }
  }

  private void handleInfo(HttpServerRequest request) {
    HttpServerResponse response = request.response();
    MultiMap headers = response.headers();
    headers.add(HEADER_CONTENT_TYPE, RESPONSE_TYPE_JSON);
    response.end(new JsonObject()
        .put("PSQL_DB_POOL_SIZE", PSQL_DB_POOL_SIZE)
        .put("PSQL_DB_PIPELINING_LIMIT", PSQL_DB_PIPELINING_LIMIT)
        .put("config", config())
        .encode());
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
            logger.debug("Your Vert.x application is started!");
          } else {
            logger.error("Unable to start your application", event.cause());
          }
        });
  }
}

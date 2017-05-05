package vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgClient;
import io.vertx.pgclient.PgClientOptions;
import io.vertx.pgclient.PostgresClient;
import io.vertx.pgclient.PostgresClientOptions;
import io.vertx.pgclient.PostgresConnection;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import static vertx.WebServer.getBooleanEnv;
import static vertx.WebServer.getIntEnv;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class DbBenchmark extends AbstractVerticle {

  private static final int DB_MAX_INFLIGHT = getIntEnv("DB_MAX_INFLIGHT", 128);
  private static final int DB_EVENT_LOOP_SIZE = getIntEnv("DB_EVENT_LOOP_SIZE", 1);
  private static final int DB_POOL_SIZE = getIntEnv("DB_POOL_SIZE", 1);
  private static final boolean DB_CONN_PIPELINED = getBooleanEnv("DB_CONN_PIPELINED", false);
  private static final int SEND_BUFFER_SIZE = getIntEnv("SEND_BUFFER_SIZE", -1);
  private static final int RECEIVE_BUFFER_SIZE = getIntEnv("RECEIVE_BUFFER_SIZE", -1);
  private static final LongAdder count = new LongAdder();
  private static Logger logger = LoggerFactory.getLogger(DbBenchmark.class.getName());

  public static void main(String[] args) throws Exception {
    JsonObject config = new JsonObject(new String(Files.readAllBytes(new File(args[0]).toPath())));
    int procs = Runtime.getRuntime().availableProcessors();
    Vertx vertx = Vertx.vertx();
    vertx.setPeriodic(1000, id -> {
      System.out.println("Count = " + DbBenchmark.count.sumThenReset());
    });
    vertx.deployVerticle(DbBenchmark.class.getName(),
        new DeploymentOptions().setInstances(DB_EVENT_LOOP_SIZE).setConfig(config), event -> {
          if (event.succeeded()) {
            logger.debug("Your Vert.x application is started!");
          } else {
            logger.error("Unable to start your application", event.cause());
          }
        });
  }

  private int inflight = 0;
  private long random = 0;

  @Override
  public void start() throws Exception {
    JsonObject config = config();
    PostgresClientOptions options = new PostgresClientOptions();
    options.setDatabase(config.getString("database"));
    options.setHost(config.getString("host"));
    options.setUsername(config.getString("username"));
    options.setPassword(config.getString("password"));
    options.setPipeliningLimit(DB_MAX_INFLIGHT);

    if (SEND_BUFFER_SIZE > 0) {
      options.setSendBufferSize(SEND_BUFFER_SIZE);
    }
    if (RECEIVE_BUFFER_SIZE > 0) {
      options.setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
    }

    PostgresClient client = PostgresClient.create(vertx, options);
    client.connect(ar -> {
      if (ar.succeeded()) {
        run(ar.result());
      } else {
        logger.error(ar.cause());
      }
    });
  }

  private void run(PostgresConnection conn) {
    while (inflight < DB_MAX_INFLIGHT) {
      inflight++;
      count.add(1);
      conn.execute("SELECT id, randomnumber from WORLD where id = " + randomWorld(), res -> {
        inflight--;
        run(conn);
        if (res.failed()) {
          logger.error(res.cause());
        }
      });
    }
  }

  int randomWorld() {
    return (int) ((random += 4) % 10000);
  }
}

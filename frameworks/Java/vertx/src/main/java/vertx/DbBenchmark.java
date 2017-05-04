package vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgClient;
import io.vertx.pgclient.PgClientOptions;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class DbBenchmark extends AbstractVerticle {

  static Logger logger = LoggerFactory.getLogger(DbBenchmark.class.getName());

  public static void main(String[] args) throws Exception {
    JsonObject config = new JsonObject(new String(Files.readAllBytes(new File(args[0]).toPath())));
    int procs = Runtime.getRuntime().availableProcessors();
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(DbBenchmark.class.getName(),
        new DeploymentOptions().setInstances(1).setConfig(config), event -> {
          if (event.succeeded()) {
            logger.debug("Your Vert.x application is started!");
          } else {
            logger.error("Unable to start your application", event.cause());
          }
        });
  }

  private PgClient client;
  private int maxInFlight = 128;
  private int inflight = 0;
  private int count = 0;

  @Override
  public void start() throws Exception {
    JsonObject config = config();
    PgClientOptions options = new PgClientOptions();
    options.setPoolsize(1);
    options.setPipelined(false);
    options.setDatabase(config.getString("database"));
    options.setHost(config.getString("host"));
    options.setUsername(config.getString("username"));
    options.setPassword(config.getString("password"));
    client = PgClient.create(vertx, options);
    run();
    vertx.setPeriodic(1000, id -> {
      System.out.println("count " + count);
      count = 0;
    });
  }

  private void run() {
    while (inflight < maxInFlight) {
      inflight++;
      count++;
      client.query("SELECT id, randomnumber from WORLD where id = " + randomWorld(), res -> {
        inflight--;
        run();
        if (res.failed()) {
          logger.error(res.cause());
        }
      });
    }
  }

  int randomWorld() {
    return 1 + ThreadLocalRandom.current().nextInt(10000);
  }
}

package vertx;

import com.julienviet.pgclient.PgClientOptions;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.io.File;
import java.util.Collections;

import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.V9_6;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class Main {

  private static EmbeddedPostgres postgres;

  public static void startPg() throws Exception {
    IRuntimeConfig config;
    File target = new File("target");
    if (target.exists() && target.isDirectory()) {
      config = EmbeddedPostgres.cachedRuntimeConfig(target.toPath());
    } else {
      config = EmbeddedPostgres.defaultRuntimeConfig();
    }
    postgres = new EmbeddedPostgres(V9_6);
    postgres.start(config, "localhost", 8081, "hello_world", "benchmarkdbuser", "benchmarkdbpass", Collections.emptyList());
    postgres.getProcess().get().importFromFile(new File("src/main/resources/create-postgres.sql"));
  }

  public static void stopPg() throws Exception {
    postgres.stop();
  }

  public static void main(String[] args) throws Exception {
    startPg();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public synchronized void start() {
        try {
          stopPg();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new WebServer(), new DeploymentOptions().setConfig(new JsonObject()
        .put("host", "localhost")
        .put("port", 8081)
        .put("username", "benchmarkdbuser")
        .put("password", "benchmarkdbpass")
        .put("database", "hello_world")
        .put("maxPoolSize", 64)
    ));
  }

}

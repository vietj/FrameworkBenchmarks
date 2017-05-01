package vertx;

import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
import io.vertx.pgclient.PgClient;
import io.vertx.pgclient.PgClientOptions;
import vertx.model.World;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class WebServer extends AbstractVerticle implements Handler<HttpServerRequest> {

	static Logger logger = LoggerFactory.getLogger(WebServer.class.getName());

	private static final String PATH_PLAINTEXT = "/plaintext";
	private static final String PATH_JSON = "/json";
	private static final String PATH_PSQL_DB = "/psql/db";

	private static final String RESPONSE_TYPE_PLAIN = "text/plain";
	private static final String RESPONSE_TYPE_JSON = "application/json";

	private static final String TEXT_MESSAGE = "message";
	private static final String HELLO_WORLD = "Hello, world!";
	private static final Buffer HELLO_WORLD_BUFFER = Buffer.buffer(HELLO_WORLD);

	private static final String HEADER_SERVER = "SERVER";
	private static final String HEADER_DATE = "DATE";
	private static final String HEADER_CONTENT = "content-type";

	private static final String SERVER = "vertx";

	private String dateString;

	private HttpServer server;

	private PostgresClient pg;

	@Override
	public void start() {

		int port = 8080;

		server = vertx.createHttpServer(new HttpServerOptions().setPipelining(true));

		server.requestHandler(WebServer.this).listen(port);

		dateString = java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now());

		pg = new PostgresClient(vertx, config());

		vertx.setPeriodic(1000, handler -> {
			dateString = java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now());
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
		case PATH_PSQL_DB:
			pg.handle(request);
			break;
		default:
			request.response().setStatusCode(404);
			request.response().end();
		}
	}

	@Override
	public void stop(){
		if ( server != null ) server.close();        
	}

	private void handlePlainText(HttpServerRequest request) {
		request.response()
		.putHeader(HEADER_CONTENT, RESPONSE_TYPE_PLAIN).putHeader(HEADER_SERVER,  SERVER)
		.putHeader(HEADER_DATE, dateString).end(HELLO_WORLD_BUFFER);
	}

	private void handleJson(HttpServerRequest request) {
		Buffer buff = Buffer.buffer(Json.encode(Collections.singletonMap(TEXT_MESSAGE, HELLO_WORLD)));
		request.response().putHeader(HEADER_CONTENT, RESPONSE_TYPE_JSON).putHeader(HEADER_SERVER,  SERVER)
		.putHeader(HEADER_DATE, dateString).end(buff);
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

		public PostgresClient(Vertx vertx, JsonObject config) {
			PgClientOptions options = new PgClientOptions();
			options.setPoolsize(2); // 2 ?
			options.setDatabase(config.getString("database"));
			options.setHost(config.getString("host"));
			options.setUsername(config.getString("username"));
			options.setPassword(config.getString("password"));
			this.database = PgClient.create(vertx, options);
		}

		final void handle(final HttpServerRequest req) {
			Future<HttpServerResponse> responseFuture = Future.future();
			Future<ResultSet> resultFuture = Future.future();
			CompositeFuture fut = CompositeFuture.join(responseFuture, resultFuture);
			fut.setHandler(ar -> {
				HttpServerResponse resp = req.response();
				if (ar.succeeded()) {
					com.github.pgasync.ResultSet resultSet = resultFuture.result();
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
					resp.setStatusCode(500).end(ar.cause().getMessage());
				}
			});
			if (req.response() != null) {
				responseFuture.complete(req.response());
			} else {
				req.responseHandler(resp -> {
					responseFuture.complete(resp);
				});
			}
			database.query("SELECT id, randomnumber from WORLD where id = " + randomWorld(), resultFuture);
		}
	}

	public static void main(String[] args) throws Exception {
		JsonObject config = new JsonObject(new String(Files.readAllBytes(new File(args[0]).toPath())));
		int procs = Runtime.getRuntime().availableProcessors();
		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(WebServer.class.getName(), 
				new DeploymentOptions().setInstances(procs*2).setConfig(config), event -> {
					if (event.succeeded()) {
						logger.debug("Your Vert.x application is started!");
					} else {
						logger.error("Unable to start your application", event.cause());
					}
				});
	}
}

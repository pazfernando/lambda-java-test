package example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * Handler for requests to Lambda function.
 */
public class Hello implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent()
                .withHeaders(headers);

        // Obtener query parameters
        Map<String, String> queryParams = input.getQueryStringParameters();
        log(context, "El valor de queryParams es: " + queryParams);
        String dbType = queryParams != null && queryParams.containsKey("dbType") ? queryParams.get("dbType") : "all";
        log(context, "El valor de dbType es: " + dbType);

        String postgresResult = "";
        String docdbResult = "";
        String postgresProxyResult = "";
        String finalMessage = "";

        if (dbType.equalsIgnoreCase("none")) {
            return response
                .withStatusCode(200)
                .withBody("No se ejecuto nada.");
        }

        if (dbType.equalsIgnoreCase("postgres") || dbType.equalsIgnoreCase("all")) {
            String host = System.getenv("PG_HOST");
            postgresResult = testPostgresConnectivity(context, host);
        }
        
        if (dbType.equalsIgnoreCase("postgres-proxy") || dbType.equalsIgnoreCase("all")) {
            String host = System.getenv("PG_PROXY_ENDPOINT");
            postgresProxyResult = testPostgresConnectivity(context, host);
        }
        
        if (dbType.equalsIgnoreCase("documentdb") || dbType.equalsIgnoreCase("all")) {
            docdbResult = testDocumentDBConnectivity(context);
        }

        if (dbType.equalsIgnoreCase("postgres")) {
            finalMessage = String.format("dbType: %s, Resultado de PostgreSQL: %s", dbType, postgresResult);
        } else if (dbType.equalsIgnoreCase("postgres-proxy")) {
            finalMessage = String.format("dbType: %s, Resultado de PostgreSQL-proxy: %s", dbType, postgresProxyResult);
        } else if (dbType.equalsIgnoreCase("documentdb")) {
            finalMessage = String.format("dbType: %s, Resultado de DocumentDB: %s", dbType, docdbResult);
        } else if (dbType.equalsIgnoreCase("all")) {
            finalMessage = String.format(
                    "dbType: %s, Resultado de PostgreSQL: %s, Resultado de PostgreSQL-proxy: %s, Resultado de DocumentDB: %s",
                    dbType, postgresResult, postgresProxyResult, docdbResult);
        } else {
            return response
                .withStatusCode(500)
                .withBody("dbType not supported.");
        }

        return response
                .withStatusCode(200)
                .withBody(finalMessage);
    }

    // -------------------------------------------------------------------------
    // Subfunción para probar conectividad a PostgreSQL
    // -------------------------------------------------------------------------
    private String testPostgresConnectivity(Context context, String host) {
        // Variables de entorno para RDS PostgreSQL
        String port = System.getenv("PG_PORT");
        String dbName = System.getenv("PG_DBNAME");
        String user = System.getenv("PG_USER");
        String password = System.getenv("PG_PASSWORD");

        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", host, port, dbName);

        log(context, "Conectando a PostgreSQL: " + jdbcUrl);

        String resultado;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            conn = DriverManager.getConnection(jdbcUrl, user, password);
            log(context, "Conexión a PostgreSQL exitosa.");

            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT version();"); // Query de ejemplo

            if (rs.next()) {
                resultado = "Versión de PostgreSQL: " + rs.getString(1);
            } else {
                resultado = "No se pudo obtener la versión de PostgreSQL.";
            }

            log(context, "Resultado de query: " + resultado);

        } catch (SQLException e) {
            log(context, "Error de SQL: " + e.getMessage());
            return "Error al conectar: " + e.getMessage();
        } finally {
            // Cerrar ResultSet, Statement y Connection en orden inverso
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignored) {
                }
            }
        }

        return resultado;
    }

    // -------------------------------------------------------------------------
    // Subfunción para probar conectividad a Amazon DocumentDB
    // -------------------------------------------------------------------------
    private String testDocumentDBConnectivity(Context context) {
        // Variables de entorno para DocumentDB
        String docdbHost = System.getenv("DOCDB_HOST"); // p.e.
                                                        // docdb-cluster.cluster-xxxxxx.us-east-1.docdb.amazonaws.com
        String docdbPort = System.getenv("DOCDB_PORT"); // 27017, usualmente
        String docdbUser = System.getenv("DOCDB_USER");
        String docdbPassword = System.getenv("DOCDB_PASSWORD");
        String docdbDatabase = System.getenv("DOCDB_DBNAME"); // p.e. "admin" u otro

        // Construir la URI para DocumentDB (con SSL y replicaSet si corresponde)
        // Nota: Ajusta los parámetros ?ssl=true&replicaSet=rs0 según la configuración
        // de tu cluster
        String docdbUri = String.format(
                "mongodb://%s:%s@%s:%s/%s?ssl=true&replicaSet=rs0&retryWrites=false",
                docdbUser, docdbPassword, docdbHost, docdbPort, docdbDatabase);

        log(context, "Conectando a DocumentDB con URI: " + docdbUri);

        try {
            ConnectionString connectionString = new ConnectionString(docdbUri);
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .build();

            // Uso de try-with-resources para asegurar cierre de recursos
            try (MongoClient mongoClient = MongoClients.create(settings)) {
                MongoDatabase database = mongoClient.getDatabase(docdbDatabase);

                // Hacemos un "ping" sencillo con el comando `db.runCommand({ ping: 1 })`
                Document pingResult = database.runCommand(new Document("ping", 1));
                log(context, "Ping a DocumentDB -> " + pingResult.toJson());

                // Como verificación adicional, podríamos listar colecciones o la versión del
                // servidor
                Document buildInfo = database.runCommand(new Document("buildInfo", 1));
                return "DocumentDB buildInfo: " + buildInfo.toJson();
            }
        } catch (Exception e) {
            log(context, "Error conectando a DocumentDB: " + e.getMessage());
            return "Error conectando a DocumentDB: " + e.getMessage();
        }
    }

    // -------------------------------------------------------------------------
    // Helper para log
    // -------------------------------------------------------------------------
    private void log(Context context, String message) {
        if (context != null && context.getLogger() != null) {
            context.getLogger().log(message + "\n");
        } else {
            System.out.println(message);
        }
    }
}

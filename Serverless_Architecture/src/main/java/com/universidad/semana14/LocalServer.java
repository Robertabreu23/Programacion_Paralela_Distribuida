package com.universidad.semana14;

import com.fasterxml.jackson.databind.JsonNode;          // Arbol JSON
import com.fasterxml.jackson.databind.ObjectMapper;      // Parser/serializador JSON
import com.sun.net.httpserver.HttpServer;               // Servidor HTTP incluido en el JDK

import java.io.InputStream;                              // Cuerpo de la peticion
import java.net.InetSocketAddress;                       // Direccion de escucha
import java.nio.charset.StandardCharsets;                // UTF-8
import java.util.Map;                                    // Respuesta

/**
 * SERVIDOR HTTP LOCAL (equivalente al endpoint de Lambda)
 * -----------------------------------------------------------------------------
 * Expone POST /task con el MISMO contrato JSON que la funcion serverless.
 * Sirve para probar y tomar capturas sin necesidad de una cuenta de AWS.
 *
 * Uso:  java -cp target/actor-serverless.jar com.universidad.semana14.LocalServer
 *       curl -X POST http://localhost:8080/task -d '{"op":"sum","payload":"1,2,3"}'
 */
public class LocalServer {

    private static final ObjectMapper MAPPER = new ObjectMapper(); // Reutilizamos el mapper

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080")); // Puerto configurable
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);     // Creamos el servidor

        server.createContext("/task", exchange -> {       // Registramos el endpoint /task
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) { // Solo aceptamos POST
                exchange.sendResponseHeaders(405, -1);    // 405 Method Not Allowed
                exchange.close();
                return;
            }

            String op, payload;                           // Datos de la peticion
            try (InputStream is = exchange.getRequestBody()) {          // Leemos el cuerpo
                JsonNode body = MAPPER.readTree(is);                    // Lo parseamos como JSON
                op = body.path("op").asText("sum");                     // Operacion (por defecto sum)
                payload = body.path("payload").asText("");              // Carga util
            }

            Map<String, Object> result = ActorService.process(op, payload); // Delegamos en los actores
            byte[] out = MAPPER.writeValueAsBytes(result);                  // Serializamos la respuesta
            int status = Boolean.TRUE.equals(result.get("ok")) ? 200 : 500; // Codigo HTTP segun resultado

            exchange.getResponseHeaders().add("Content-Type", "application/json"); // Cabecera JSON
            exchange.sendResponseHeaders(status, out.length);            // Enviamos cabeceras
            exchange.getResponseBody().write(out);                       // Enviamos el cuerpo
            exchange.close();                                            // Cerramos la conexion
        });

        server.setExecutor(null);                          // Executor por defecto del JDK
        server.start();                                    // Arrancamos el servidor
        System.out.println("Servidor local escuchando en http://localhost:" + port + "/task");
    }
}

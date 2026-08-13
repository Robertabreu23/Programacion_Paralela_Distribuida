package com.universidad.semana14;

import com.amazonaws.services.lambda.runtime.Context;             // Contexto de ejecucion de Lambda
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;// Handler generico (entrada/salida como streams)
import com.fasterxml.jackson.databind.JsonNode;                   // Arbol JSON de Jackson
import com.fasterxml.jackson.databind.ObjectMapper;               // Serializador/deserializador JSON

import java.io.InputStream;                                       // Evento de entrada
import java.io.OutputStream;                                      // Respuesta de salida
import java.nio.charset.StandardCharsets;                         // UTF-8
import java.util.HashMap;                                         // Mapas de respuesta
import java.util.Map;                                             // Interfaz de mapa

/**
 * HANDLER DE AWS LAMBDA
 * -----------------------------------------------------------------------------
 * Punto de entrada HTTP. Acepta el evento de una Function URL / API Gateway
 * (formato v2.0) y tambien un JSON plano (util para pruebas con `sam local` o
 * la consola de AWS). Traduce HTTP -> mensaje de actor -> HTTP.
 */
public class LambdaHandler implements RequestStreamHandler {

    /** ObjectMapper es caro de crear: se reutiliza entre invocaciones. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws java.io.IOException {
        JsonNode event = MAPPER.readTree(input);                  // Parseamos el evento completo

        // API Gateway/Function URL envuelve la peticion del usuario en el campo "body".
        JsonNode body = event;                                     // Por defecto asumimos JSON plano
        if (event.has("body") && !event.get("body").isNull()) {    // Si viene envuelto...
            String raw = event.get("body").asText();               // ...extraemos el body como texto
            body = raw.isEmpty() ? MAPPER.createObjectNode() : MAPPER.readTree(raw); // y lo parseamos
        }

        String op = body.path("op").asText("sum");                 // Operacion pedida (por defecto "sum")
        String payload = body.path("payload").asText("");          // Datos de entrada (por defecto vacio)

        context.getLogger().log("Peticion recibida op=" + op + " payload=" + payload + "\n");

        Map<String, Object> result = ActorService.process(op, payload); // Delegamos en el pool de actores
        result.put("requestId", context.getAwsRequestId());             // Trazabilidad en CloudWatch

        // Construimos la respuesta en formato HTTP de API Gateway v2.
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", Boolean.TRUE.equals(result.get("ok")) ? 200 : 500); // 200 si ok, 500 si fallo
        response.put("headers", Map.of("Content-Type", "application/json"));           // Cabeceras JSON
        response.put("body", MAPPER.writeValueAsString(result));                       // Cuerpo serializado

        output.write(MAPPER.writeValueAsBytes(response));           // Escribimos la respuesta al stream
        output.flush();                                             // Nos aseguramos de vaciar el buffer
    }
}

package com.universidad.semana14;

import akka.actor.typed.ActorSystem;                     // Raiz del sistema de actores
import akka.actor.typed.javadsl.AskPattern;              // Permite preguntar a un actor desde fuera del sistema

import java.time.Duration;                               // Timeouts
import java.util.HashMap;                                // Mapa para construir la respuesta JSON
import java.util.Map;                                    // Interfaz de mapa
import java.util.concurrent.CompletionStage;             // Futuro asincrono de Java
import java.util.concurrent.TimeUnit;                    // Unidades de tiempo

/**
 * SERVICIO DE ACTORES (capa puente)
 * -----------------------------------------------------------------------------
 * Mantiene un unico ActorSystem por contenedor de Lambda (patron singleton).
 * En serverless esto es clave: crear un ActorSystem cuesta cientos de ms, asi que
 * se crea en el "cold start" y se REUTILIZA en todas las invocaciones "calientes".
 */
public final class ActorService {

    /** Instancia unica del sistema de actores; volatile por el doble chequeo. */
    private static volatile ActorSystem<Supervisor.Command> system;

    /** Numero de workers del pool; configurable por variable de entorno en Lambda. */
    private static final int POOL_SIZE =
            Integer.parseInt(System.getenv().getOrDefault("WORKER_POOL_SIZE", "3"));

    /** Timeout total de una peticion vista desde el handler HTTP. */
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

    private ActorService() {}                            // Clase de utilidad: no se instancia

    /** Devuelve el ActorSystem, creandolo solo la primera vez (thread-safe). */
    public static ActorSystem<Supervisor.Command> system() {
        if (system == null) {                            // Primer chequeo sin bloquear (caso comun: ya existe)
            synchronized (ActorService.class) {          // Bloqueo solo en el arranque en frio
                if (system == null) {                    // Segundo chequeo dentro del bloqueo
                    system = ActorSystem.create(Supervisor.create(POOL_SIZE), "actor-serverless");
                    system.log().info("[SISTEMA] ActorSystem iniciado (cold start) con pool={}", POOL_SIZE);
                }
            }
        }
        return system;                                   // Devolvemos siempre la misma instancia
    }

    /**
     * Ejecuta una tarea en el pool de actores y devuelve un mapa listo para
     * serializar a JSON. Nunca lanza excepciones: los fallos se traducen a JSON.
     */
    public static Map<String, Object> process(String op, String payload) {
        Map<String, Object> out = new HashMap<>();       // Respuesta que devolveremos
        out.put("op", op);                               // Eco de la operacion pedida
        out.put("payload", payload);                     // Eco de la entrada
        long t0 = System.nanoTime();                     // Marca de tiempo para medir latencia

        try {
            // AskPattern.ask lanza la peticion al supervisor y devuelve un futuro.
            CompletionStage<Worker.Result> future = AskPattern.ask(
                    system(),                                                   // Actor destino (el supervisor)
                    replyTo -> new Supervisor.Submit(op, payload, replyTo),      // Mensaje a enviar
                    REQUEST_TIMEOUT,                                             // Timeout de la peticion
                    system().scheduler());                                       // Scheduler que vigila el timeout

            // Esperamos el resultado con un limite duro: el bloqueo ocurre en el hilo
            // de Lambda, no dentro de un actor, por lo que no bloquea el dispatcher.
            Worker.Result r = future.toCompletableFuture().get(6, TimeUnit.SECONDS);

            out.put("ok", r.ok);                          // Estado logico de la tarea
            out.put("worker", r.worker);                  // Worker que la atendio
            out.put("result", r.value);                   // Resultado o mensaje de error controlado
        } catch (Exception e) {                           // Timeout, interrupcion o error inesperado
            out.put("ok", false);                         // Marcamos fallo
            out.put("worker", "n/a");                     // No hubo worker que respondiera
            out.put("result", "Error de ejecucion: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }

        out.put("latencyMs", (System.nanoTime() - t0) / 1_000_000); // Latencia observada en milisegundos
        return out;                                       // Mapa que el handler convierte a JSON
    }
}

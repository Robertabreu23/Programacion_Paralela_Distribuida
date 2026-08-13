package com.universidad.semana14;

import akka.actor.typed.ActorRef;                        // Referencia a otro actor
import akka.actor.typed.Behavior;                        // Comportamiento de un actor
import akka.actor.typed.SupervisorStrategy;              // Estrategias de supervision (restart, resume, stop)
import akka.actor.typed.javadsl.AbstractBehavior;        // Base OO para actores
import akka.actor.typed.javadsl.ActorContext;            // Contexto (spawn, ask, log)
import akka.actor.typed.javadsl.Behaviors;               // Fabrica de comportamientos
import akka.actor.typed.javadsl.Receive;                 // Tabla de mensajes

import java.io.Serializable;                             // Mensajes serializables
import java.time.Duration;                               // Tiempos de espera y backoff
import java.util.ArrayList;                              // Lista de hijos
import java.util.List;                                   // Interfaz de lista

/**
 * ACTOR SUPERVISOR
 * -----------------------------------------------------------------------------
 * Crea un grupo (pool) de workers hijos, reparte el trabajo entre ellos con
 * round-robin y define la ESTRATEGIA DE SUPERVISION: si un hijo lanza una
 * excepcion, Akka lo reinicia automaticamente sin tumbar al resto del sistema.
 */
public class Supervisor extends AbstractBehavior<Supervisor.Command> {

    /** Interfaz marcadora de los mensajes del supervisor. */
    public interface Command extends Serializable {}

    /** Peticion de trabajo que llega desde el handler HTTP/Lambda. */
    public static final class Submit implements Command {
        public final String op;                           // Operacion solicitada
        public final String payload;                      // Datos de entrada
        public final ActorRef<Worker.Result> replyTo;     // A quien responder el resultado final

        public Submit(String op, String payload, ActorRef<Worker.Result> replyTo) {
            this.op = op;                                 // Operacion
            this.payload = payload;                       // Carga util
            this.replyTo = replyTo;                       // Destinatario final
        }
    }

    /** Mensaje interno: respuesta (o fallo) de un worker devuelta por el patron ask. */
    private static final class WorkerReply implements Command {
        final Worker.Result result;                       // Resultado si todo fue bien (null si hubo fallo)
        final Throwable failure;                          // Excepcion/timeout si el worker murio (null si fue bien)
        final String workerName;                          // Worker al que se le pidio el trabajo
        final ActorRef<Worker.Result> replyTo;            // Cliente original que espera respuesta

        WorkerReply(Worker.Result result, Throwable failure, String workerName, ActorRef<Worker.Result> replyTo) {
            this.result = result;                         // Resultado del worker
            this.failure = failure;                       // Causa del fallo, si la hubo
            this.workerName = workerName;                 // Nombre del worker implicado
            this.replyTo = replyTo;                       // Cliente original
        }
    }

    private final List<ActorRef<Worker.Command>> workers = new ArrayList<>(); // Pool de hijos
    private int next = 0;                                                     // Indice round-robin
    // Si un worker muere por una excepcion no llega a responder: el ask vence y
    // asi detectamos el fallo rapidamente para devolver un error controlado.
    private final Duration askTimeout = Duration.ofSeconds(1);                // Tiempo maximo de espera por tarea

    /** Fabrica del supervisor: recibe cuantos workers debe crear. */
    public static Behavior<Command> create(int poolSize) {
        return Behaviors.setup(ctx -> new Supervisor(ctx, poolSize)); // setup ejecuta el constructor al arrancar
    }

    /** Constructor: crea el pool de workers supervisados. */
    private Supervisor(ActorContext<Command> context, int poolSize) {
        super(context);                                              // Guarda el contexto
        for (int i = 1; i <= poolSize; i++) {                        // Creamos poolSize workers
            // Behaviors.supervise envuelve el comportamiento del worker con una estrategia:
            // ante CUALQUIER excepcion se reinicia el actor con backoff exponencial
            // (0.2s, 0.4s, 0.8s... hasta 2s) para no saturar el sistema.
            Behavior<Worker.Command> supervised =
                    Behaviors.supervise(Worker.create())
                             .onFailure(Exception.class,
                                     SupervisorStrategy.restartWithBackoff(
                                             Duration.ofMillis(200),  // Espera minima antes de reiniciar
                                             Duration.ofSeconds(2),   // Espera maxima
                                             0.2));                   // Factor de aleatoriedad (jitter)

            ActorRef<Worker.Command> w = context.spawn(supervised, "worker-" + i); // Nace el actor hijo
            workers.add(w);                                                        // Lo guardamos en el pool
        }
        context.getLog().info("[SUPERVISOR] Pool creado con {} workers supervisados", poolSize);
    }

    /** Mensajes que sabe manejar el supervisor. */
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Submit.class, this::onSubmit)             // Peticion externa de trabajo
                .onMessage(WorkerReply.class, this::onWorkerReply)   // Respuesta interna del worker
                .build();
    }

    /** Reparte la tarea al siguiente worker (round-robin) usando el patron ask. */
    private Behavior<Command> onSubmit(Submit msg) {
        ActorRef<Worker.Command> worker = workers.get(next);         // Elegimos worker segun el indice
        next = (next + 1) % workers.size();                          // Avanzamos el turno de forma circular
        String workerName = worker.path().name();                    // Nombre para trazas y mensajes de error

        getContext().getLog().info("[SUPERVISOR] Despachando op='{}' a {}", msg.op, workerName);

        // ask: enviamos la tarea y esperamos la respuesta de forma ASINCRONA.
        // Si el worker muere (excepcion) o tarda mas de askTimeout, llega failure != null.
        getContext().ask(
                Worker.Result.class,                                  // Tipo de la respuesta esperada
                worker,                                               // Actor destino
                askTimeout,                                           // Timeout de la peticion
                replyTo -> new Worker.Task(msg.op, msg.payload, replyTo), // Construccion del mensaje
                (res, thr) -> new WorkerReply(res, thr, workerName, msg.replyTo) // Adaptacion a mensaje propio
        );
        return this;                                                  // El supervisor sigue disponible (no se bloquea)
    }

    /** Procesa la respuesta del worker y contesta al cliente original. */
    private Behavior<Command> onWorkerReply(WorkerReply msg) {
        if (msg.failure != null) {                                    // Caso de fallo: excepcion o timeout
            getContext().getLog().error("[SUPERVISOR] {} fallo ({}). Akka lo reinicia; respondemos error controlado",
                    msg.workerName, msg.failure.getClass().getSimpleName());
            // Tolerancia a fallos: el cliente SIEMPRE recibe una respuesta, nunca se queda colgado.
            msg.replyTo.tell(new Worker.Result(false, msg.workerName,
                    "El worker fallo y fue reiniciado por el supervisor: " + msg.failure.getMessage()));
        } else {                                                      // Caso correcto
            getContext().getLog().info("[SUPERVISOR] Resultado de {} -> {}", msg.workerName, msg.result.value);
            msg.replyTo.tell(msg.result);                             // Reenviamos el resultado al cliente
        }
        return this;                                                  // Mantenemos el comportamiento
    }
}

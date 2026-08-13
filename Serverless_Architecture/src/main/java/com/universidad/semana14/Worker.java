package com.universidad.semana14;

import akka.actor.typed.Behavior;                       // Tipo que describe "como se comporta" un actor
import akka.actor.typed.ActorRef;                       // Direccion (referencia) de un actor para enviarle mensajes
import akka.actor.typed.PreRestart;                     // Senal que Akka envia justo antes de reiniciar un actor
import akka.actor.typed.javadsl.AbstractBehavior;       // Clase base para escribir actores en estilo orientado a objetos
import akka.actor.typed.javadsl.ActorContext;           // Contexto del actor: log, self, spawn de hijos, ask, etc.
import akka.actor.typed.javadsl.Behaviors;              // Fabrica de comportamientos (setup, receive, etc.)
import akka.actor.typed.javadsl.Receive;                // Tabla de mensajes que el actor sabe manejar

import java.util.Arrays;                                // Utilidades para trocear la carga "1,2,3"
import java.io.Serializable;                            // Marca los mensajes como serializables (buena practica en Akka)

/**
 * ACTOR WORKER
 * -----------------------------------------------------------------------------
 * Es el actor "trabajador": recibe una tarea, la procesa y responde.
 * No comparte memoria con nadie; toda la comunicacion es por mensajes inmutables,
 * por eso NO hacen falta locks ni sincronizacion.
 */
public class Worker extends AbstractBehavior<Worker.Command> {

    /** Interfaz marcadora: todo mensaje que este actor acepta implementa Command. */
    public interface Command extends Serializable {}

    /** Mensaje de entrada: una tarea a procesar. Es inmutable (campos final). */
    public static final class Task implements Command {
        public final String op;                          // Operacion pedida: "sum", "upper", "reverse" o "fail"
        public final String payload;                     // Datos de entrada, por ejemplo "1,2,3" o "hola"
        public final ActorRef<Result> replyTo;           // A quien hay que contestarle el resultado

        public Task(String op, String payload, ActorRef<Result> replyTo) {
            this.op = op;                                // Guardamos la operacion
            this.payload = payload;                      // Guardamos la carga util
            this.replyTo = replyTo;                      // Guardamos el destinatario de la respuesta
        }
    }

    /** Mensaje de salida: el resultado del procesamiento. Tambien inmutable. */
    public static final class Result implements Serializable {
        public final boolean ok;                         // true si la tarea se proceso bien
        public final String worker;                      // Nombre del actor que la proceso (trazabilidad)
        public final String value;                       // Valor calculado o mensaje de error

        public Result(boolean ok, String worker, String value) {
            this.ok = ok;                                // Estado de la operacion
            this.worker = worker;                        // Identidad del worker
            this.value = value;                          // Resultado o descripcion del fallo
        }
    }

    /** Fabrica: devuelve el Behavior inicial del worker (aun no es un actor vivo). */
    public static Behavior<Command> create() {
        return Behaviors.setup(Worker::new);             // setup ejecuta el constructor cuando el actor arranca
    }

    /** Constructor privado: solo lo invoca Behaviors.setup con el contexto ya creado. */
    private Worker(ActorContext<Command> context) {
        super(context);                                  // Guarda el contexto en la clase base
        context.getLog().info("[WORKER] {} creado y listo", context.getSelf().path().name());
    }

    /** Declara que mensajes y senales sabe manejar este actor. */
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()                       // Constructor de la tabla de despacho
                .onMessage(Task.class, this::onTask)     // Cuando llegue un Task -> metodo onTask
                .onSignal(PreRestart.class, this::onPreRestart) // Cuando el supervisor lo reinicie -> log
                .build();                                // Construye el Receive
    }

    /** Manejador principal: procesa una tarea y responde al solicitante. */
    private Behavior<Command> onTask(Task task) {
        String me = getContext().getSelf().path().name();            // Nombre del worker actual (worker-1, worker-2...)
        getContext().getLog().info("[WORKER] {} procesando op='{}' payload='{}'", me, task.op, task.payload);

        // Fallo intencionado para demostrar la supervision: lanzamos una excepcion.
        if ("fail".equalsIgnoreCase(task.op)) {                      // Si la operacion pedida es "fail"...
            throw new IllegalStateException("Fallo intencionado en " + me); // ...rompemos el actor a proposito
        }

        String value;                                                // Aqui guardamos el resultado calculado
        switch (task.op == null ? "" : task.op.toLowerCase()) {       // Elegimos la operacion (null-safe)
            case "sum":                                              // Suma de una lista "1,2,3"
                value = String.valueOf(
                        Arrays.stream(task.payload.split(","))       // Partimos por comas
                              .map(String::trim)                     // Quitamos espacios
                              .filter(s -> !s.isEmpty())             // Descartamos vacios
                              .mapToLong(Long::parseLong)            // Convertimos a numero
                              .sum());                               // Sumamos
                break;
            case "upper":                                            // Pasa el texto a mayusculas
                value = task.payload.toUpperCase();
                break;
            case "reverse":                                          // Invierte el texto
                value = new StringBuilder(task.payload).reverse().toString();
                break;
            default:                                                 // Operacion desconocida: error controlado
                task.replyTo.tell(new Result(false, me, "Operacion no soportada: " + task.op));
                return this;                                         // Seguimos con el mismo comportamiento
        }

        task.replyTo.tell(new Result(true, me, value));              // Enviamos la respuesta (fire and forget)
        getContext().getLog().info("[WORKER] {} respondio '{}'", me, value);
        return this;                                                 // El actor mantiene su comportamiento
    }

    /** Se ejecuta cuando el supervisor decide reiniciar este worker tras un fallo. */
    private Behavior<Command> onPreRestart(PreRestart signal) {
        getContext().getLog().warn("[WORKER] {} recibio PreRestart: el supervisor lo esta reiniciando",
                getContext().getSelf().path().name());
        return this;                                                 // Akka creara una instancia limpia despues
    }
}

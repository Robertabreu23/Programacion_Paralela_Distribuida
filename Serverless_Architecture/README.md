# Modelo de Actores y Arquitecturas Serverless — Semana 14

**Microservicio con Akka Typed (Java 17) desplegado como función AWS Lambda con endpoint HTTP.**

| | |
|---|---|
| **Autor** | Robert G. Abreu Delgado |
| **Asignatura** | Arquitectura de Software |
| **Actividad** | Semana 14 (4–10 de agosto) |
| **Vídeo** | *(pendiente: añadir enlace aquí — guion en [docs/guion-video.md](docs/guion-video.md))* |
| **Repositorio** | *(pendiente: añadir URL de GitHub)* |

Este README es a la vez la documentación del proyecto y el informe de la actividad.

---

## Índice

1. [Introducción y objetivos](#1-introducción-y-objetivos)
2. [El Modelo de Actores](#2-el-modelo-de-actores)
3. [Diseño del microservicio](#3-diseño-del-microservicio)
4. [Arquitectura serverless](#4-arquitectura-serverless)
5. [Cómo ejecutar y desplegar](#5-cómo-ejecutar-y-desplegar)
6. [Presentación del código comentado](#6-presentación-del-código-comentado)
7. [Gestión de errores y tolerancia a fallos](#7-gestión-de-errores-y-tolerancia-a-fallos)
8. [Ejecución y evidencias](#8-ejecución-y-evidencias)
9. [Análisis: actores frente a funciones sin estado](#9-análisis-actores-frente-a-funciones-sin-estado)
10. [Conclusiones](#10-conclusiones)
11. [Referencias](#11-referencias)

---

## 1. Introducción y objetivos

Este proyecto construye un microservicio basado en el **Modelo de Actores** y lo despliega en una
**arquitectura serverless**. El sistema recibe peticiones HTTP en formato JSON, las convierte en
mensajes dirigidos a un conjunto de actores supervisados y devuelve el resultado del procesamiento,
garantizando que el fallo de un actor no interrumpa el servicio.

Objetivos concretos:

- Comprender y aplicar el Modelo de Actores como paradigma de concurrencia y distribución.
- Implementar un actor supervisor que gestione un grupo de actores worker.
- Configurar un patrón de supervisión que reinicie automáticamente al worker que falla.
- Encapsular el microservicio en una función serverless con endpoint HTTP.
- Manejar ejecución asíncrona y los fallos típicos de una arquitectura serverless.
- Comparar el uso de actores frente a funciones sin estado (serverless puro).

### 1.1 Tecnologías utilizadas

| Componente | Tecnología | Justificación |
|---|---|---|
| Modelo de actores | Akka Typed 2.6.21 (Java 17) | Última versión con licencia Apache 2.0; API tipada que evita errores de mensajes en tiempo de compilación |
| Runtime serverless | AWS Lambda (`java17`) | Runtime gestionado, escalado automático y pago por uso |
| Exposición HTTP | Lambda Function URL / API Gateway HTTP API | Endpoint HTTPS sin servidores que administrar |
| Infraestructura como código | AWS SAM (`template.yaml`) | Despliegue reproducible con un solo comando |
| Serialización | Jackson 2.17 | Conversión JSON ↔ objetos del dominio |
| Empaquetado | Maven + `maven-shade-plugin` | Fat jar autocontenido exigido por Lambda |

---

## 2. El Modelo de Actores

El Modelo de Actores, formulado por Carl Hewitt en 1973 y popularizado por Erlang/OTP y Akka,
propone que la unidad básica de computación concurrente sea el **actor**: una entidad que posee
estado privado, un buzón (*mailbox*) de mensajes y un comportamiento. Un actor solo puede hacer tres
cosas al recibir un mensaje: enviar mensajes a otros actores, crear nuevos actores y decidir cómo se
comportará ante el siguiente mensaje.

### 2.1 Propiedades fundamentales

- **Aislamiento**: ningún actor accede a la memoria de otro. El estado es privado, por lo que
  desaparecen las condiciones de carrera.
- **Sin locks**: los mensajes de un mismo actor se procesan de uno en uno y en orden, lo que da
  exclusión mutua sin sincronización explícita ni riesgo de interbloqueo.
- **Asincronía**: el envío de un mensaje (`tell`) no bloquea al emisor; el receptor lo procesará
  cuando le llegue su turno en el mailbox.
- **Transparencia de ubicación**: un `ActorRef` puede apuntar a un actor local o remoto; el código de
  negocio no cambia, lo que facilita la distribución.
- **Jerarquía y supervisión**: todo actor tiene un padre que decide qué hacer si el hijo falla; es la
  base de la filosofía *let it crash*.

### 2.2 Supervisión y filosofía «let it crash»

En lugar de programar defensivamente cada posible excepción dentro de la lógica de negocio, el Modelo
de Actores separa el código de trabajo del código de recuperación. Si un actor entra en un estado
inválido, se le deja fallar; su supervisor decide entonces entre:

| Estrategia | Efecto |
|---|---|
| `restart` | Se descarta el estado corrupto y se recrea el actor limpio |
| `resume` | Se conserva el estado y se ignora el error |
| `stop` | Se detiene el actor definitivamente |
| Escalar | El fallo sube al supervisor del nivel superior |

En este proyecto se usa **restart con backoff exponencial**, que además evita que un fallo persistente
provoque reinicios en bucle que saturen la CPU.

---

## 3. Diseño del microservicio

### 3.1 Diagrama de actores

```
                       +-------------------------------+
   POST /task          |      AWS Lambda (JVM)         |
   {"op","payload"}    |                               |
   ------------------> |  LambdaHandler (HTTP -> msg)  |
                       |            |                  |
                       |            v  AskPattern.ask  |
                       |  +-------------------------+  |
                       |  |   ActorSystem raiz      |  |
                       |  |  "actor-serverless"     |  |
                       |  |          |              |  |
                       |  |          v              |  |
                       |  |  +-----------------+    |  |
                       |  |  |   SUPERVISOR    |    |  |
                       |  |  | round-robin +   |    |  |
                       |  |  | restartWith-    |    |  |
                       |  |  | Backoff         |    |  |
                       |  |  +--+----+-----+---+    |  |
                       |  |     |    |     |        |  |
                       |  |     v    v     v        |  |
                       |  |  [w-1] [w-2] [w-3]      |  |
                       |  |   Actores worker        |  |
                       |  +-------------------------+  |
   <------------------ |  Respuesta JSON + codigo HTTP |
   {"ok","result"}     +-------------------------------+
```

La jerarquía es: usuario (raíz del ActorSystem) → Supervisor → `worker-1..worker-N`. Como los workers
son hijos del supervisor, este es responsable de su ciclo de vida y aplica la estrategia de
supervisión definida al crearlos.

### 3.2 Flujo de mensajes de una petición correcta

```
Cliente          LambdaHandler        Supervisor            worker-N
   |  POST JSON       |                   |                     |
   |----------------->|                   |                     |
   |                  | ask(Submit)       |                     |
   |                  |------------------>|                     |
   |                  |                   | ask(Task)           |
   |                  |                   |-------------------->|
   |                  |                   |                     | procesa
   |                  |                   |   Result(ok=true)   |
   |                  |                   |<--------------------|
   |                  |  Worker.Result    |                     |
   |                  |<------------------|                     |
   |  200 + JSON      |                   |                     |
   |<-----------------|                   |                     |
```

### 3.3 Flujo cuando un worker falla

```
Cliente          Supervisor           worker-1            Akka (supervision)
   |  op="fail"       |                   |                     |
   |----------------->| ask(Task)         |                     |
   |                  |------------------>| throw IllegalState  |
   |                  |                   |-------------------->|
   |                  |                   |    PreRestart       |
   |                  |                   |<--------------------|
   |                  |                   |  nueva instancia    |
   |                  |                   |<--------------------|
   |                  | AskTimeout (1s)   |    (worker-1 vivo)  |
   |                  |<------------------|                     |
   |  500 + JSON      |                   |                     |
   |  "worker         |                   |                     |
   |   reiniciado"    |                   |                     |
   |<-----------------|                   |                     |
```

Puntos clave del diseño: el worker que falla no responde, por lo que el `ask` del supervisor vence a
1 segundo y este devuelve un **error controlado** al cliente; en paralelo, Akka ya ha recreado la
instancia del worker, que queda disponible para la siguiente petición. El cliente nunca se queda
esperando indefinidamente y el resto de workers no se ven afectados.

### 3.4 Contrato de la API

`POST /task`

| Campo | Dirección | Descripción |
|---|---|---|
| `op` | entrada | Operación: `sum`, `upper`, `reverse` o `fail` (fallo intencionado) |
| `payload` | entrada | Datos: lista `"1,2,3"` para `sum` o texto para las demás |
| `ok` | salida | `true` si la tarea se completó correctamente |
| `worker` | salida | Actor que atendió la petición (trazabilidad del round-robin) |
| `result` | salida | Resultado calculado o descripción del error controlado |
| `latencyMs` | salida | Latencia medida dentro de la función |
| `requestId` | salida | Identificador de la invocación de Lambda (CloudWatch) |

Código HTTP `200` si `ok=true`, `500` si hubo error controlado.

---

## 4. Arquitectura serverless

*Serverless* no significa que no existan servidores, sino que su aprovisionamiento, parcheado y
escalado dejan de ser responsabilidad del desarrollador. El proveedor ejecuta el código en
contenedores efímeros que se crean cuando llega una petición y se destruyen cuando dejan de usarse.
El modelo dominante es **FaaS** (*Function as a Service*): se despliega una función, se asocia a un
disparador (una petición HTTP, un evento de cola, un fichero subido) y se paga únicamente por el
tiempo de cómputo consumido, medido en milisegundos.

### 4.1 Ventajas

- **Escalado automático y elástico**: de cero a miles de ejecuciones concurrentes sin configurar
  grupos de autoescalado ni balanceadores.
- **Cero mantenimiento de servidores**: no hay sistema operativo, ni parches, ni capacidad ociosa.
- **Pago por uso real**: si no hay peticiones, el coste es cero; se factura por invocación y por
  GB-segundo consumido.
- **Alta disponibilidad por defecto**: el proveedor replica la ejecución en varias zonas de
  disponibilidad.
- **Despliegue rápido y granular**: cada función se versiona y despliega de forma independiente.

### 4.2 Limitaciones

- **Arranque en frío** (*cold start*): la primera invocación debe crear el contenedor e inicializar la
  JVM y el ActorSystem. En este proyecto se midieron **~195 ms** de latencia adicional en la primera
  petición frente a 0–2 ms en las siguientes.
- **Sin estado garantizado**: el contenedor puede destruirse en cualquier momento, por lo que no se
  puede confiar en la memoria entre invocaciones.
- **Límites duros**: tiempo máximo de ejecución (15 minutos en AWS Lambda), tamaño del paquete y
  memoria configurable.
- **Dependencia del proveedor** (*vendor lock-in*): el formato del evento y las herramientas son
  específicos de cada nube.
- **Observabilidad y depuración más complejas**: no hay un proceso permanente al que conectarse; todo
  se diagnostica mediante logs y trazas.

### 4.3 Configuración del entorno ([`template.yaml`](template.yaml))

```yaml
Resources:
  ActorFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: com.universidad.semana14.LambdaHandler::handleRequest
      CodeUri: target/actor-serverless.jar
      Runtime: java17
      MemorySize: 1024        # más memoria => más CPU => menor cold start de la JVM
      Timeout: 30
      Environment:
        Variables:
          WORKER_POOL_SIZE: '3'
      FunctionUrlConfig:
        AuthType: NONE        # endpoint HTTPS público (demo académica)
      Events:
        ApiEvent:
          Type: HttpApi
          Properties: { Path: /task, Method: POST }
```

El despliegue completo se automatiza en [`deploy.sh`](deploy.sh): compila el fat jar con Maven,
ejecuta `sam deploy` para crear o actualizar el stack de CloudFormation y finalmente consulta la URL
pública de la función para probarla con `curl`.

### 4.4 Adaptación del Modelo de Actores al entorno serverless

Un `ActorSystem` es un recurso caro (crea dispatchers y pools de hilos). Crearlo en cada invocación
arruinaría el rendimiento, por lo que `ActorService` lo mantiene en una variable estática con
inicialización perezosa y doble chequeo: se construye durante el arranque en frío y se **reutiliza**
en todas las invocaciones calientes del mismo contenedor. Además, el bloqueo a la espera del resultado
se hace en el hilo del handler de Lambda y nunca dentro de un actor, de forma que no se bloquea el
dispatcher de Akka.

---

## 5. Cómo ejecutar y desplegar

### 5.1 Requisitos

- JDK 17 o superior
- Maven 3.8+ (`brew install maven` en macOS)
- Solo para desplegar: AWS CLI configurado y AWS SAM CLI

### 5.2 Ejecución local

```bash
# Compilar el fat jar
mvn clean package

# Opción A: demo por consola (muestra el fallo y el reinicio del worker)
./run-local.sh demo

# Opción B: endpoint HTTP local, mismo contrato que Lambda
./run-local.sh server
curl -X POST http://localhost:8080/task \
     -H 'Content-Type: application/json' \
     -d '{"op":"sum","payload":"10,20,30,40"}'
```

### 5.3 Despliegue en AWS Lambda

```bash
aws configure          # 1. credenciales
./deploy.sh            # 2. compila y crea el stack de CloudFormation

# 3. probar el endpoint que devuelve el script
curl -X POST https://<id>.lambda-url.us-east-1.on.aws/ \
     -H 'Content-Type: application/json' \
     -d '{"op":"upper","payload":"hola serverless"}'
```

Prueba local del evento Lambda sin desplegar y consulta de logs:

```bash
sam local invoke ActorFunction -e examples/event-apigateway.json
sam logs -n actor-serverless-semana14 --tail
```

### 5.4 Configuración

| Variable | Por defecto | Descripción |
|---|---|---|
| `WORKER_POOL_SIZE` | `3` | Número de actores worker del pool |
| `PORT` | `8080` | Puerto del servidor local |

---

## 6. Presentación del código comentado

### 6.1 Estructura del repositorio

```
.
|-- pom.xml                  Dependencias y empaquetado fat jar
|-- template.yaml            Infraestructura serverless (AWS SAM)
|-- deploy.sh                Compilacion + despliegue en AWS
|-- run-local.sh             Ejecucion local: demo o servidor HTTP
|-- examples/                Peticiones JSON de ejemplo
|-- docs/                    Logs de ejecucion y guion del video
`-- src/main/java/com/universidad/semana14/
    |-- Worker.java          Actor worker (procesa y falla a proposito)
    |-- Supervisor.java      Actor supervisor (pool + reinicio automatico)
    |-- ActorService.java    ActorSystem singleton + patron ask
    |-- LambdaHandler.java   Handler HTTP de AWS Lambda
    |-- LocalServer.java     Endpoint HTTP local equivalente (JDK)
    `-- LocalDemo.java       Demo por consola de la supervision
```

### 6.2 [`Worker.java`](src/main/java/com/universidad/semana14/Worker.java) — el actor trabajador

```java
public class Worker extends AbstractBehavior<Worker.Command> {      // (1)
  public interface Command extends Serializable {}                  // (2)

  public static final class Task implements Command {               // (3)
    public final String op; public final String payload;
    public final ActorRef<Result> replyTo;                          // (4)
  }
  public static final class Result implements Serializable {        // (5)
    public final boolean ok; public final String worker; public final String value;
  }

  public static Behavior<Command> create() {                        // (6)
    return Behaviors.setup(Worker::new);
  }

  @Override public Receive<Command> createReceive() {               // (7)
    return newReceiveBuilder()
      .onMessage(Task.class, this::onTask)
      .onSignal(PreRestart.class, this::onPreRestart)               // (8)
      .build();
  }

  private Behavior<Command> onTask(Task task) {
    if ("fail".equalsIgnoreCase(task.op))                           // (9)
      throw new IllegalStateException("Fallo intencionado en " + me);
    ...
    task.replyTo.tell(new Result(true, me, value));                 // (10)
    return this;                                                    // (11)
  }
}
```

1. `AbstractBehavior` es el estilo orientado a objetos de Akka Typed: la clase encapsula el estado
   privado del actor y su tipo genérico fija qué mensajes acepta.
2. Todos los mensajes implementan una interfaz marcadora común; el compilador impide enviar un
   mensaje que el actor no sepa manejar.
3. `Task` es inmutable (campos `final`): esto es lo que hace seguro compartirlo entre hilos.
4. `replyTo` viaja **dentro** del mensaje. Es el patrón de respuesta de Akka Typed: no existe un
   *sender* implícito, el emisor declara explícitamente a dónde quiere la respuesta.
5. `Result` también es inmutable e incluye el nombre del worker para trazabilidad.
6. `Behaviors.setup` difiere la construcción hasta que el actor arranca realmente, entregando el
   `ActorContext` (log, self, spawn).
7. `createReceive` define la tabla de despacho: qué método atiende cada tipo de mensaje.
8. `onSignal` captura señales del ciclo de vida. `PreRestart` se registra en el log y demuestra en la
   práctica que la supervisión está actuando.
9. Fallo intencionado exigido por el enunciado: la excepción **no se captura**, se deja propagar
   (*let it crash*) para que el supervisor decida.
10. `tell` envía la respuesta de forma asíncrona (*fire and forget*) sin bloquear.
11. Devolver `this` significa «sigo comportándome igual»; un actor podría devolver otro `Behavior`
    para cambiar de estado (máquina de estados sin variables compartidas).

### 6.3 [`Supervisor.java`](src/main/java/com/universidad/semana14/Supervisor.java) — pool y estrategia de supervisión

```java
private Supervisor(ActorContext<Command> context, int poolSize) {
  for (int i = 1; i <= poolSize; i++) {
    Behavior<Worker.Command> supervised =
      Behaviors.supervise(Worker.create())                          // (1)
               .onFailure(Exception.class,
                  SupervisorStrategy.restartWithBackoff(            // (2)
                      Duration.ofMillis(200), Duration.ofSeconds(2), 0.2));
    workers.add(context.spawn(supervised, "worker-" + i));          // (3)
  }
}

private Behavior<Command> onSubmit(Submit msg) {
  ActorRef<Worker.Command> worker = workers.get(next);              // (4)
  next = (next + 1) % workers.size();
  getContext().ask(                                                 // (5)
      Worker.Result.class, worker, askTimeout,
      replyTo -> new Worker.Task(msg.op, msg.payload, replyTo),
      (res, thr) -> new WorkerReply(res, thr, workerName, msg.replyTo));
  return this;                                                      // (6)
}

private Behavior<Command> onWorkerReply(WorkerReply msg) {
  if (msg.failure != null) {                                        // (7)
      msg.replyTo.tell(new Worker.Result(false, msg.workerName,
          "El worker fallo y fue reiniciado por el supervisor: " + msg.failure.getMessage()));
  } else {
      msg.replyTo.tell(msg.result);                                 // (8)
  }
  return this;
}
```

1. `Behaviors.supervise` envuelve el comportamiento del worker con una política de fallos: es una
   decoración, el código del worker no se entera.
2. `restartWithBackoff` reinicia el actor esperando entre 200 ms y 2 s, con un 20 % de *jitter*
   aleatorio; así un fallo repetitivo no genera un bucle de reinicios que consuma la CPU.
3. `spawn` crea el actor hijo con nombre estable (`worker-1`, `worker-2`, …), lo que permite
   identificarlo en los logs.
4. Reparto **round-robin**: el índice circular distribuye la carga de forma uniforme entre los actores
   del pool.
5. `ctx.ask` envía la tarea y registra un temporizador. Es completamente asíncrono: el supervisor
   **no** se bloquea esperando y puede seguir aceptando peticiones.
6. Por eso devuelve `this` inmediatamente: la respuesta llegará más tarde como un mensaje interno
   `WorkerReply`.
7. Si el worker murió o no contestó a tiempo, `thr != null`: se responde un error controlado en JSON.
   Este es el punto exacto donde el sistema convierte un fallo en una respuesta.
8. En el camino feliz simplemente se reenvía el resultado al cliente original.

### 6.4 [`ActorService.java`](src/main/java/com/universidad/semana14/ActorService.java) — ActorSystem reutilizable

```java
private static volatile ActorSystem<Supervisor.Command> system;      // (1)

public static ActorSystem<Supervisor.Command> system() {
  if (system == null) {                                              // (2)
    synchronized (ActorService.class) {
      if (system == null)
        system = ActorSystem.create(Supervisor.create(POOL_SIZE), "actor-serverless");
    }
  }
  return system;
}

public static Map<String, Object> process(String op, String payload) {
  CompletionStage<Worker.Result> future = AskPattern.ask(            // (3)
      system(), replyTo -> new Supervisor.Submit(op, payload, replyTo),
      REQUEST_TIMEOUT, system().scheduler());
  try {
    Worker.Result r = future.toCompletableFuture().get(6, TimeUnit.SECONDS);  // (4)
    ...
  } catch (Exception e) { ... }                                      // (5)
}
```

1. `volatile` + singleton: el ActorSystem sobrevive entre invocaciones del mismo contenedor Lambda,
   amortizando su coste de creación.
2. Doble chequeo: solo se sincroniza durante el arranque en frío; las invocaciones calientes no pagan
   el coste del bloqueo.
3. `AskPattern.ask` es la puerta de entrada desde código **no actor** (el handler HTTP) hacia el
   sistema de actores; devuelve un `CompletionStage`.
4. Se espera el resultado con un límite duro, más amplio que el timeout interno del supervisor,
   formando dos barreras de protección concéntricas.
5. Ninguna excepción escapa del servicio: todo fallo se traduce a un JSON de error, que es lo que
   espera un cliente HTTP.

### 6.5 [`LambdaHandler.java`](src/main/java/com/universidad/semana14/LambdaHandler.java) — adaptador HTTP

```java
public class LambdaHandler implements RequestStreamHandler {         // (1)
  private static final ObjectMapper MAPPER = new ObjectMapper();     // (2)

  public void handleRequest(InputStream in, OutputStream out, Context ctx) throws IOException {
    JsonNode event = MAPPER.readTree(in);
    JsonNode body = event;
    if (event.has("body") && !event.get("body").isNull())             // (3)
        body = MAPPER.readTree(event.get("body").asText());

    String op = body.path("op").asText("sum");                        // (4)
    String payload = body.path("payload").asText("");

    Map<String, Object> result = ActorService.process(op, payload);   // (5)
    result.put("requestId", ctx.getAwsRequestId());                   // (6)

    Map<String, Object> response = new HashMap<>();
    response.put("statusCode", Boolean.TRUE.equals(result.get("ok")) ? 200 : 500);  // (7)
    response.put("headers", Map.of("Content-Type", "application/json"));
    response.put("body", MAPPER.writeValueAsString(result));
    out.write(MAPPER.writeValueAsBytes(response));                    // (8)
  }
}
```

1. `RequestStreamHandler` da control total sobre el JSON de entrada y salida, lo que permite aceptar
   tanto eventos de API Gateway/Function URL como un JSON plano de prueba.
2. El `ObjectMapper` es estático porque su creación es costosa y se reutiliza entre invocaciones
   calientes.
3. API Gateway envuelve la petición del usuario dentro del campo `body` como texto; aquí se
   desenvuelve de forma compatible con ambos formatos.
4. Valores por defecto: una petición mal formada no rompe la función.
5. Único punto de unión entre el mundo HTTP y el mundo de los actores.
6. El `requestId` permite correlacionar la respuesta con los logs de CloudWatch.
7. Traducción del resultado lógico a código HTTP: 200 si `ok`, 500 si hubo error controlado.
8. Formato de respuesta de API Gateway v2 (`statusCode`, `headers`, `body`).

---

## 7. Gestión de errores y tolerancia a fallos

| Tipo de fallo | Detección | Respuesta del sistema |
|---|---|---|
| Excepción dentro de un worker | El `ask` del supervisor vence a 1 s porque el actor muere sin responder | Akka reinicia el worker con backoff; el cliente recibe 500 con un mensaje explicativo |
| Operación no soportada | Validación dentro del worker | `Result(ok=false)` sin romper el actor; HTTP 500 con el detalle |
| Worker lento o saturado | Timeout del `ask` (1 s) y del `AskPattern` (5–6 s) | Error controlado; el resto del pool sigue atendiendo |
| JSON de entrada inválido | Valores por defecto en `LambdaHandler` | Se procesa con `op="sum"` y payload vacío en lugar de fallar |
| Cold start de Lambda | Primera invocación del contenedor | ActorSystem singleton: solo se paga una vez (~195 ms medidos) |
| Excepción inesperada global | `try/catch` en `ActorService.process` | Se serializa como JSON de error; la función nunca devuelve una traza cruda |

El principio de diseño es que el cliente **siempre** recibe una respuesta bien formada dentro de un
tiempo acotado. Existen dos barreras de tiempo concéntricas: el timeout del supervisor hacia el worker
(1 s) y el timeout del handler hacia el supervisor (5–6 s), ambos muy por debajo del timeout de la
función Lambda (30 s), de modo que la respuesta de error se genera siempre antes de que la plataforma
corte la invocación.

---

## 8. Ejecución y evidencias

Logs completos: [`docs/demo-logs.txt`](docs/demo-logs.txt) y [`docs/http-examples.txt`](docs/http-examples.txt).

### 8.1 Peticiones y respuestas HTTP reales

```console
$ curl -i -X POST http://localhost:8080/task -d '{"op":"sum","payload":"10,20,30,40"}'
HTTP/1.1 200 OK
Content-type: application/json

{"result":"100","op":"sum","payload":"10,20,30,40","ok":true,
 "worker":"worker-2","latencyMs":0}

$ curl -i -X POST http://localhost:8080/task -d '{"op":"fail","payload":"boom"}'
HTTP/1.1 500 Internal Server Error
Content-type: application/json

{"result":"El worker fallo y fue reiniciado por el supervisor: Ask timed out on
 [Actor[akka://actor-serverless/user/worker-3]] after [1000 ms]. Message of type
 [com.universidad.semana14.Worker$Task]...","op":"fail","payload":"boom",
 "ok":false,"worker":"worker-3","latencyMs":1015}

$ curl -X POST http://localhost:8080/task -d '{"op":"upper","payload":"sigue vivo tras el fallo"}'
{"result":"SIGUE VIVO TRAS EL FALLO","op":"upper","ok":true,
 "worker":"worker-1","latencyMs":0}
```

La tercera petición demuestra la tolerancia a fallos: inmediatamente después de que un worker muriera
y fuera reiniciado, el servicio sigue respondiendo con normalidad y con latencia normal.

### 8.2 Log de arranque y reparto de trabajo

```log
INFO Worker     - [WORKER] worker-1 creado y listo
INFO Worker     - [WORKER] worker-2 creado y listo
INFO Worker     - [WORKER] worker-3 creado y listo
INFO Supervisor - [SUPERVISOR] Pool creado con 3 workers supervisados
INFO Supervisor - [SUPERVISOR] Despachando op='sum' a worker-1
INFO Worker     - [WORKER] worker-1 procesando op='sum' payload='10,20,30,40'
INFO Worker     - [WORKER] worker-1 respondio '100'
INFO Supervisor - [SUPERVISOR] Resultado de worker-1 -> 100
>> PETICION  : sum('10,20,30,40')
<< RESPUESTA : {result=100, ok=true, worker=worker-1, latencyMs=195}

INFO Supervisor - [SUPERVISOR] Despachando op='upper' a worker-2
INFO Worker     - [WORKER] worker-2 respondio 'ARQUITECTURA SERVERLESS'
<< RESPUESTA : {result=ARQUITECTURA SERVERLESS, ok=true, worker=worker-2, latencyMs=2}
```

Se observa el reparto round-robin (`worker-1`, `worker-2`, `worker-3`) y el efecto del arranque en
frío: 195 ms en la primera petición frente a 1–2 ms en las siguientes.

### 8.3 Log del fallo y del reinicio automático

```log
INFO  Supervisor - [SUPERVISOR] Despachando op='fail' a worker-1
INFO  Worker     - [WORKER] worker-1 procesando op='fail' payload='provoca-error'
WARN  Worker     - [WORKER] worker-1 recibio PreRestart: el supervisor lo esta reiniciando
ERROR Worker     - Supervisor RestartSupervisor saw failure [1]:
                   Fallo intencionado en worker-1
java.lang.IllegalStateException: Fallo intencionado en worker-1
    at com.universidad.semana14.Worker.onTask(Worker.java:79)
    at akka.actor.typed.internal.RestartSupervisor.aroundReceive(Supervision.scala:279)
    ...
INFO  Worker     - [WORKER] worker-1 creado y listo          <-- REINICIADO
ERROR Supervisor - [SUPERVISOR] worker-1 fallo (TimeoutException).
                   Akka lo reinicia; respondemos error controlado
<< RESPUESTA : {ok=false, worker=worker-1,
                result=El worker fallo y fue reiniciado por el supervisor..., latencyMs=1020}

INFO  Supervisor - [SUPERVISOR] Despachando op='sum' a worker-2
INFO  Worker     - [WORKER] worker-2 respondio '6'
<< RESPUESTA : {result=6, ok=true, worker=worker-2, latencyMs=1}
=== FIN DE LA DEMO: el sistema respondio a TODAS las peticiones ===
```

Esta es la evidencia central de la actividad. La secuencia `PreRestart` → excepción → «worker-1 creado
y listo» demuestra que el supervisor detectó el fallo y recreó al actor sin intervención humana y sin
detener el ActorSystem.

### 8.4 Métricas observadas

| Escenario | Latencia | Observación |
|---|---|---|
| Primera petición (cold start) | 195 ms | Incluye la creación del ActorSystem y del pool |
| Peticiones siguientes | 0–2 ms | ActorSystem reutilizado; solo coste de mensajería |
| Petición con fallo | ~1015 ms | Marcado por el timeout del `ask` hacia el worker muerto |
| Petición posterior al fallo | 0–1 ms | El sistema recupera su rendimiento normal |

---

## 9. Análisis: actores frente a funciones sin estado

| Criterio | Modelo de Actores (Akka) | Serverless puro (funciones sin estado) |
|---|---|---|
| **Estado** | Privado y duradero dentro de cada actor; permite acumuladores, sesiones y máquinas de estado | Sin estado por definición; cualquier dato debe ir a un almacén externo (DynamoDB, Redis, S3) |
| **Concurrencia** | Miles de actores ligeros sobre unos pocos hilos; control fino del reparto | Gestionada por el proveedor: una instancia de función por petición |
| **Tolerancia a fallos** | Explícita y programable: jerarquía de supervisión con restart, resume, stop o escalado | Implícita: la invocación falla y se reintenta; no hay recuperación de estado local |
| **Coordinación** | Natural entre actores mediante mensajes; permite protocolos y flujos con memoria | Requiere orquestación externa (Step Functions, colas SQS, eventos) |
| **Escalado** | Vertical y por clúster (Akka Cluster); requiere planificación | Automático y prácticamente ilimitado, de cero a miles de instancias |
| **Coste** | Se paga la infraestructura esté o no en uso | Pago por milisegundo consumido; coste cero en reposo |
| **Arranque** | Coste inicial del ActorSystem (cientos de ms) | Cold start del runtime; menor si la función es ligera |
| **Complejidad** | Mayor: hay que diseñar protocolos, mensajes y estrategias de supervisión | Menor para casos simples; crece al orquestar muchas funciones |

### 9.1 Discusión

Las funciones sin estado son ideales para transformaciones cortas y aisladas, donde cada petición es
independiente: validar un fichero, redimensionar una imagen o exponer un CRUD sencillo. Su fuerza es
la simplicidad operativa y el escalado automático. Su debilidad aparece cuando el problema requiere
memoria entre mensajes o coordinación entre unidades de trabajo: entonces hay que externalizar el
estado, lo que introduce latencia de red, coste y problemas de consistencia.

El Modelo de Actores brilla exactamente en ese punto: permite mantener estado vivo en memoria con
acceso serializado y seguro, modelar entidades de larga vida (una partida, un carrito, un dispositivo
IoT) y definir de forma declarativa qué hacer cuando algo falla. Su coste es una mayor complejidad de
diseño y la necesidad de un runtime que esté ejecutándose.

La conclusión práctica de este trabajo es que ambos modelos son **complementarios**, pero su
combinación tiene un matiz importante: dentro de una función Lambda, los actores aportan concurrencia
estructurada, reparto de carga y supervisión **dentro de la invocación**, pero **no** aportan
persistencia **entre** invocaciones, porque el contenedor puede destruirse en cualquier momento. Es
decir, se hereda el aislamiento y la tolerancia a fallos del Modelo de Actores, pero se renuncia a los
actores de larga vida. Si el dominio necesita actores duraderos (Akka Persistence o Akka Cluster), el
destino natural no es FaaS sino contenedores de larga vida (ECS, EKS) o un runtime especializado. Por
eso el pool de este proyecto se dimensiona pequeño (3 workers) y las tareas son cortas: encaja con el
modelo de ejecución efímera.

---

## 10. Conclusiones

- Se implementó un microservicio funcional con un supervisor que gestiona un pool de tres workers y
  aplica una estrategia de reinicio con backoff exponencial.
- El fallo intencionado (`op="fail"`) demuestra en los logs el ciclo completo: excepción, señal
  `PreRestart`, recreación del actor y continuidad del servicio.
- El microservicio se encapsuló en una función AWS Lambda con endpoint HTTP que recibe y devuelve
  JSON, con infraestructura declarada en AWS SAM y despliegue automatizado.
- La ejecución es asíncrona de extremo a extremo (patrón `ask`) y está protegida por timeouts
  concéntricos, de modo que ningún cliente queda bloqueado.
- El ActorSystem se reutiliza entre invocaciones, reduciendo la latencia de 195 ms a menos de 2 ms
  tras el arranque en frío.
- El análisis comparativo muestra que actores y funciones sin estado resuelven problemas distintos y
  que su combinación es útil, aunque limitada por la naturaleza efímera de FaaS.

---

## 11. Referencias

- Akka Documentation. *Introduction to Actors*. https://doc.akka.io/docs/akka/current/typed/actors.html
- Akka Documentation. *Fault Tolerance and Supervision*. https://doc.akka.io/docs/akka/current/typed/fault-tolerance.html
- Hewitt, C.; Bishop, P.; Steiger, R. (1973). *A Universal Modular ACTOR Formalism for Artificial Intelligence*.
- Armstrong, J. (2003). *Making reliable distributed systems in the presence of software errors* (Erlang/OTP).
- Baeldung. *Serverless Architecture with AWS Lambda*. https://www.baeldung.com/aws-lambda-serverless
- Wikipedia. *Serverless computing*. https://en.wikipedia.org/wiki/Serverless_computing
- AWS. *Lambda Developer Guide* y *AWS SAM Developer Guide*. https://docs.aws.amazon.com/lambda/

---

## Publicar en GitHub

```bash
git remote add origin https://github.com/<usuario>/actor-serverless-semana14.git
git branch -M main
git push -u origin main
```

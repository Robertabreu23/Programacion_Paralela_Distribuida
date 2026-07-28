"""
===============================================================================
 PROTOTIPO DE CONSENSO DISTRIBUIDO — ALGORITMO RAFT
===============================================================================
Robert Abreu 23-0121
 Semana 12: Consenso Distribuido - Paxos y Raft
 Lenguaje: Python 3 (solo biblioteca estandar)

 DESCRIPCION GENERAL
 -------------------
 Este programa simula un cluster Raft de N nodos (por defecto 5) ejecutandose
 como hilos independientes dentro de un mismo proceso. Cada nodo mantiene su
 propio estado, su propio log replicado y se comunica con los demas UNICAMENTE
 mediante mensajes que viajan por una red simulada (clase Network). No existe
 memoria compartida entre nodos: todo se resuelve por paso de mensajes, tal
 como ocurriria en un sistema distribuido real.

 ROLES DE RAFT IMPLEMENTADOS
 ---------------------------
   * FOLLOWER  (seguidor)  : estado inicial. Solo responde RPCs.
   * CANDIDATE (candidato) : se postula cuando expira su election timeout.
   * LEADER    (lider)     : unico que atiende clientes y replica el log.

 RPCs IMPLEMENTADOS (los dos del paper original de Ongaro & Ousterhout)
 ---------------------------------------------------------------------
   * RequestVote   : usado durante la eleccion de lider.
   * AppendEntries : usado para replicar entradas y como heartbeat (latido).

 MECANISMOS DE SEGURIDAD IMPLEMENTADOS
 -------------------------------------
   * Terms monotonos crecientes (reloj logico del cluster).
   * Un voto por term (votedFor).
   * Restriccion de voto por actualidad del log (election restriction).
   * Consistencia de log via prevLogIndex / prevLogTerm.
   * Commit solo por mayoria y solo de entradas del term actual.

 ESCENARIO QUE SE EJECUTA AUTOMATICAMENTE
 ----------------------------------------
   FASE 1: arranque del cluster y eleccion del primer lider.
   FASE 2: un cliente propone "A=1"; se replica y se compromete (commit).
   FASE 3: FALLO SIMULADO -> se apaga el lider actual.
   FASE 4: el cluster detecta el fallo y elige un nuevo lider (nuevo term).
   FASE 5: se propone "B=2" con el nuevo lider (el sistema sigue disponible).
   FASE 6: RECUPERACION -> el nodo caido vuelve y se sincroniza con el lider.
   FASE 7: verificacion final de consistencia entre todas las replicas vivas.
===============================================================================
"""

import logging
import os
import queue
import random
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# =============================================================================
# PARAMETROS DE TIEMPO DEL PROTOCOLO
# =============================================================================
# Regla de Raft: broadcastTime << electionTimeout << MTBF
HEARTBEAT_INTERVAL = 0.40        # cada cuanto el lider envia latidos
ELECTION_TIMEOUT_MIN = 1.20      # timeout de eleccion aleatorio (minimo)
ELECTION_TIMEOUT_MAX = 2.40      # timeout de eleccion aleatorio (maximo)
NETWORK_DELAY_MIN = 0.005        # latencia minima simulada de la red
NETWORK_DELAY_MAX = 0.030        # latencia maxima simulada de la red
TICK = 0.02                      # resolucion del bucle principal de cada nodo


# =============================================================================
# LOGGING: escribe simultaneamente en consola y en logs/ejecucion_raft.log
# =============================================================================
def configurar_logging() -> logging.Logger:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    carpeta_logs = os.path.join(base, "logs")
    os.makedirs(carpeta_logs, exist_ok=True)
    ruta = os.path.join(carpeta_logs, "ejecucion_raft.log")

    logger = logging.getLogger("raft")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter(
        fmt="[%(asctime)s.%(msecs)03d] %(message)s", datefmt="%H:%M:%S"
    )
    consola = logging.StreamHandler(sys.stdout)
    consola.setFormatter(formato)
    archivo = logging.FileHandler(ruta, mode="w", encoding="utf-8")
    archivo.setFormatter(formato)

    logger.addHandler(consola)
    logger.addHandler(archivo)
    return logger


LOG = configurar_logging()


def titulo(texto: str) -> None:
    """Imprime un separador visual para delimitar las fases del escenario."""
    LOG.info("")
    LOG.info("=" * 78)
    LOG.info(texto)
    LOG.info("=" * 78)


# =============================================================================
# ESTRUCTURAS DE DATOS
# =============================================================================
@dataclass
class LogEntry:
    """Una entrada del log replicado: el comando y el term en que se creo."""
    term: int
    command: str          # ejemplo: "A=1"

    def __repr__(self) -> str:
        return f"(term={self.term}, cmd='{self.command}')"


@dataclass
class Message:
    """Mensaje (RPC) que viaja por la red simulada entre dos nodos."""
    tipo: str                                  # RequestVote | RequestVoteResp |
                                               # AppendEntries | AppendEntriesResp
    emisor: str
    receptor: str
    term: int
    datos: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# RED SIMULADA
# =============================================================================
class Network:
    """
    Simula el canal de comunicacion entre nodos.

    Responsabilidades:
      - Entregar mensajes con una latencia aleatoria (red asincrona).
      - DESCARTAR todo mensaje que entre o salga de un nodo marcado como caido.
        Esto es lo que modela el fallo: para el resto del cluster, un nodo
        caido es indistinguible de un nodo muy lento o particionado.
    """

    def __init__(self) -> None:
        self.buzones: Dict[str, "queue.Queue[Message]"] = {}
        self.caidos: set = set()
        self.lock = threading.Lock()
        self.mensajes_enviados = 0
        self.mensajes_descartados = 0

    def registrar(self, node_id: str) -> "queue.Queue[Message]":
        self.buzones[node_id] = queue.Queue()
        return self.buzones[node_id]

    def caer(self, node_id: str) -> None:
        """Marca un nodo como caido: deja de enviar y de recibir mensajes."""
        with self.lock:
            self.caidos.add(node_id)

    def levantar(self, node_id: str) -> None:
        """Reincorpora un nodo al cluster."""
        with self.lock:
            self.caidos.discard(node_id)
        # Se vacia el buzon: los mensajes de mientras estuvo caido se perdieron.
        buzon = self.buzones[node_id]
        while not buzon.empty():
            try:
                buzon.get_nowait()
            except queue.Empty:
                break

    def esta_caido(self, node_id: str) -> bool:
        with self.lock:
            return node_id in self.caidos

    def enviar(self, msg: Message) -> None:
        """Entrega asincrona con retardo; descarta si alguno de los dos cayo."""
        with self.lock:
            if msg.emisor in self.caidos or msg.receptor in self.caidos:
                self.mensajes_descartados += 1
                return
            self.mensajes_enviados += 1

        retardo = random.uniform(NETWORK_DELAY_MIN, NETWORK_DELAY_MAX)

        def entregar() -> None:
            # Se vuelve a comprobar al momento de la entrega: el nodo pudo
            # caerse mientras el mensaje viajaba por la red.
            with self.lock:
                if msg.emisor in self.caidos or msg.receptor in self.caidos:
                    self.mensajes_descartados += 1
                    return
            self.buzones[msg.receptor].put(msg)

        t = threading.Timer(retardo, entregar)
        t.daemon = True
        t.start()


# =============================================================================
# NODO RAFT
# =============================================================================
class RaftNode(threading.Thread):
    """
    Un servidor Raft. Vive en su propio hilo y ejecuta indefinidamente el
    bucle: (1) procesar mensajes entrantes, (2) revisar temporizadores.

    Indexacion del log: se usa base 1 (como en el paper).
      - indice 0        -> entrada ficticia "cero" (log vacio)
      - indice i        -> self.log[i - 1]
    """

    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

    def __init__(self, node_id: str, pares: List[str], red: Network) -> None:
        super().__init__(daemon=True, name=node_id)
        self.id = node_id
        self.pares = [p for p in pares if p != node_id]
        self.red = red
        self.buzon = red.registrar(node_id)

        # ---- Estado persistente (sobreviviria a un reinicio) ----------------
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []

        # ---- Estado volatil en todos los servidores -------------------------
        self.estado: str = RaftNode.FOLLOWER
        self.commit_index: int = 0     # ultima entrada comprometida
        self.last_applied: int = 0     # ultima entrada aplicada a la maquina
        self.lider_actual: Optional[str] = None

        # ---- Estado volatil solo en el lider --------------------------------
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # ---- Maquina de estados replicada (el "dato" acordado) --------------
        self.maquina_estados: Dict[str, str] = {}

        # ---- Temporizadores --------------------------------------------------
        self.election_timeout: float = self._nuevo_timeout()
        self.ultimo_contacto: float = time.time()
        self.ultimo_heartbeat: float = 0.0

        self.votos_recibidos: set = set()
        self.activo = True
        self.lock = threading.RLock()

    # -------------------------------------------------------------------------
    # Utilidades internas
    # -------------------------------------------------------------------------
    def _nuevo_timeout(self) -> float:
        """Timeout ALEATORIO: es lo que evita que todos se postulen a la vez."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _reiniciar_timer_eleccion(self) -> None:
        self.ultimo_contacto = time.time()
        self.election_timeout = self._nuevo_timeout()

    @property
    def ultimo_indice(self) -> int:
        return len(self.log)

    @property
    def ultimo_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _mayoria(self) -> int:
        total = len(self.pares) + 1
        return total // 2 + 1

    def log_info(self, texto: str) -> None:
        LOG.info(f"{self.id:>7} | T{self.current_term:<2} | {self.estado:<9} | {texto}")

    # -------------------------------------------------------------------------
    # Transiciones de estado
    # -------------------------------------------------------------------------
    def volverse_seguidor(self, term: int, motivo: str = "") -> None:
        cambio = self.estado != RaftNode.FOLLOWER or term > self.current_term
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.estado = RaftNode.FOLLOWER
        self._reiniciar_timer_eleccion()
        if cambio:
            self.log_info(f"-> pasa a FOLLOWER {motivo}")

    def volverse_candidato(self) -> None:
        """
        Se cumplio el election timeout sin recibir noticias del lider:
        el nodo incrementa el term, se vota a si mismo y pide votos.
        """
        self.current_term += 1
        self.estado = RaftNode.CANDIDATE
        self.voted_for = self.id
        self.votos_recibidos = {self.id}
        self._reiniciar_timer_eleccion()
        self.log_info(
            f"** ELECTION TIMEOUT ** inicia eleccion para el term {self.current_term} "
            f"(se auto-vota, 1/{self._mayoria()} votos necesarios)"
        )
        for par in self.pares:
            self.red.enviar(Message(
                tipo="RequestVote",
                emisor=self.id,
                receptor=par,
                term=self.current_term,
                datos={
                    "lastLogIndex": self.ultimo_indice,
                    "lastLogTerm": self.ultimo_term,
                },
            ))

    def volverse_lider(self) -> None:
        self.estado = RaftNode.LEADER
        self.lider_actual = self.id
        # Al asumir, el lider asume optimistamente que todos estan a su nivel.
        self.next_index = {p: self.ultimo_indice + 1 for p in self.pares}
        self.match_index = {p: 0 for p in self.pares}
        self.log_info(
            f"*** ELECTO LIDER del term {self.current_term} con "
            f"{len(self.votos_recibidos)}/{len(self.pares)+1} votos ***"
        )
        self.enviar_append_entries()   # heartbeat inmediato: impone autoridad

    # -------------------------------------------------------------------------
    # RPC 1: RequestVote  (fase de ELECCION DE LIDER)
    # -------------------------------------------------------------------------
    def manejar_request_vote(self, msg: Message) -> None:
        candidato = msg.emisor
        term = msg.term
        last_log_index = msg.datos["lastLogIndex"]
        last_log_term = msg.datos["lastLogTerm"]

        # Regla general: si veo un term mayor, me actualizo y me hago seguidor.
        if term > self.current_term:
            self.volverse_seguidor(term, f"(vio term superior de {candidato})")

        conceder = False
        if term < self.current_term:
            motivo = f"term {term} obsoleto (el mio es {self.current_term})"
        elif self.voted_for not in (None, candidato):
            motivo = f"ya vote por {self.voted_for} en el term {self.current_term}"
        else:
            # RESTRICCION DE ELECCION: solo se vota a quien tenga un log al
            # menos tan actualizado como el propio. Garantiza que el nuevo
            # lider posee todas las entradas ya comprometidas.
            al_dia = (last_log_term > self.ultimo_term) or (
                last_log_term == self.ultimo_term and last_log_index >= self.ultimo_indice
            )
            if al_dia:
                conceder = True
                self.voted_for = candidato
                self._reiniciar_timer_eleccion()
                motivo = "log del candidato al dia"
            else:
                motivo = "el log del candidato esta desactualizado"

        self.log_info(
            f"RequestVote de {candidato} (term {term}) -> "
            f"{'VOTO CONCEDIDO' if conceder else 'VOTO DENEGADO'}: {motivo}"
        )
        self.red.enviar(Message(
            tipo="RequestVoteResp",
            emisor=self.id,
            receptor=candidato,
            term=self.current_term,
            datos={"votoConcedido": conceder},
        ))

    def manejar_respuesta_voto(self, msg: Message) -> None:
        if msg.term > self.current_term:
            self.volverse_seguidor(msg.term, "(respuesta con term superior)")
            return
        if self.estado != RaftNode.CANDIDATE or msg.term != self.current_term:
            return   # respuesta tardia o ya no soy candidato: se ignora

        if msg.datos["votoConcedido"]:
            self.votos_recibidos.add(msg.emisor)
            self.log_info(
                f"recibe voto de {msg.emisor} "
                f"({len(self.votos_recibidos)}/{self._mayoria()} necesarios)"
            )
            if len(self.votos_recibidos) >= self._mayoria():
                self.volverse_lider()

    # -------------------------------------------------------------------------
    # RPC 2: AppendEntries  (REPLICACION DE LOG + HEARTBEAT)
    # -------------------------------------------------------------------------
    def enviar_append_entries(self) -> None:
        """El lider envia a cada seguidor las entradas que le faltan."""
        self.ultimo_heartbeat = time.time()
        for par in self.pares:
            next_i = self.next_index.get(par, self.ultimo_indice + 1)
            prev_index = next_i - 1
            prev_term = self.log[prev_index - 1].term if prev_index > 0 else 0
            entradas = self.log[prev_index:]     # lo que le falta al seguidor

            self.red.enviar(Message(
                tipo="AppendEntries",
                emisor=self.id,
                receptor=par,
                term=self.current_term,
                datos={
                    "prevLogIndex": prev_index,
                    "prevLogTerm": prev_term,
                    "entries": [(e.term, e.command) for e in entradas],
                    "leaderCommit": self.commit_index,
                },
            ))

    def manejar_append_entries(self, msg: Message) -> None:
        term = msg.term
        prev_index = msg.datos["prevLogIndex"]
        prev_term = msg.datos["prevLogTerm"]
        entradas = [LogEntry(t, c) for t, c in msg.datos["entries"]]
        leader_commit = msg.datos["leaderCommit"]

        # 1) Rechazar si el "lider" viene de un term viejo.
        if term < self.current_term:
            self.red.enviar(Message(
                tipo="AppendEntriesResp", emisor=self.id, receptor=msg.emisor,
                term=self.current_term,
                datos={"exito": False, "matchIndex": 0},
            ))
            return

        # 2) Lider legitimo: me hago seguidor y reinicio mi election timeout.
        self.volverse_seguidor(term, f"(reconoce a {msg.emisor} como lider)")
        self.lider_actual = msg.emisor

        # 3) CHEQUEO DE CONSISTENCIA DE LOG (Log Matching Property).
        #    Mi log debe coincidir con el del lider en prevLogIndex/prevLogTerm.
        if prev_index > 0:
            if self.ultimo_indice < prev_index or self.log[prev_index - 1].term != prev_term:
                self.log_info(
                    f"AppendEntries de {msg.emisor} RECHAZADO: no coincide en "
                    f"prevLogIndex={prev_index} (mi log tiene {self.ultimo_indice} entradas)"
                )
                self.red.enviar(Message(
                    tipo="AppendEntriesResp", emisor=self.id, receptor=msg.emisor,
                    term=self.current_term,
                    datos={"exito": False, "matchIndex": 0},
                ))
                return

        # 4) Se eliminan conflictos y se anexan las entradas nuevas.
        if entradas:
            self.log = self.log[:prev_index] + entradas
            self.log_info(
                f"replica {len(entradas)} entrada(s) de {msg.emisor}: "
                f"{[e.command for e in entradas]} -> log={self.ultimo_indice} entradas"
            )

        # 5) Se avanza el commitIndex hasta donde indique el lider.
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.ultimo_indice)
            self.aplicar_entradas()

        self.red.enviar(Message(
            tipo="AppendEntriesResp", emisor=self.id, receptor=msg.emisor,
            term=self.current_term,
            datos={"exito": True, "matchIndex": self.ultimo_indice},
        ))

    def manejar_respuesta_append(self, msg: Message) -> None:
        if msg.term > self.current_term:
            self.volverse_seguidor(msg.term, "(respuesta con term superior)")
            return
        if self.estado != RaftNode.LEADER:
            return

        if msg.datos["exito"]:
            self.match_index[msg.emisor] = msg.datos["matchIndex"]
            self.next_index[msg.emisor] = msg.datos["matchIndex"] + 1
            self.actualizar_commit_index()
        else:
            # Retroceso: el lider prueba con un indice anterior hasta encontrar
            # el punto en que ambos logs coinciden.
            self.next_index[msg.emisor] = max(1, self.next_index.get(msg.emisor, 1) - 1)
            self.log_info(
                f"{msg.emisor} rechazo la replicacion -> reintenta desde el "
                f"indice {self.next_index[msg.emisor]}"
            )

    # -------------------------------------------------------------------------
    # COMMIT Y APLICACION A LA MAQUINA DE ESTADOS
    # -------------------------------------------------------------------------
    def actualizar_commit_index(self) -> None:
        """
        Una entrada se compromete cuando esta replicada en la MAYORIA y
        pertenece al term actual del lider (regla de seguridad del paper).
        """
        for n in range(self.ultimo_indice, self.commit_index, -1):
            replicas = 1 + sum(1 for p in self.pares if self.match_index.get(p, 0) >= n)
            if replicas >= self._mayoria() and self.log[n - 1].term == self.current_term:
                self.commit_index = n
                self.log_info(
                    f"### CONSENSO ALCANZADO ### entrada #{n} '{self.log[n-1].command}' "
                    f"replicada en {replicas}/{len(self.pares)+1} nodos (mayoria) -> COMMIT"
                )
                self.aplicar_entradas()
                break

    def aplicar_entradas(self) -> None:
        """Aplica al 'estado' las entradas comprometidas y aun no aplicadas."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            comando = self.log[self.last_applied - 1].command
            if "=" in comando:
                clave, valor = comando.split("=", 1)
                self.maquina_estados[clave.strip()] = valor.strip()
            self.log_info(
                f"aplica entrada #{self.last_applied} '{comando}' "
                f"-> estado={self.maquina_estados}"
            )

    # -------------------------------------------------------------------------
    # INTERFAZ DE CLIENTE
    # -------------------------------------------------------------------------
    def proponer(self, comando: str) -> bool:
        """Un cliente solicita un cambio. Solo el lider puede aceptarlo."""
        with self.lock:
            if self.estado != RaftNode.LEADER:
                return False
            self.log.append(LogEntry(self.current_term, comando))
            self.log_info(
                f">>> PROPUESTA DEL CLIENTE '{comando}' aceptada como entrada "
                f"#{self.ultimo_indice} (term {self.current_term}); replicando..."
            )
            self.enviar_append_entries()
            return True

    # -------------------------------------------------------------------------
    # BUCLE PRINCIPAL DEL NODO
    # -------------------------------------------------------------------------
    def run(self) -> None:
        while self.activo:
            # Un nodo caido no procesa nada: simula estar apagado.
            if self.red.esta_caido(self.id):
                time.sleep(TICK)
                self._reiniciar_timer_eleccion()   # al volver, empieza limpio
                continue

            # 1) Procesar mensajes entrantes
            try:
                msg = self.buzon.get(timeout=TICK)
            except queue.Empty:
                msg = None

            if msg is not None:
                with self.lock:
                    if msg.tipo == "RequestVote":
                        self.manejar_request_vote(msg)
                    elif msg.tipo == "RequestVoteResp":
                        self.manejar_respuesta_voto(msg)
                    elif msg.tipo == "AppendEntries":
                        self.manejar_append_entries(msg)
                    elif msg.tipo == "AppendEntriesResp":
                        self.manejar_respuesta_append(msg)

            # 2) Revisar temporizadores
            with self.lock:
                ahora = time.time()
                if self.estado == RaftNode.LEADER:
                    if ahora - self.ultimo_heartbeat >= HEARTBEAT_INTERVAL:
                        self.enviar_append_entries()
                else:
                    if ahora - self.ultimo_contacto >= self.election_timeout:
                        self.volverse_candidato()

    def detener(self) -> None:
        self.activo = False


# =============================================================================
# CLUSTER: agrupa los nodos y expone utilidades para el escenario de prueba
# =============================================================================
class ClusterRaft:
    def __init__(self, cantidad: int = 5) -> None:
        self.red = Network()
        ids = [f"nodo-{i}" for i in range(1, cantidad + 1)]
        self.nodos: Dict[str, RaftNode] = {
            nid: RaftNode(nid, ids, self.red) for nid in ids
        }

    def iniciar(self) -> None:
        for n in self.nodos.values():
            n.start()

    def detener(self) -> None:
        for n in self.nodos.values():
            n.detener()

    def lider(self) -> Optional[RaftNode]:
        vivos = [n for n in self.nodos.values() if not self.red.esta_caido(n.id)]
        lideres = [n for n in vivos if n.estado == RaftNode.LEADER]
        if not lideres:
            return None
        return max(lideres, key=lambda n: n.current_term)

    def esperar_lider(self, timeout: float = 12.0) -> Optional[RaftNode]:
        limite = time.time() + timeout
        while time.time() < limite:
            lider = self.lider()
            if lider is not None:
                time.sleep(0.6)          # deja que los latidos se estabilicen
                return self.lider()
            time.sleep(0.05)
        return None

    def tabla_estado(self, titulo_tabla: str) -> None:
        LOG.info("")
        LOG.info(f"--- {titulo_tabla} ---")
        LOG.info(f"{'NODO':<9}{'ESTADO':<11}{'TERM':<6}{'COMMIT':<8}"
                 f"{'LOG':<28}{'MAQUINA DE ESTADOS'}")
        for nid in sorted(self.nodos):
            n = self.nodos[nid]
            if self.red.esta_caido(nid):
                LOG.info(f"{nid:<9}{'(CAIDO)':<11}{'-':<6}{'-':<8}{'-':<28}-")
                continue
            comandos = ", ".join(e.command for e in n.log) or "(vacio)"
            LOG.info(f"{nid:<9}{n.estado:<11}{n.current_term:<6}"
                     f"{n.commit_index:<8}{comandos:<28}{n.maquina_estados}")
        LOG.info("")


# =============================================================================
# ESCENARIO DE DEMOSTRACION
# =============================================================================
def main() -> None:
    random.seed()   # cambiar por un entero fijo si se desea reproducibilidad

    titulo("FASE 1 — ARRANQUE DEL CLUSTER Y ELECCION DEL PRIMER LIDER")
    LOG.info("Se inician 5 nodos en estado FOLLOWER. Ninguno conoce a un lider,")
    LOG.info("por lo que el primero cuyo election timeout expire se postulara.")
    LOG.info("Quorum necesario para decidir: 3 de 5 nodos.")
    LOG.info("")

    cluster = ClusterRaft(cantidad=5)
    cluster.iniciar()

    lider1 = cluster.esperar_lider()
    if lider1 is None:
        LOG.info("No se logro elegir lider. Fin de la simulacion.")
        cluster.detener()
        return
    LOG.info("")
    LOG.info(f">>> RESULTADO FASE 1: lider electo = {lider1.id} "
             f"(term {lider1.current_term})")
    cluster.tabla_estado("ESTADO TRAS LA PRIMERA ELECCION")

    # -------------------------------------------------------------------------
    titulo("FASE 2 — PROPUESTA DEL CLIENTE: 'A=1' (replicacion de log)")
    LOG.info(f"El cliente contacta al lider {lider1.id} y solicita fijar A=1.")
    LOG.info("El lider anexa la entrada a su log y la replica con AppendEntries.")
    LOG.info("Solo cuando la mayoria confirma, la entrada se compromete (commit).")
    LOG.info("")
    lider1.proponer("A=1")
    time.sleep(2.0)
    cluster.tabla_estado("ESTADO TRAS COMPROMETER 'A=1'")

    # -------------------------------------------------------------------------
    titulo("FASE 3 — FALLO SIMULADO: SE APAGA EL LIDER")
    caido = lider1.id
    LOG.info(f"Se desconecta al lider {caido}. La red descartara todos sus")
    LOG.info("mensajes en ambos sentidos, por lo que dejaran de llegar latidos.")
    LOG.info("Los seguidores deberian detectarlo al expirar su election timeout.")
    LOG.info("")
    cluster.red.caer(caido)
    time.sleep(0.5)

    # -------------------------------------------------------------------------
    titulo("FASE 4 — DETECCION DEL FALLO Y ELECCION DE UN NUEVO LIDER")
    lider2 = cluster.esperar_lider()
    if lider2 is None:
        LOG.info("No se pudo elegir un nuevo lider.")
        cluster.detener()
        return
    LOG.info("")
    LOG.info(f">>> RESULTADO FASE 4: nuevo lider = {lider2.id} "
             f"(term {lider2.current_term}). El cluster sobrevivio al fallo "
             f"porque 4 de 5 nodos siguen vivos (quorum = 3).")
    cluster.tabla_estado("ESTADO TRAS LA SEGUNDA ELECCION")

    # -------------------------------------------------------------------------
    titulo("FASE 5 — EL SISTEMA SIGUE OPERATIVO: NUEVA PROPUESTA 'B=2'")
    LOG.info(f"Con {caido} aun caido, el cliente propone B=2 al nuevo lider.")
    LOG.info("")
    lider2.proponer("B=2")
    time.sleep(2.0)
    cluster.tabla_estado("ESTADO CON EL NODO CAIDO (obsérvese que sigue habiendo consenso)")

    # -------------------------------------------------------------------------
    titulo("FASE 6 — RECUPERACION: EL NODO CAIDO SE REINCORPORA")
    LOG.info(f"Se vuelve a conectar {caido}. Al recibir un AppendEntries con un")
    LOG.info("term mayor, reconocera al nuevo lider y se sincronizara: el lider")
    LOG.info("le enviara las entradas que le faltan hasta igualar los logs.")
    LOG.info("")
    cluster.red.levantar(caido)
    time.sleep(3.0)
    cluster.tabla_estado("ESTADO TRAS LA RECUPERACION")

    # -------------------------------------------------------------------------
    titulo("FASE 7 — VERIFICACION FINAL DE CONSISTENCIA")
    estados = {nid: dict(n.maquina_estados) for nid, n in cluster.nodos.items()}
    logs = {nid: [e.command for e in n.log] for nid, n in cluster.nodos.items()}
    for nid in sorted(cluster.nodos):
        LOG.info(f"  {nid}: log={logs[nid]}  estado={estados[nid]}")

    referencia = estados[sorted(estados)[0]]
    consistente = all(v == referencia for v in estados.values())
    LOG.info("")
    LOG.info(f"Mensajes entregados por la red : {cluster.red.mensajes_enviados}")
    LOG.info(f"Mensajes descartados por fallos: {cluster.red.mensajes_descartados}")
    LOG.info("")
    if consistente:
        LOG.info("RESULTADO: TODAS LAS REPLICAS COINCIDEN. El consenso se mantuvo")
        LOG.info("a pesar del fallo del lider (propiedad de State Machine Safety).")
    else:
        LOG.info("RESULTADO: se detectaron divergencias entre replicas.")
    LOG.info("=" * 78)

    cluster.detener()
    time.sleep(0.3)


if __name__ == "__main__":
    main()

"""
===============================================================================
 PROTOTIPO COMPLEMENTARIO — PAXOS DE DECRETO UNICO (Single-Decree Paxos)
===============================================================================
Robert Abreu 23-0121
 Semana 12: Consenso Distribuido - Paxos y Raft
 Lenguaje: Python 3 (solo biblioteca estandar)

 OBJETIVO
 --------
 Mostrar en codigo las DOS FASES clasicas de Paxos, para contrastarlas con la
 implementacion de Raft del archivo raft_simulation.py.

 ROLES DE PAXOS
 --------------
   * PROPOSER (proponente): recibe la peticion del cliente e intenta que el
     cluster acuerde un valor. No decide nada por si mismo.
   * ACCEPTOR (aceptador) : la "memoria" del sistema. Vota por los numeros de
     propuesta y recuerda que valor acepto. Un valor queda elegido cuando lo
     acepta una MAYORIA de aceptadores.
   * LEARNER (aprendiz)   : se entera del valor finalmente elegido.

 LAS DOS FASES
 -------------
   FASE 1 (PREPARE / PROMISE)
     1a. El proposer elige un numero de propuesta n (unico y creciente) y
         envia PREPARE(n) a todos los aceptadores.
     1b. Un aceptador responde PROMISE(n) si n es mayor que cualquier numero
         que haya prometido antes; ademas informa el valor que ya hubiera
         aceptado (si existe). Al prometer, se compromete a ignorar toda
         propuesta con numero menor que n.

   FASE 2 (ACCEPT / ACCEPTED)
     2a. Si el proposer recibio promesas de una mayoria, propone un valor:
         si ALGUN aceptador ya habia aceptado algo, DEBE reproponer ese valor
         (el de mayor numero de propuesta); solo si nadie habia aceptado nada
         puede usar su propio valor. Envia ACCEPT(n, v).
     2b. El aceptador acepta (n, v) si no prometio nada mayor que n.
         Si una mayoria acepta, el valor queda ELEGIDO (chosen) para siempre.

 ESTA REGLA DE LA FASE 2a ES EL CORAZON DE LA SEGURIDAD DE PAXOS: garantiza
 que, una vez elegido un valor, ningun proposer posterior pueda cambiarlo.

 ESCENARIO QUE SE EJECUTA
 ------------------------
   FASE A: 5 aceptadores sanos; el proposer P1 hace acordar "A=1".
   FASE B: caen 2 aceptadores (quedan 3 = quorum justo). Un segundo proposer
           P2 intenta imponer "B=99" y el protocolo lo obliga a reproponer
           "A=1": el consenso previo es inmutable.
   FASE C: cae un tercer aceptador (solo quedan 2 de 5). Ya no hay mayoria:
           el sistema deja de progresar pero NUNCA se corrompe.
   FASE D: se recuperan los aceptadores y el sistema vuelve a operar.
===============================================================================
"""

import logging
import os
import random
import sys
from typing import Dict, List, Optional, Tuple

# =============================================================================
# LOGGING (consola + archivo logs/ejecucion_paxos.log)
# =============================================================================
def configurar_logging() -> logging.Logger:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    carpeta = os.path.join(base, "logs")
    os.makedirs(carpeta, exist_ok=True)
    logger = logging.getLogger("paxos")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(message)s")
    for h in (logging.StreamHandler(sys.stdout),
              logging.FileHandler(os.path.join(carpeta, "ejecucion_paxos.log"),
                                  mode="w", encoding="utf-8")):
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


LOG = configurar_logging()


def titulo(texto: str) -> None:
    LOG.info("")
    LOG.info("=" * 78)
    LOG.info(texto)
    LOG.info("=" * 78)


# =============================================================================
# ACCEPTOR — el rol que garantiza la seguridad del algoritmo
# =============================================================================
class Acceptor:
    def __init__(self, nombre: str) -> None:
        self.nombre = nombre
        self.caido = False
        # Numero de propuesta mas alto que ha PROMETIDO respetar.
        self.promesa_maxima: int = 0
        # Ultima propuesta que efectivamente ACEPTO (numero y valor).
        self.numero_aceptado: int = 0
        self.valor_aceptado: Optional[str] = None

    # ---- FASE 1b -----------------------------------------------------------
    def recibir_prepare(self, n: int) -> Optional[Tuple[int, Optional[str]]]:
        """Devuelve PROMISE(n, ultimo_aceptado) o None si rechaza."""
        if self.caido:
            LOG.info(f"    {self.nombre}: [SIN RESPUESTA — nodo caido]")
            return None
        if n > self.promesa_maxima:
            self.promesa_maxima = n
            extra = (f"; ya habia aceptado ({self.numero_aceptado}, "
                     f"'{self.valor_aceptado}')" if self.valor_aceptado else
                     "; nunca ha aceptado nada")
            LOG.info(f"    {self.nombre}: PROMISE({n}) concedida{extra}")
            return (self.numero_aceptado, self.valor_aceptado)
        LOG.info(f"    {self.nombre}: PREPARE({n}) RECHAZADO "
                 f"(ya prometio {self.promesa_maxima})")
        return None

    # ---- FASE 2b -----------------------------------------------------------
    def recibir_accept(self, n: int, valor: str) -> bool:
        if self.caido:
            LOG.info(f"    {self.nombre}: [SIN RESPUESTA — nodo caido]")
            return False
        if n >= self.promesa_maxima:
            self.promesa_maxima = n
            self.numero_aceptado = n
            self.valor_aceptado = valor
            LOG.info(f"    {self.nombre}: ACCEPTED({n}, '{valor}')")
            return True
        LOG.info(f"    {self.nombre}: ACCEPT({n}) RECHAZADO "
                 f"(ya prometio {self.promesa_maxima})")
        return False


# =============================================================================
# LEARNER — solo registra el valor elegido
# =============================================================================
class Learner:
    def __init__(self) -> None:
        self.valor_elegido: Optional[str] = None

    def aprender(self, valor: str) -> None:
        self.valor_elegido = valor
        LOG.info(f"    LEARNER: valor aprendido y aplicado -> '{valor}'")


# =============================================================================
# PROPOSER — ejecuta las dos fases
# =============================================================================
class Proposer:
    def __init__(self, nombre: str, indice: int, total_proposers: int,
                 aceptadores: List[Acceptor], learner: Learner) -> None:
        self.nombre = nombre
        self.indice = indice
        self.total_proposers = total_proposers
        self.aceptadores = aceptadores
        self.learner = learner
        self.ronda = 0

    def _siguiente_numero(self) -> int:
        """Numeros de propuesta globalmente unicos y crecientes por proposer."""
        self.ronda += 1
        return self.ronda * self.total_proposers + self.indice

    def _quorum(self) -> int:
        return len(self.aceptadores) // 2 + 1

    def proponer(self, valor_deseado: str) -> Optional[str]:
        n = self._siguiente_numero()
        quorum = self._quorum()
        LOG.info("")
        LOG.info(f"[{self.nombre}] Cliente solicita acordar '{valor_deseado}'. "
                 f"Numero de propuesta n={n}. Quorum = {quorum}/{len(self.aceptadores)}.")

        # ------------------- FASE 1: PREPARE / PROMISE ------------------------
        LOG.info(f"  --- FASE 1: PREPARE({n}) enviado a los {len(self.aceptadores)} aceptadores ---")
        promesas: List[Tuple[int, Optional[str]]] = []
        for a in self.aceptadores:
            r = a.recibir_prepare(n)
            if r is not None:
                promesas.append(r)

        if len(promesas) < quorum:
            LOG.info(f"  >> FASE 1 FALLIDA: solo {len(promesas)} promesa(s) de las "
                     f"{quorum} necesarias. Sin mayoria NO se puede avanzar "
                     f"(se preserva la seguridad, se pierde la disponibilidad).")
            return None
        LOG.info(f"  >> FASE 1 EXITOSA: {len(promesas)} promesas obtenidas "
                 f"(se necesitaban {quorum}).")

        # ------------- REGLA CLAVE DE SEGURIDAD (fase 2a) ---------------------
        # Si alguien ya habia aceptado un valor, hay que reproponer ESE valor.
        aceptados = [(num, val) for num, val in promesas if val is not None]
        if aceptados:
            num_max, valor_previo = max(aceptados, key=lambda x: x[0])
            valor = valor_previo
            LOG.info(f"  !! Un aceptador ya habia aceptado ('{valor_previo}', n={num_max}).")
            LOG.info(f"  !! REGLA DE PAXOS: {self.nombre} DEBE abandonar "
                     f"'{valor_deseado}' y reproponer '{valor}'.")
        else:
            valor = valor_deseado
            LOG.info(f"  -- Ningun aceptador tenia valor previo: se propone "
                     f"el valor propio '{valor}'.")

        # ------------------- FASE 2: ACCEPT / ACCEPTED ------------------------
        LOG.info(f"  --- FASE 2: ACCEPT({n}, '{valor}') enviado a los aceptadores ---")
        aceptaciones = sum(1 for a in self.aceptadores if a.recibir_accept(n, valor))

        if aceptaciones >= quorum:
            LOG.info(f"  >> FASE 2 EXITOSA: {aceptaciones} aceptaciones "
                     f"(se necesitaban {quorum}).")
            LOG.info(f"  ### VALOR ELEGIDO (CHOSEN) = '{valor}' ###")
            self.learner.aprender(valor)
            return valor

        LOG.info(f"  >> FASE 2 FALLIDA: solo {aceptaciones} aceptacion(es) de las "
                 f"{quorum} necesarias. No hay consenso en esta ronda.")
        return None


# =============================================================================
# ESCENARIO DE DEMOSTRACION
# =============================================================================
def estado_aceptadores(aceptadores: List[Acceptor]) -> None:
    LOG.info("")
    LOG.info("  --- ESTADO DE LOS ACEPTADORES ---")
    LOG.info(f"  {'ACEPTADOR':<12}{'ESTADO':<10}{'PROMESA MAX':<14}"
             f"{'ACEPTADO (n, valor)'}")
    for a in aceptadores:
        estado = "CAIDO" if a.caido else "ACTIVO"
        acc = (f"({a.numero_aceptado}, '{a.valor_aceptado}')"
               if a.valor_aceptado else "(ninguno)")
        LOG.info(f"  {a.nombre:<12}{estado:<10}{a.promesa_maxima:<14}{acc}")
    LOG.info("")


def main() -> None:
    random.seed()
    aceptadores = [Acceptor(f"acc-{i}") for i in range(1, 6)]
    learner = Learner()
    p1 = Proposer("PROPOSER-1", indice=1, total_proposers=2,
                  aceptadores=aceptadores, learner=learner)
    p2 = Proposer("PROPOSER-2", indice=2, total_proposers=2,
                  aceptadores=aceptadores, learner=learner)

    titulo("FASE A — CONSENSO NORMAL CON 5 ACEPTADORES SANOS")
    LOG.info("El proposer P1 intenta que el cluster acuerde el valor 'A=1'.")
    p1.proponer("A=1")
    estado_aceptadores(aceptadores)

    titulo("FASE B — FALLO SIMULADO: CAEN 2 DE LOS 5 ACEPTADORES")
    aceptadores[3].caido = True
    aceptadores[4].caido = True
    LOG.info("acc-4 y acc-5 dejan de responder. Quedan 3 activos: aun hay quorum (3).")
    LOG.info("Ahora un SEGUNDO proposer intenta imponer un valor distinto, 'B=99'.")
    LOG.info("Se espera que el protocolo lo obligue a respetar el valor ya elegido.")
    resultado = p2.proponer("B=99")
    estado_aceptadores(aceptadores)
    LOG.info(f"  >>> Resultado de la FASE B: el valor acordado sigue siendo '{resultado}'.")
    LOG.info("      El consenso previo es INMUTABLE, aunque el cliente pidiera otra")
    LOG.info("      cosa y aunque hubiera nodos caidos.")

    titulo("FASE C — FALLO MAYOR: CAE UN TERCER ACEPTADOR (2 DE 5 ACTIVOS)")
    aceptadores[2].caido = True
    LOG.info("Con solo 2 aceptadores activos ya no existe mayoria (se requieren 3).")
    LOG.info("El protocolo debe DETENERSE sin corromper el valor ya elegido:")
    LOG.info("Paxos sacrifica DISPONIBILIDAD antes que CONSISTENCIA (teorema CAP).")
    p1.proponer("C=7")
    estado_aceptadores(aceptadores)

    titulo("FASE D — RECUPERACION: LOS ACEPTADORES VUELVEN AL CLUSTER")
    for a in aceptadores:
        a.caido = False
    LOG.info("Los tres aceptadores se reincorporan. Se vuelve a tener quorum.")
    LOG.info("Notese que acc-4 y acc-5 NO conocian el valor: la nueva ronda los")
    LOG.info("pondra al dia gracias a la regla de reproposicion de la fase 2a.")
    final = p1.proponer("D=3")
    estado_aceptadores(aceptadores)

    titulo("RESUMEN FINAL")
    LOG.info(f"Valor consensuado por el cluster : '{final}'")
    LOG.info(f"Valor conocido por el learner    : '{learner.valor_elegido}'")
    LOG.info("")
    LOG.info("CONCLUSION: pese a tres intentos de proponer valores distintos")
    LOG.info("('B=99', 'C=7', 'D=3') y a la caida de hasta 3 de 5 aceptadores,")
    LOG.info("el unico valor jamas elegido fue 'A=1'. Paxos garantiza que un")
    LOG.info("valor decidido no puede ser revocado ni sobrescrito.")
    LOG.info("=" * 78)


if __name__ == "__main__":
    main()

# Consenso Distribuido — Prototipos de Raft y Paxos

Actividad de la Semana 12: implementación de un prototipo que simula el consenso
distribuido mediante los algoritmos **Raft** (principal) y **Paxos de decreto único**
(complementario, para efectos de comparación).

---

## 1. Contenido del repositorio

```
consenso-distribuido/
├── README.md                     # Este archivo (instrucciones)
├── src/
│   ├── raft_simulation.py        # Prototipo principal: Raft con 5 nodos
│   └── paxos_simulation.py       # Prototipo complementario: Paxos (prepare/accept)
├── logs/
│   ├── ejecucion_raft.log        # Salida completa de la corrida de Raft
│   └── ejecucion_paxos.log       # Salida completa de la corrida de Paxos
└── docs/
    └── Informe_Consenso_Distribuido.docx   # Investigación, comparativa e informe
```

## 2. Requisitos

- Python 3.8 o superior.
- **No se requiere ninguna dependencia externa**: ambos programas usan solo la
  biblioteca estándar (`threading`, `queue`, `random`, `logging`, `time`).

Verificar la versión instalada:

```bash
python3 --version
```

## 3. Ejecución

### Prototipo Raft (elección de líder + replicación de log + fallo del líder)

```bash
python3 src/raft_simulation.py
```

Duración aproximada: 15–20 segundos. La salida se muestra en pantalla y se guarda
automáticamente en `logs/ejecucion_raft.log`.

### Prototipo Paxos (fases prepare/accept)

```bash
python3 src/paxos_simulation.py
```

Duración aproximada: 1 segundo. La salida se guarda en `logs/ejecucion_paxos.log`.

## 4. Qué demuestra cada prototipo

### Raft (`src/raft_simulation.py`)

Cluster de **5 nodos** ejecutándose como hilos independientes que se comunican
exclusivamente por paso de mensajes a través de una red simulada.

| Fase | Qué ocurre |
|------|------------|
| 1 | Arranque del cluster y **elección del primer líder** (RPC `RequestVote`) |
| 2 | El cliente propone `A=1`; se **replica** (RPC `AppendEntries`) y se compromete por mayoría |
| 3 | **Fallo simulado**: el líder se desconecta y deja de enviar latidos |
| 4 | Los seguidores detectan el fallo por *election timeout* y **eligen un nuevo líder** |
| 5 | El sistema **sigue operativo**: se propone y compromete `B=2` sin el nodo caído |
| 6 | **Recuperación**: el nodo caído regresa y el líder lo sincroniza |
| 7 | **Verificación**: todas las réplicas tienen el mismo log y el mismo estado |

Parámetros configurables al inicio del archivo:

```python
HEARTBEAT_INTERVAL   = 0.40   # intervalo de latidos del líder
ELECTION_TIMEOUT_MIN = 1.20   # timeout de elección (mínimo)
ELECTION_TIMEOUT_MAX = 2.40   # timeout de elección (máximo)
```

Para cambiar el número de nodos, modifique en `main()`:

```python
cluster = ClusterRaft(cantidad=5)   # p. ej. 3, 5 o 7 nodos
```

> Nota: los tiempos de espera son aleatorios, por lo que **cada ejecución elige un
> líder distinto**. Esto es el comportamiento correcto y esperado del algoritmo.
> Para obtener corridas reproducibles, fije la semilla en `main()`: `random.seed(42)`.

### Paxos (`src/paxos_simulation.py`)

5 aceptadores, 2 proponentes y 1 aprendiz. Demuestra las dos fases del algoritmo y,
sobre todo, la propiedad de seguridad: **una vez elegido un valor, ningún proponente
posterior puede cambiarlo**, aunque el cliente pida otra cosa y aunque haya nodos caídos.

## 5. Tolerancia a fallos

Ambos prototipos requieren **mayoría (quórum)** para decidir:

| Nodos totales | Quórum | Fallos tolerados |
|---------------|--------|------------------|
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

Si se pierde el quórum, el sistema **deja de progresar pero nunca se corrompe**:
se sacrifica disponibilidad para preservar consistencia.



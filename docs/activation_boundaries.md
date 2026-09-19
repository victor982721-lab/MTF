# Fronteras de activación: cierre OFFLINE

## Estado actual

Este documento separa **implementación local** de **evidencia del servidor**.
El [informe de ingeniería](../reports/engineering/latest/engineering_consolidation.md)
y los [resultados reproducibles](../reports/engineering/latest/engineering_results.json)
son el registro del gate local. Ninguna fixture prueba autorización, elegibilidad,
ejecución en el bróker, rentabilidad ni aceptación contractual de Pepperstone.

### A — IMPLEMENTADO Y COMPROBADO OFFLINE

| Capacidad | Evidencia local y límite |
|---|---|
| Ingesta causal, M1/M5/M15, indicadores y detector compartido | [Causalidad](../tests/test_capture_causality.py), [fronteras del dominio](../tests/test_domain_boundaries.py) y [consumidores de señales](../tests/test_signal_consumers.py). Los parámetros de estrategia no se optimizan durante este cierre. |
| Calidad bid/ask, disponibilidad, reconexión y cantidades exactas | [Quote quality](../tests/test_ctrader_quote_quality.py), [contratos CFD](../tests/test_cfd_contracts.py); rechazos explícitos ante dato insuficiente, no cotizaciones reales. |
| PAPER incremental, replay, checkpoints, reanudación e idempotencia | [PAPER session](../tests/test_paper_session.py), [persistencia de producto](../tests/test_product_persistence.py) y [recuperación](../tests/test_persistence_recovery.py) y [migraciones interrumpidas](../tests/test_migration_delivery.py). SQLite v4 distingue producto, cantidades y economía desconocida. |
| SDK/codec, framing, gateway, correlation y validación de respuestas | [Contratos de sesión](../tests/test_ctrader_session_contract.py) y [contratos DEMO](../tests/test_ctrader_demo_contracts.py) con Protobuf instalado y transporte falso/local. No se sustituye el codec oficial por diccionarios en el gate de entrega. |
| Instalación, imports, CLI, configuración y fixtures distribuidos | Instalación y comandos en [README](../README.md); las pruebas de entrega cubren un entorno aislado y configuración empaquetada, no sólo el checkout de desarrollo. |
| UI fixture y JavaScript | [Gate JavaScript](../tests/test_ui_javascript.py) con Node obligatorio en el gate de entrega. La UI es observacional y no recalcula la estrategia. |
| Herramientas de ingeniería y artefactos portables | [Tooling](../reports/engineering/latest/engineering_tooling.json): locks, versiones, alcance de lint/formato/tipos, arquitectura, pruebas y huella de archivos. Los resultados se regeneran desde el árbol a verificar. |

### Composición DEMO comprobada localmente

El servicio [CTraderDemoApplication](../mtf_lab/ops/ctrader_demo_composition.py)
recibe cliente, resolutores de referencias de secretos, selección explícita,
política y almacenamiento de intents. `DemoExecutionSignalConsumer` conecta el
detector con el ejecutor durante la ingesta, sin extraer una señal de un resultado
PAPER terminado para enviarla por una ruta independiente. El wrapper offline
instancia ese mismo servicio con `LoopbackProtobufTransport` y un servidor falso.

Las [pruebas de composición](../tests/test_demo_composition.py) recorren
Protobuf, autorización simulada, catálogo/datos normalizados, RuntimeCoordinator,
señal, límites, journal durable, mensajes de orden, eventos, reconciliación,
cierre y reporte. La composición reutilizable no exige respuestas predeterminadas
de una fixture para poder operar; las expectativas sintéticas pertenecen a la
prueba. La ejecución permanece desactivada por defecto y REAL/LIVE se rechaza.

La reanudación de la **API DEMO** exige `stream_identity` estable y
`resume_source="full_prefix"`: el input conserva el prefijo original y puede
extenderlo con eventos nuevos. El punto de continuación se valida contra
`last_event_id` persistido, no contra un conteo de filas. Un sufijo aislado se
rechaza; no se promete una API DEMO de tail-only. Este contrato es distinto del
replay/cursor de la sesión CFD PAPER.

El journal se recupera antes de nuevas entradas. Un intent sin actualización
se conserva como `UNKNOWN`, las colisiones de payload fallan cerrado y los
estados inciertos requieren reconciliación antes de permitir otra entrada.
Recovery no llama a `submit`. Activar la composición requiere journal durable;
un store sólo en memoria no acredita esa barrera.

### B — IMPLEMENTADO, PENDIENTE DE VALIDACIÓN EXTERNA

La aplicación implementa los contratos para autorización de aplicación,
descubrimiento, selección DEMO explícita, autorización de cuenta, catálogo,
market data y ejecución DEMO con persistencia/reconciliación. La composición se
prueba localmente con credenciales ficticias y gateway controlado; sus pruebas
no constituyen una sesión autorizada de cTrader.

En el estado vigente, el servidor DEMO concedió el alcance `TRADING` únicamente
para la cuenta aprobada (sufijo `5097`) y la ruta VIEW original permanece intacta.
Esto prueba el alcance administrativo/técnico observado, no una ejecución: hay
0 órdenes y la canaria de software sigue `STAGED`, sin instalación ni
automatización creada.

Lo no observado offline es el resultado que debe devolver el servidor real:
permisos y entorno de la cuenta, símbolos habilitados, escalas, límites de volumen,
negociación, horarios, spread/comisiones/conversiones, aceptación/rechazo,
fills, posiciones e historia. Un timeout ambiguo permanece `UNKNOWN` y exige
reconciliación: no autoriza un reenvío automático. Ausencia en una respuesta no
se convierte en certeza de que una orden nunca existió.

### C — CANARIA DEMO ACOTADA Y GATES EXTERNOS

La lectura DEMO de sólo lectura con OAuth y alcance `accounts` ya fue observada
en el estado vigente. El usuario aprobó una canaria manual para EUR/USD en la
cuenta DEMO de sufijo `5097`, el lunes 2026-09-21 de 09:00 a 09:20 hora de
México (inicio 15:00 UTC): dos ciclos independientes, primero 1 BUY y su cierre,
después 1 SELL y su cierre, hasta 1,000 unidades (0.01 lot), una sola posición,
SL de 1.5 ATR, TP de 3 ATR, riesgo de hasta 0.05% por
ciclo, stop de prueba de 0.1% del equity, holding de hasta 300 s y cuatro
mutaciones nominales (máximo seis). El servidor concedió `TRADING` DEMO sólo para
esa cuenta; no se repite OAuth ni se cambia la cuenta seleccionada, pero cada
conexión exige discovery fresco de lectura y verificación de esa cuenta. Los
gates que permanecen son:

1. Promover e instalar el software de la canaria, que permanece `STAGED`, sólo
   para la ventana y límites aprobados. La automatización aún no está creada; se
   planifica un heartbeat nativo una vez que el software sea aceptado. No se
   extiende la ventana ni se reintenta un resultado ambiguo.
2. Verificar catálogo, límites y condiciones reales del bróker para la
   cuenta/símbolo, sin convertir una plantilla local en autorización.
3. Ejecutar y reconciliar dos ciclos independientes contra el servidor **DEMO**:
   primero 1 BUY y cierre, después 1 SELL y cierre. Esta
   autorización no abre estrategia, operación continua, forward, extensión
   automática, REAL ni LIVE; el gate offline no ejecuta este paso.
4. Confirmar condiciones contractuales de Pepperstone relevantes para México,
   uso/almacenamiento/redistribución de datos y costes. No se declara validación
   contractual ni se recomienda contratar en función de los sintéticos; una
   especificación de costes no equivale a cargos observados.

La lista C contiene una acción técnica acotada y hechos, decisiones o
autorizaciones externos, no una apertura general de trading. No se solicitan
secretos ni se inicia autenticación como parte de la reproducción offline.

## Límites que no se levantan al aprobar el gate

- La reproducción del gate offline no inicia OAuth, consulta cuentas ni abre
  conexiones al bróker.
- No usa stores del usuario ni credenciales reales; HOME/XDG/SQLite se aíslan.
- Los eventos de ejecución de las fixtures son **sintéticos**, no operaciones.
- La ruta **REAL/LIVE sigue rechazada fail-closed** y no forma parte de esta entrega.
- La concesión técnica `TRADING` DEMO sólo cubre la cuenta y ventana aprobadas;
  el software sigue `STAGED`; la automatización aún no está creada y se planifica
  un heartbeat nativo sólo después de aceptar el software. No habilita extensión
  o reintento.
- Una cuenta etiquetada localmente como DEMO no basta: la ruta externa exige
  evidencia de sesión, identidad, generación, entorno y permisos observados.
- Los fixtures no demuestran rentabilidad, rendimiento futuro, tarifas vigentes
  ni permisos de redistribución de cotizaciones reales.

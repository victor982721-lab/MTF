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

Lo no observado offline es el resultado que debe devolver el servidor real:
permisos y entorno de la cuenta, símbolos habilitados, escalas, límites de volumen,
negociación, horarios, spread/comisiones/conversiones, aceptación/rechazo,
fills, posiciones e historia. Un timeout ambiguo permanece `UNKNOWN` y exige
reconciliación: no autoriza un reenvío automático. Ausencia en una respuesta no
se convierte en certeza de que una orden nunca existió.

### C — INTERVENCIÓN DEL USUARIO PENDIENTE

1. Registrar y obtener la aprobación de una aplicación cTrader.
2. Disponer externamente de `CTRADER_CLIENT_ID` y `CTRADER_CLIENT_SECRET`.
3. Completar OAuth y consentir el alcance aplicable.
4. Descubrir las cuentas realmente autorizadas por ese consentimiento.
5. Seleccionar explícitamente una cuenta **DEMO**, sin selección automática.
6. Verificar catálogo y condiciones reales del bróker para la cuenta/símbolo.
7. Autorizar y verificar la ruta contra el servidor **DEMO**, incluidos permisos,
   ejecución y reconciliación. El gate offline no ejecuta este paso.
8. Confirmar condiciones contractuales de Pepperstone relevantes para México,
   uso/almacenamiento/redistribución de datos y costes. No se declara validación
   contractual ni se recomienda contratar en función de los sintéticos.

La lista C contiene hechos, decisiones y autorizaciones externos, no tareas de
programación diferidas. No se solicitan secretos ni se inicia autenticación como
parte de la reproducción offline.

## Límites que no se levantan al aprobar el gate

- El gate no inicia OAuth, consulta cuentas ni abre conexiones al bróker.
- No usa stores del usuario ni credenciales reales; HOME/XDG/SQLite se aíslan.
- Los eventos de ejecución de las fixtures son **sintéticos**, no operaciones.
- La ruta **REAL/LIVE sigue rechazada fail-closed** y no forma parte de esta entrega.
- Una cuenta etiquetada localmente como DEMO no basta: la ruta externa exige
  evidencia de sesión, identidad, generación, entorno y permisos observados.
- Los fixtures no demuestran rentabilidad, rendimiento futuro, tarifas vigentes
  ni permisos de redistribución de cotizaciones reales.

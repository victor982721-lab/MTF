# Consolidación de ingeniería MTF Lab — cierre OFFLINE

Fecha de entrega: 2026-09-11. Alcance: código, empaquetado, contratos, fixtures,
reproducción y publicación de la fase **OFFLINE**. No es activación de cuenta,
validación contractual de Pepperstone ni operación DEMO/REAL.

## Identidad y preservación

El punto publicado anterior era `46545533992918ca44804038814f1adc19107f8b`.
Al comenzar este cierre, `HEAD`, `main` y el remoto coincidían allí, pero había
**32 archivos rastreados modificados y 42 nuevos**, sin archivos rastreados
faltantes. Los **107 hashes** del informe previo coincidieron con el inventario
inicial del checkout; no se hizo reset ni se descartó código no rastreado.

La [evidencia histórica consolidada](../history/2026-09-11-refactor.json)
conserva una sola copia de baseline, resultados y tooling anteriores. Sus cifras
son antecedentes, **no** el gate actual. Sólo se normalizaron paths personales,
temporales y referencias internas; los resultados y fingerprints no se alteraron.
Las secciones 1–4 conservan los hallazgos de ese refactor, integrados en esta entrega.

El estado actual del árbol se acredita con
[engineering_results.json](engineering_results.json) y
[engineering_tooling.json](engineering_tooling.json): UTC, versiones, hashes por
archivo, conteos separados, gates y límites externos. Los manifests no pueden
incluir su propio hash ni el SHA del commit que los contiene; la identidad de
contenido y sus exclusiones explícitas evitan esa referencia circular. El SHA
publicado se obtiene del commit de GitHub y se contrasta con los hashes del
manifest; un `HEAD` de procedencia anterior no se presenta como el nuevo commit.

## 1. Responsabilidades y dependencia

| Antes | Después y frontera comprobable |
|---|---|
| `data/ctrader.py`: 109,966 bytes; configuración, codec, socket, sesión, mercado y fixtures juntos | Fachada pública explícita; `ctrader_protocol`, `ctrader_transport`, `ctrader_session`, `ctrader_accounts`, `ctrader_market`, configuración/errores y fixtures separados. Un solo cliente/lector, no un cliente nuevo por caso de uso. |
| `ops/cli.py`: 107,252 bytes; argumentos y ejecución mezclados | Fachada/entrada, `cli_arguments`, handlers pequeños y servicios de aplicación. Los servicios conservan la ejecución de los comandos reales. |
| PAPER desactivaba el simulador binario vaciando sus privados | `RuntimeCoordinator` compone un consumidor de registro, binario o CFD. El detector no cambia al cambiar el producto. |
| Replay del detector seguido de otra simulación CFD completa | `CTraderPaperSession`: `ingest`, `ingest_many`, `advance`, `snapshot`, `restore`, `finish`; replay alimenta esa misma sesión. |
| Normalización, dominio financiero y almacenamiento parcialmente mezclados | Captura/normalización en adaptadores; calidad, aritmética y transiciones CFD en `core`; composición y commits en `ops`; SQLite/UI mediante contratos por producto. |

La dirección comprobada es presentación → servicios/adaptadores → dominio.
`core` no importa sockets, OAuth, SQLite, CLI ni SDK, y el auditor no encontró
accesos a reloj ambiental/entorno en ese núcleo. El SDK continúa siendo opcional:
importar dominio o pedir `--help` no lo carga, no abre transporte ni escribe estado.

Los dos ciclos reales desaparecieron: DEMO transporte/ejecutor y
pipeline/runtime/integración. El selector de referencia M1 tiene una sola
implementación en `core.reference`. Se retiraron `_CFDOnlyRuntimeCoordinator`,
`_clear_virtual_book`, `_request_locked`, `_request_via_pump` y el alias sin
consumidores legítimos `KrakenLikeProvider`; no se conservaron implementaciones
paralelas escondidas detrás de fachadas.

## 2. Correcciones semánticas y regresiones

| Problema constatado | Corrección y prueba verificable |
|---|---|
| Ordenar por tiempo de mercado adelantaba datos tardíos | Sobre de captura v1, orden por disponibilidad/secuencia durable; pruebas de ask tardío y cambio real de secuencia en `test_capture_causality.py`. El hash de payload no sustituye dos observaciones legítimas. |
| Recepción inexistente se confundía con latencia cero; unidades implícitas | Recepción desconocida conserva `None` y una política histórica explícita; Spot usa milisegundos declarados. Regresiones de recepción, disponibilidad de trendbar y metadata del lector. |
| Un bid nuevo rejuvenecía el ask retenido; camino `has_both` podía ignorar el libro | Calidad/tiempos por lado hasta el fill; ask t=0 + bid t=180 con tolerancia 60 bloquea entrada LONG y mid operativo; caso SHORT simétrico. `test_ctrader_quote_quality.py` y `test_cfd_contracts.py`. |
| Snapshot, timestamp ausente, cruce o cambio de conexión podían parecer operables | Estados/razones tipados, sin decisiones por substrings. El cruce conserva evidencia cruda pero no llega al fill. Generación nueva exige reconciliación explícita. |
| Base nativa de trendbars etiquetada como trade | Base `native` y rechazo de mezcla no demostrada; calentamiento y traducción de ida/vuelta cubiertos en `test_domain_boundaries.py`. |
| PAPER dependía de borrar libros binarios y rehacer resultados | Consumidores por composición y simulador CFD persistente. Pruebas de detector idéntico, constructor binario no invocado y checkpoint del consumidor en `test_signal_consumers.py`/`test_paper_session.py`. |
| Cursor/checkpoint podían separarse del producto o duplicar sufijos | Commit atómico de hechos, proyecciones, producto y checkpoint; rollback también de memoria; cursor compuesto y hash de integridad. Repetir sufijo confirmado no resucita terminales. |
| Respuestas/heartbeat competían por lectura o envío | Lector único por generación, puerta única de envío, correlación y fases por solicitud. EOF, cancelación, fragmentación, tráfico continuo, backpressure y reconexión tienen pruebas controladas. |
| Timeout genérico volvía incierto incluso un error local | Validación local/no-enviado separada de enviado sin respuesta o respuesta inválida. No se infiere la fase de una variable global del cliente. |
| Protobuf ausente se convertía en cero; estados por substrings y caché vieja | Presencia explícita con clases generadas reales, enums exactos, cantidades Decimal, deals deduplicados e historial/detalle paginado. Ausencia en Reconcile no equivale a orden inexistente. |
| Un caller podía inventar evidencia autenticada | Proof emitido por la sesión tras inventario/autenticación observados en transporte local; identidad, cuenta, generación, caducidad y revocación se validan antes de enviar. Gateway conectado a `CTraderClient.request_message` y codec real. |
| Cierre CFD y resultado económico se reducían a una etiqueta | `CLOSED` puede coexistir con economía `INDETERMINATE`, bruto observado y neto `None`; comisión desconocida explícita. Cantidades exactas sobreviven SQLite, UI y cambios de contexto Decimal. |
| Retención terminal podía permitir reapertura tras evicción | Consulta exacta al archivo durable; sin ella se falla con `ARCHIVE_REQUIRED`. Capacidad activa explícita y vencimiento causal a `UNKNOWN`, sin fill/PnL inventado. No se usa Bloom probabilístico. |

La revisión visual además detectó una regresión de esta iteración: un paréntesis
extra en la nueva tabla CFD impedía compilar todo el script. El baseline compilaba;
el candidato defectuoso no. Tras corregirlo, la ejecución del navegador expuso
llamadas heredadas que pasaban las columnas a `rows` en vez de `table` y abortaban
la carga con datos no vacíos. El navegador también detectó paths SVG heredados
que comenzaban con `L` si el calentamiento dejaba valores iniciales nulos: las
curvas ahora comienzan con `M` y no unen huecos. Son parches de presentación,
sin recalcular indicadores, con regresiones JavaScript reales, contención de
texto largo en tarjetas y revisión de navegador; la API HTTP por sí sola no se presenta como validación de UI.

### Hipótesis descartadas o delimitadas

- No se necesitaba actualizar el SDK, añadir otro cliente de red ni iniciar OAuth
  para probar gateway/codec. Sí faltaba hacer explícito su cableado y su evidencia.
- Un archivo invertido **no** implica equivalencia si cambia el orden observado.
  Sólo el reordenamiento físico que conserva los sobres originales es invariante.
- La ausencia de issues no acredita dataset completo. Cobertura solicitada,
  observada, fin declarado, continuidad y calidad son dimensiones independientes.
- `CFDQuote.mid` es una propiedad aritmética, no autorización de uso. `mid_at`
  aplica la tolerancia y la calidad; los fills usan el gate del lado requerido.
- Un primer cálculo del auditor sobrerrepresentaba imports de símbolos de paquete
  como dependencias de todos sus hijos. Se corrigió el resolvedor y se añadió su
  regresión; las cifras siguientes usan el mismo auditor corregido en ambos lados.
- El tamaño de archivo y un conteo de pruebas no prueban arquitectura ni completitud
  operativa. Tampoco se declara una reducción global de accesos privados.

## 3. Complejidad y herramientas

Métrica comparable: complejidad AST conservadora de `tools/engineering_audit.py`;
Ruff C901 es una segunda puerta, no se intercambian sus valores numéricos.

| Responsabilidad intervenida | Antes | Después |
|---|---:|---:|
| `cmd_watch` → `WatchService.run` | 62 | 7 |
| `IncrementalProcessor.from_checkpoint` | 60 | 6 |
| `CTraderDemoTransport._snapshot_from_response` | 44 | 9 |
| `CTraderProvider.normalize_spot` | 25 | 9 |
| solicitud cTrader: `_request_locked` → `request` | 24 | 4 |
| `CTraderDemoTransport._send` | 22 | 10 |
| `normalize_ctrader_capture` | 21 | 8 |
| `spot_event_to_cfd_quote` | 16 | 8 |

| Inventario histórico del refactor | Baseline | Refactor previo al cierre |
|---|---:|---:|
| Módulos / aristas | 40 / 120 | 62 / 197 |
| Ciclos reales | 2 | 0 |
| Funciones con complejidad >10 | 158 | 133 |
| Máximo heredado | 76 | 76 |
| Accesos privados entre objetos | 89 | 101 |

La extracción aumenta módulos/helpers; no se utiliza ese crecimiento como mejora
por sí solo. Permanecen deudas heredadas visibles, entre ellas `load_config` (76),
`LocalImporter.read` (59), `_trigger_evaluation` (54) y `_accept_candle` (36).
No se modificaron estrategia/parámetros para mejorar resultados ni se reformateó
indiscriminadamente ese código.

El gate de herramientas tiene alcance explícito, no una afirmación de que todo
el código heredado esté libre de complejidad. Los módulos críticos y todo el
código nuevo de entrega se relacionan en
[engineering_tooling.json](engineering_tooling.json), con comandos, versiones y
resultados. Mypy usa modo estricto en los módulos declarados. Las métricas de
esta tabla son históricas del refactor; la auditoría del árbol de entrega se
regenera en [engineering_results.json](engineering_results.json).

## 4. Equivalencia, persistencia y transición

- Se conservan los comandos `doctor`, `demo`, `import`, `replay`, `backtest`,
  `report`, `watch`, `ui`, `ctrader` y `cfd-paper`, y sus fachadas públicas probadas.
  Kraken y binario conservan regresiones locales; no se probó conectividad externa.
- Mismo sobre/configuración: replay completo, particiones de lotes y reinicio en
  mitad de posición conservan decisiones, fills, cantidades, costes y estados.
  La partición generativa usa semilla **7321**. Repetir el sufijo confirmado es no-op.
- Retrasar 180 segundos la disponibilidad de una cotización de entrada conserva
  una decisión anterior del detector, pero cambia un resultado cerrado a
  `UNKNOWN` sin entrada: contraprueba causal, no invariancia artificial.
- SQLite evoluciona de v3 a v4 de forma aditiva; `cfd_trades` no usa `stake` binario
  para guardar unidades. Hechos, análisis/membresías y proyecciones se distinguen;
  no se borra historial para ocultar un conflicto.
- Captura v1, sesión PAPER v1 y snapshot CFD v3 tienen contratos independientes.
  Un snapshot incompatible se rechaza; no se renombra su versión para aceptarlo.
  Las fachadas conservan imports, no autorizan reinterpretación económica de datos.
- `--chunk-size` controla commits; `--session` reproduce sobres archivados de una
  sesión existente. JSONL se lee de forma perezosa y el ordenamiento externo usa
  un índice temporal SQLite. El cursor combina disponibilidad/secuencia.
- `--order market_time_corrected` es inspección con identidad distinta, no PAPER
  causal. Un histórico sin recepción conserva la política histórica declarada.
  La exportación de payloads exige `--include-payloads`; el resultado normal es compacto.
- El lector se reanuda con `reader_resume_arguments()`: secuencia global y generación
  explícitas, sin reiniciar privados. Clocks y controles locales se archivan; un
  heartbeat del socket no rejuvenece precios.

## Correcciones objetivas adicionales de entrega

### Recuperación SQLite

La revisión reprodujo un fallo de reapertura tras interrumpir la migración v2→v3:
el rename legacy ya existía, el marcador seguía en v2 y el segundo rename fallaba.
La migración ahora agrupa DDL/copia/retiro de la tabla legacy en una transacción
explícita, sin commits implícitos de `executescript`, y reconoce estados parciales.

La copia compara los ocho campos del checkpoint. Un duplicado idéntico es no-op;
una colisión diferente falla con rollback y conserva ambas fuentes, no sobrescribe
para completar el gate. El índice de consulta se crea después de retirar la tabla
legacy para no perderlo con ella. Las
[regresiones de migración](../../../tests/test_migration_delivery.py) prueban
interrupción, fallo inyectado/rollback, reintento, conflictos, índice, preservación
v3→v4 y rechazo de schema futuro. Sólo usan bases temporales.

### Distribución instalada

El empaquetado anterior sólo funcionaba como checkout: un wheel instalado fuera
de él buscaba los TOML en `site-packages/config`, donde no existían. El cierre
incorpora defaults como recursos de distribución y resolución canónica de paths
para configuración/CLI, conservando los parámetros existentes. Las pruebas de
packaging verifican una instalación regular fuera del checkout; el estado de
runtime no se escribe dentro del paquete instalado.

### Composición DEMO y frontera de aplicación

Se cerró una brecha real del checkout auditado: auth/gateway/órdenes estaban
probados por separado, pero eso no acreditaba una aplicación completa conectada
al detector. [CTraderDemoApplication](../../../mtf_lab/ops/ctrader_demo_composition.py)
compone el cliente/proveedor, catálogo y datos normalizados con RuntimeCoordinator
y `DemoExecutionSignalConsumer`, política, intents durables y transporte oficial.
El wrapper de prueba inyecta el transporte Protobuf local en ese mismo servicio;
no es un ejecutor alternativo de producción ni fabrica una señal manual.

Las [regresiones](../../../tests/test_demo_composition.py) y la
[matriz de activación](../../../docs/activation_boundaries.md) distinguen evidencia
local de hechos del servidor. La validación sintética incluye los mensajes de
apertura, eventos, reconciliación, cierre y persistencia; no envía operaciones al
bróker. La CLI no activa esta ruta externamente y REAL/LIVE sigue cerrado.

La revisión de cierre además corrigió la recuperación de ejecución: restaurar
únicamente IDs dejaba sin representar un envío incierto. Ahora se restauran
estados, se conserva `UNKNOWN` si falta una actualización y se reconcilia antes
de nuevas entradas. Duplicados conflictivos del journal fallan cerrado. La API
DEMO reanuda con identidad estable y prefijo completo comprobado por
`last_event_id`; rechaza un sufijo aislado en lugar de saltar eventos por índice.
El contrato y sus regresiones están ligados en la matriz de activación.

## 5. Medición y alcance de rendimiento

El benchmark de entrega es sintético, con HOME/XDG/SQLite temporales y red
externa bloqueada. Su comando, volumen, muestras, memoria trazada, checkpoint y
fingerprint se registran junto al gate en
[engineering_results.json](engineering_results.json). Se ejecuta mediante
[tools/benchmark_paper.py](../../../tools/benchmark_paper.py).

La evidencia histórica contiene una corrida de 7,500 sobres y el diagnóstico de
una proyección de checkpoint redundante. Es antecedente de ingeniería, no una
medición atribuida al SHA nuevo. Los tiempos incluyen trazado/commits y dependen
del host/carga; no se anuncia un speedup general ni latencia de producción. La
memoria trazada de Python no equivale al RSS total. No se afirma haber saturado
todas las ventanas de retención ni demostrado una meseta global.

## 6. Cierre de entrega y reproducción

La [guía de instalación](../../../README.md) contiene los comandos completos
para instalación base, extra cTrader, tooling de desarrollo, suite offline,
auditoría, demo, CFD PAPER y UI fixture, sin autenticarse.

El gate de entrega recompila, ejecuta `git diff --check` y `pip check`, exige el
SDK/Protobuf instalado y Node real, vuelve a descubrir y ejecutar la suite
completa, y corre lint/formato/tipos del alcance declarado y arquitectura estricta.
Los conteos **discovered / executed / passed / failed / skipped** se toman del
resultado recién generado: no se reutiliza el conteo de la auditoría anterior.
Los artefactos incluyen el benchmark sintético y la huella de los archivos
verificados; se comprueba que los inputs no cambien durante la corrida.

La publicación se completa con commit/push y verificación desde un **clone nuevo
del remoto**, instalación aislada desde los locks, imports y gate reproducible.
No se rellenan archivos faltantes con archivos ocultos del checkout original ni
se versionan `.venv`, `.venv-dev`, stores, bases o logs de ejecución.

### Estado actual

- **IMPLEMENTADO Y COMPROBADO OFFLINE:** contratos causales y de producto,
  PAPER incremental/replay, recuperación, SQLite v4, SDK/codec/gateway local,
  composición DEMO con datos sintéticos, límites fail-closed, empaquetado, CLI,
  fixtures, UI/JS y herramientas/manifests reproducibles.
- **IMPLEMENTADO, PENDIENTE DE VALIDACIÓN EXTERNA:** comportamiento de la ruta
  autorizada frente a cTrader, catálogo/condiciones y respuestas reales del bróker.
- **INTERVENCIÓN DEL USUARIO PENDIENTE:** únicamente hechos y autorizaciones de
  aplicación, credenciales externas, OAuth, cuenta DEMO y condiciones del bróker,
  detallados en la [matriz A/B/C](../../../docs/activation_boundaries.md).

**Externas: NO EJECUTADAS.** No se inició OAuth, consultaron cuentas, usaron
credenciales reales ni enviaron operaciones. REAL/LIVE permanece rechazado.
Fixtures y pruebas locales no demuestran rentabilidad ni elegibilidad contractual.

## Referencias

- [Estado cTrader](../../../docs/ctrader_integration_status.md).
- [Auditor de arquitectura](../../../tools/engineering_audit.py) y
  [runner offline](../../../tools/offline_tests.py).
- Descriptores Protobuf del SDK instalado, contrastados por pruebas locales;
  [mensajes oficiales cTrader](https://help.ctrader.com/open-api/messages/) y
  [presencia Protobuf](https://protobuf.dev/programming-guides/field_presence/).

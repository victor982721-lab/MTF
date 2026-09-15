# Estado de integración cTrader / Forex-CFD

La consolidación histórica del 2026-09-11 publicó la implementación
offline anterior; no activó cuentas, no realizó OAuth y no envió operaciones. El [informe
de ingeniería](../reports/engineering/latest/engineering_consolidation.md), sus
[resultados](../reports/engineering/latest/engineering_results.json) y el
[manifest de tooling](../reports/engineering/latest/engineering_tooling.json)
son la evidencia reproducible del gate final. La [matriz A/B/C de fronteras](activation_boundaries.md) separa hechos locales de evidencia que sólo puede
producir un servidor autenticado.

## Estado actual

### Validación DEMO conectada de sólo lectura — 2026-09-14

El receipt privado, fuera del repositorio (SHA-256
`a2d6af011fa43b05187c2aace05f6b5c51e592dc9a5e506f872b4cf1f43ac72e`; no se
reproduce el identificador concreto), deja constancia de OAuth vigente con
`SCOPE_VIEW`/`accounts` para una sola cuenta DEMO Winter. No se autorizó REAL,
trading ni órdenes, y no se repitió OAuth.

La consulta de sólo lectura verificó la secuencia connect → application auth →
account discovery → account auth → catalog → history, con 1,940 símbolos y
EUR/USD resuelto. El receipt
`../runtime/market-evidence/connected-v18-20260914T195151Z.json` (SHA-256
`168ef960a9aa2ef0bf10263045bbd3652e5683ace036524bc4e17878c78faf49`)
conserva 9,999 barras M1 nativas en 20 páginas. El servidor indicó
`hasMore=true`, por lo que la captura es `PARTIAL`, no una cobertura completa;
no hay bid/ask ni fills PAPER. El contexto de periodo a nivel de respuesta se
propaga ahora a trendbars cuyo campo hijo es opcional; una discrepancia de
request/response o la ausencia de contexto queda rechazada fail-closed.

La observación continua real conservó un evento, pero terminó bloqueada por
`available_at < event_time` y calidad `CROSSED`/bid=ask. No se aplicó clamping,
reordenamiento, dato sintético, reintento ni orden. El gate que impide pasar la
captura parcial a PAPER está en
`../runtime/market-evidence/connected-v18-partial-gate-20260914T195256Z.json`.
La misma barrera se revalidó con el runtime V19 STAGED en
`../runtime/market-evidence/connected-v19-partial-gate-20260914T2132Z/receipt.json`
(SHA-256 `cb18e425f07581b0c12a3724de2123f65b4ff00ebeb28f995637392312ba3d10`):
returncode 2, `PARTIAL_HISTORY_CAPTURE`, sin crear DB ni reporte.

### Diagnóstico de lectura acotado — 2026-09-15

El probe aislado reutilizó el OAuth DEMO vigente (`accounts`, `SCOPE_VIEW`) sin
repetir OAuth ni abrir SQLite. El receipt completo es
`../runtime/market-evidence/ctrader-diagnostic-20260915T004049Z/receipt.json`
(SHA-256 `bdac08161c9bfd726c43b30b5cf802092c79da805e7f6b3fc4e3afe934eadcf5`);
la evidencia de los 12 eventos está ligada por
`fd77a9c2d53aad880e4057c972688ba03e7f293bddf4e9f826c34319deb1f51f`.

Los 12 `SpotEvent` conservaron el timestamp original en milisegundos, los
enteros relativos crudos de `bid`/`ask`, `symbolId`, escala 100000, cinco
dígitos y pip 4. Los campos necesarios estuvieron presentes y no hubo
actualizaciones parciales (12/12 con ambas piernas). Todos presentaron
`bid == ask` y ninguno `bid > ask`; la regla vigente `bid >= ask -> CROSSED`
los marcó `INVALID`/no operables. Los precios normalizados coincidieron con
los enteros crudos escalados, por lo que esta igualdad proviene del servidor y
no de la composición de piernas o de la normalización. Igualdad no se
interpretó como corrupción automática ni como cotización válida.

En esta ventana no se observó `available_at < event_time` (0/12); el caso
anterior de `-1.576856 s` queda conservado en el receipt V18. NTP local estuvo
sincronizado antes y después, y no se alteraron reloj, red o privacidad. La
comparación cruda/recepción no muestra mutación del normalizador; sin una
referencia independiente del reloj del servidor no es posible separar por
completo desfase del servidor y desfase local. Por ese motivo no se aplicó una
corrección ni se relajó el rechazo de timestamps futuros. Las regresiones de
igualdad/CROSSED y timestamp futuro están en
`../tests/test_ctrader_quote_quality.py`.

Como contraste, la fixture offline recorrió captura → indicadores/señal →
PAPER → SQLite → reportes y reanudación byte-equivalente: 190 barras sintéticas,
una señal y tres trades PAPER cerrados. Su receipt es
`../runtime/market-evidence/paper-report-e2e-20260914T194812Z-v18/paper-report-e2e-receipt.json`.
Esto no demuestra frescura, continuidad, costes, fills ni rentabilidad del
servidor.

Las secciones fechadas más abajo que describen OAuth o aprobación como
pendientes son antecedentes históricos; el estado vigente es el de esta
sección. La lectura real sigue siendo un gate separado de operación y no abre
la ejecución DEMO.

### Captura histórica DEMO bounded V22 — 2026-09-15

El receipt privado de la ventana acotada existente es
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/window-receipt.json`.
Conserva EUR/USD M1 en base de precio `native`, no fixture sintética: 1,327
barras nativas en 3 páginas, de `2026-09-09T22:52:00Z` a
`2026-09-10T20:59:00Z`, sin gaps ni incidencias. El raw
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/history-native-bounded.jsonl`
tiene SHA-256 `80022a3890ee6f3a867819e4c4b19b72cf5c2dfb71b18714d37464aa330f0dad`.
La captura no contiene bid/ask, `quote_events=0` ni `paper_fills=0`; la
consulta fue de sólo lectura en DEMO, sin órdenes y sin abrir SQLite.

La consulta fuente conserva `source_has_more=true` (3,000 barras en 6 páginas),
pero la selección bounded queda separada y satisfecha: `bounded_complete=true`,
`bounded_has_more=false`, 1,327 barras seleccionadas/raw, inicio y fin cubiertos,
fin exacto y cero gaps. Esto cierra únicamente la ventana solicitada; no
re-etiqueta la fuente como completa ni acredita cobertura histórica adicional.

El gate de continuidad/cobertura exige el marcador de selección bounded en todas
las páginas raw y metadatos, y barras M1 adyacentes sobre `event_time`; sólo así
emite `CONTINUOUS`. Un marcador ausente, un gap o una respuesta completa no
bounded conserva continuidad `UNKNOWN` y bloqueo fail-closed. El `END` legacy de
V22 no llevaba marcador; el fix compatible derivó continuidad sólo después de
verificar esas condiciones. El replay local ya quedó comprobado sobre el raw
existente: reporte y reanudación byte-equivalentes en
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/`,
SHA-256 del reporte `3beab3d08ace024fd812a3812420db4aadaed943953f9cc87a65ccb1dc0cdda0`,
`coverage_satisfied=true`, `CONTINUOUS`, 142 señales y
`paper_capture_complete=true`. No hubo bid/ask; 426 intents quedaron
`UNKNOWN` por ausencia de cotización y `FILLED=0`. El receipt compacto es
`../runtime/market-evidence/ctrader-real-history-paper-e2e-20260915.json`.
La captura/replay no acredita rentabilidad, ventaja neta ni operación.

La reanudación `cfd-paper --session` sin `--input` recupera la
`instrument_spec` durable de las páginas históricas antes de crear un análisis;
si falta, entra en conflicto o la configuración/orden no coincide, termina sin
abrir un análisis nuevo. Esta protección evita sustituir el `symbol_id` observado
por el fallback de una configuración fixture.
La prueba focal del contrato queda en `../tests/test_ctrader_historical_paper_gate.py`.

### Ruta `watch` → PAPER local y tick-data — 2026-09-15

`ctrader watch` ya entrega las señales y los `SpotEvent` del único lector al
producto local `FOREX_CFD_LOCAL_PAPER`. El sink persiste `cfd_trades`, conserva
su `paper_analysis_id` en el checkpoint y exige bid/ask explícitos, ordenados y
`VALID`; no crea un ejecutor, no envía órdenes y no convierte un fill local en
un fill del servidor. La fixture de CLI produjo 3 fills y 3 cierres sobre 700
mensajes sintéticos; receipt
`../runtime/market-evidence/ctrader-watch-paper-fixture-20260915.json`
(SHA-256 `c05ced74fb3578b50af9897911b8980486832bb6a922112e855e2c81c773be00`).

La canaria conectada de sólo lectura reutilizó el OAuth DEMO vigente sin
intercambio/refresh ni órdenes. El servidor entregó 27 eventos, pero el feed
observado conservó `CROSSED`/bid=ask y una actualización parcial; el sink
registró 0 fills y mantuvo el análisis bloqueado. Receipt compacto:
`../runtime/market-evidence/ctrader-watch-live-paper-canary-20260915.json`
(SHA-256 `cb99fad8cb09726a9616c7b9c5c358d2a2bcec7df0b30f4ad2fa2b360f4f9b95`).

La misma ruta en el runtime V27 `STAGED/NOT_PROMOTED` recibió 2 cotizaciones
DEMO completas (`bid < ask`, calidad `VALID`) y las ingirió en PAPER; no hubo
señales porque el arranque quedó en calentamiento/`partial_bucket`, por lo que
el resultado correcto fue `fills=0`, no un fill inventado. Receipt:
`../runtime/market-evidence/ctrader-watch-live-paper-canary-v27-20260915.json`
(SHA-256 `dce8d1bae3df40f4e6d583989172aae85c27f23576fa590de81e88ab15d1870d`).

El adaptador también expone `fetch_tick_data()` para
`ProtoOAGetTickDataReq/Res`: BID y ASK se solicitan por separado, se
reconstruyen sus timestamps y precios acumulativos y se limita la ventana a
siete días. El resultado conserva páginas/receipts, pero no fabrica un BBO
uniendo dos solicitudes. La canaria de lectura observó 386 ticks BID y 386 ASK,
una página por lado, completas y sin incidencias, en
`../runtime/market-evidence/ctrader-tick-data-read-only-canary-20260915.json`
(SHA-256 `c3abd241781c2f6742b9610f9e0d90a61f0f7187c11a1198f7d12f106f45053f`).

### Evolución H0–H6 local — 2026-09-13

La [matriz de implementación y aceptación](demo_reliability_plan.md) describe
los cambios posteriores: economía CFD v2, investigación causal, codec generado,
gestión de riesgo y supervisor deshabilitado. Los receipts históricos de abajo
no validan estos bytes nuevos. No se publicó ni instaló una release operativa
como parte de esta evolución y no se volvió a consultar el portal.

El codec candidato usa Protobuf 7.36.1 y schemas Spotware 91, sin SDK antiguo.
La presencia del SDK anterior en `.venv` no prueba utilidad: Protobuf 3.20.1
queda rechazado. La reparación SQLite WAL-reset, la resistencia real de 72
horas y las 30 sesiones DEMO son gates separados de la implementación local.

### Preparación externa DEMO — 2026-09-12

Se verificó en el navegador una sesión cTrader ID autenticada y la cuenta
Pepperstone DEMO vinculada. Esta observación de la interfaz no equivale a una
sesión Open API de MTF. La aplicación **MTF Lab** se registró y el portal confirma
**`Submitted`** (en revisión), todavía no **`Approved`**. Se verificó que el contacto
autorizado quedó guardado; no se duplican aquí sus datos personales. Víctor autorizó los términos de Open API y la comunicación del contacto
a Spotware y a los brokers para soporte; no debe repetirse ese consentimiento
mientras no cambie su alcance.

El perfil inicial es `config/ctrader_query.toml`, exclusivamente **DEMO** y
scope **`accounts`**. El callback configurado es
`http://127.0.0.1:8767/oauth/callback`. El doctor local observa el SDK, pero informa
`APP_CREDENTIALS_REQUIRED`; el portal ya ofrece credenciales de aplicación, pero
no se han exportado a almacenamiento local ni realizado OAuth para MTF. Cualquier cuenta REAL/LIVE queda fuera de esta tarea;
no se habilitan órdenes ni se mueven fondos. La aceptación de Open API no se
presenta como validación contractual general de Pepperstone.

#### Preparación local verificada

La integración de consulta de lectura e importación del histórico nativo quedó
validada con **422/422 pruebas**, sin fallos ni omisiones, Ruff/formato, mypy y
Pyright en verde. La suite se ejecutó con HOME/XDG/TMP/estado aislados y cero
intentos de red externa; los imports/smokes no activaron el SDK por defecto.
El [receipt compacto](../reports/quality/ctrader-demo-preparation-20260912.json)
conserva la identidad de los inputs y las métricas del gate final. En el cierre inicial, los cambios
quedaron locales y todavía sin commit, publicación ni instalación de release;
ese receipt describe la preparación anterior a su consolidación en Git.

El intercambio y refresh mantienen el token en memoria hasta verificar el eco
del servidor, el permiso efectivo, los endpoints y la generación de conexión.
La consulta revalida esa evidencia fresca: esta fase rechaza permisos de trading
no pedidos e inventarios con cuentas REAL/LIVE. El discovery público permanece
sin tokens; el acceso al eco sensible es explícito e interno a la verificación.

El histórico se exporta con metadatos mínimos de cuenta/entorno/especificación,
archivos privados y publicación sin reemplazo. La reproducción exige
`price_base=native` y `order=market_time_corrected`, conserva la recepción original
y no fabrica bid/ask ni fills. Las capturas parciales, vacías o incompatibles no
se presentan como análisis completo. La ruta y los comandos están en el README.
Esa preparación de 422 pruebas no incluía aún el runner continuo. **La conexión
real de MTF por Open API sigue pendiente de aprobación y OAuth**.

### Runner y observabilidad offline — 2026-09-12

La base anterior se consolidó en el commit local `9d066c6`, sin publicarla ni
instalar una release. El alcance posterior añade `ctrader watch`, composición
de lectura del proveedor existente con runtime MTF, límites, checkpoint
atómico, reanudación exacta y parada cooperativa. La UI separa procedencia,
frescura y estado de feed, y hace visibles los fallos de refresco. El
[manual de uso](ctrader_watch.md) documenta fixtures y límites de recuperación.

Este alcance no usa OAuth, cuentas ni conexión real al bróker. La ruta externa
se conserva para activación posterior; el compromiso de conexión DEMO no se
cierra con una validación sintética.

El commit local de implementación `f6bcb1e` pasó el gate global con **457/457
pruebas**, cero fallos/omisiones/intentos de red externa, Ruff/formato, mypy,
Pyright y auditoría arquitectónica en verde. Los fuentes permanecieron
idénticos durante la corrida. El [receipt de esta ampliación](../reports/quality/ctrader-watch-observability-20260912.json)
incluye identidad, cobertura, revisión y QA visual: pausa/continuidad/warmup
correctos, error visible, datos previos conservados y recuperación automática.
El cierre posterior sólo agrega documentación/evidencia; no publica ni instala
una release.


### IMPLEMENTADO Y COMPROBADO OFFLINE

- La fachada `mtf_lab.data.ctrader` separa configuración, Protobuf/codec,
  transporte, sesión, cuentas, mercado y fixtures. Importar el dominio o usar
  `--help` no carga un transporte externo.
- El lector por generación, framing, correlación, heartbeat, backpressure,
  reconexión y causas de cierre/EOF se prueban con transporte controlado.
- `CaptureEnvelope` v1 conserva `event_time`, `received_at`, `available_at`,
  secuencia global y generación. `CTraderPaperSession` mantiene detector y CFD
  durante ingesta, avance, checkpoint, restauración y cierre, con replay causal
  e idempotente.
- La calidad bid/ask, frescura, timestamps, cruces, discontinuidades y
  cantidades Decimal llegan al fill. Trendbars usan base `native`; no se
  reinterpretan como operaciones negociadas.
- SQLite schema v4 separa `cfd_trades` del producto binario y conserva datos v3
  como evidencia legacy. La recuperación y los límites de retención son
  explícitos; no se descartan estados activos silenciosamente.
- La composición local cubre RuntimeCoordinator, señal, gates de riesgo/safety,
  intent durable, construcción de `ProtoOANewOrderReq`, eventos de ejecución,
  reconciliación, cierre y persistencia mediante Protobuf instalado y gateway
  falso/local. `ctrader fixture` y `cfd-paper` son fixtures sintéticos.
- La instalación, los imports, los defaults TOML empaquetados, los lanzadores,
  el fixture CFD, la UI de sólo lectura y el gate JavaScript se validan sin
  credenciales ni red de bróker.

Las pruebas y fixtures no demuestran rentabilidad, ventaja estadística,
condiciones de ejecución ni tarifas. Las cotizaciones sintéticas no son
redistribución ni observación de mercado real.

### IMPLEMENTADO, PENDIENTE DE VALIDACIÓN EXTERNA

El código y los contratos para autenticación de aplicación, descubrimiento de
cuentas, selección DEMO explícita, autorización de cuenta, catálogo/símbolo,
market data y transporte DEMO están implementados y probados localmente. La
prueba local no sustituye una respuesta de cTrader: quedan sin observar los
permisos y entorno de la cuenta, símbolos habilitados, escalas, límites de
volumen, horarios, spreads, comisiones, conversiones, fills, posiciones,
historia y respuestas de aceptación/rechazo. Un timeout ambiguo permanece
`UNKNOWN` y exige reconciliación; no autoriza reintentar una orden.

La ejecución externa permanece deshabilitada y **REAL/LIVE se rechaza
fail-closed**. Ningún fixture prueba que una orden haya llegado a un servidor.

### ETAPAS EXTERNAS PENDIENTES

Los únicos bloqueos externos de esta fase son:

1. Obtener la aprobación de Spotware para la aplicación ya registrada.
2. Disponer fuera del repositorio de `CTRADER_CLIENT_ID` y
   `CTRADER_CLIENT_SECRET`.
3. Completar OAuth y consentir el alcance requerido.
4. Descubrir las cuentas realmente autorizadas y seleccionar explícitamente una
   cuenta **DEMO**.
5. Verificar catálogo, condiciones del bróker y la ruta de lectura autorizada
   contra el servidor **DEMO**; cualquier ejecución exige otro alcance expreso.
6. Confirmar las condiciones contractuales de Pepperstone aplicables a México,
   almacenamiento/redistribución de datos y costes; no están verificadas.

Estos puntos son hechos, decisiones o autorizaciones externas, no tareas de
programación pendientes. No se solicitan secretos ni se inicia autenticación
para reproducir el gate offline.

## Referencias de protocolo

- [Open API cTrader](https://help.ctrader.com/open-api/), [endpoints](https://help.ctrader.com/open-api/proxies-endpoints/), [conexión](https://help.ctrader.com/open-api/connection/), [autenticación](https://help.ctrader.com/open-api/account-authentication/) y [datos de símbolos](https://help.ctrader.com/open-api/symbol-data/).
- [Contrato Protobuf de presencia de campos](https://protobuf.dev/programming-guides/field_presence/).
- [Mensajes cTrader](https://help.ctrader.com/open-api/messages/) y [modelos](https://help.ctrader.com/open-api/model-messages/).
- [Contrato `Decimal` de Python](https://docs.python.org/3/library/decimal.html).

El [borrador de consulta Pepperstone](pepperstone_ctrader_openapi_draft.md) es
sólo un borrador local y no acredita validación contractual ni fue enviado por
esta entrega.

## Cierre de implementación local H0–H6 — 2026-09-13

Código validado: `c1246cc173725e9dc22ae813e3f2f9b6f96bf5e6`, **653/653 pruebas**,
codec generado operativo en el runtime candidato, wheel reproducible y smoke
instalado aislado. El [comprobante local](../reports/quality/mtf-demo-reliability-20260913.json)
y la [matriz H0–H6](demo_reliability_plan.md) detallan alcance y evidencia.
Esto cierra `MTF-REL-001`, no la operación desatendida: `MTF-VAL-001` conserva
72 horas reales, resolución/verificación SQLite y aceptación de piloto; las
30 sesiones DEMO no se realizaron. No hubo nueva lectura del portal, OAuth,
ampliación de scopes, conexión API/órdenes, push ni instalación operativa.
`MTF-DEM-001` mantiene su estado externo por separado; no se reinterpreta la
observación histórica de la aplicación como aprobación vigente.

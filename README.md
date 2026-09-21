# MTF Lab

Plataforma local, determinista y Linux-first para investigar la relación de un
instrumento en varias temporalidades y ejecutar simulaciones virtuales. El
recorrido principal es:

`fuente -> normalización/calidad -> M1/M5/M15 -> EMA/RSI/ATR -> trend_pullback_v1 -> persistencia -> simulación/backtest -> reporte/UI`.

El núcleo no llama a un LLM ni a OpenAI. Las señales, simulaciones y fixtures
son hipótesis y datos sintéticos: **no son recomendaciones, órdenes ni
prueba de rentabilidad**.

## Contexto durable (sin memoria nativa)

Este README y los documentos enlazados son la fuente persistente del proyecto. Los límites de activación están en `docs/activation_boundaries.md`, el estado de cTrader en `docs/ctrader_integration_status.md` y los parámetros efectivos en `config/*.toml`; no agregues texto libre ni una sección `[knowledge]` a los TOML porque el cargador rechaza claves desconocidas.

## Estado actual

### Canaria técnica DEMO — 2026-09-21

El código validado, publicado e instalado es
`0a6adfb5b9bf1915edb43399495ae10e08ae9707`: **1,315/1,315 pruebas**, cero
fallos/omisiones, 80.463% de líneas y 64.290% de ramas. La instalación usa
`run_id=20260921T231515Z-61e48a36` y 132 archivos/RECORD byte-equivalentes en
ambos entornos, con tres launchers verificados.

La canaria permanece `CURRENT_REVIEW`/`NO_EJECUTADA`: el controlador terminó
`OBSERVED_INPUTS_BLOCKED` por falta de ATR válido para el timeframe trigger,
sin órdenes ni ciclos. El detalle y los receipts están en el [estado operativo vigente](docs/operational_status.md#estado-vigente--2026-09-21).

Las dos excepciones humanas siguen acotadas a esta canaria: `bid == ask`
genuinamente observado y ancla de equity inicial del trial. La regla predeterminada estricta
permanece fuera de ellas; no se habilitan REAL, estrategia, 72 horas ni otra
fecha/automatización.

### Desarrollo aislado — 2026-09-20

La segunda iteración de `codex/mtf-offline-hardening-20260920` corrigió los
13 hallazgos de riesgo, causalidad, recuperación, archivos, autenticación y UI,
además de reforzar la auditoría Python de escrituras y los reportes paginados.
El código `ca3ebaa6806106465a3a4662df4ed71c541b913d` pasó **1,217/1,217 pruebas**,
incluidas 68 nuevas, sin fallos ni omisiones. El [cierre de calidad](docs/refactor_quality.md)
conserva los resultados y límites de aquella etapa, que se cerró sin integrar
ni instalar. La promoción posterior se distingue en el bloque siguiente; no
convierte la validación DEV en evidencia de ejecución DEMO.

### Actualización operativa — 2026-09-20

**Antecedente fechado; no describe la instalación ni una programación vigente.**

El código `968bf0e7439b878e4b2d403d9b00bba9c7186223` pasó **1,219/1,219 pruebas**,
sin fallos ni omisiones, y quedó integrado y publicado en `main`. El runtime
canónico quedó entonces `ACTIVE_CANONICAL`, con `run_id=20260920T162153Z-f4c62495`,
130 archivos del paquete byte-equivalentes y rollback inmediato conservado.
Python 3.12.14, SQLite 3.53.1 y Protobuf 7.36.1 permanecen sin cambios; los tres
launchers de usuario y los imports instalados pasaron el smoke aislado desde
un cwd ajeno. La identidad de esta entrega es el SHA y el `run_id`, no un
cambio del número de paquete `0.1.0`.

El [estado operativo](docs/operational_status.md) conserva los receipts de
esa entrega y los distingue del estado actual. El plan original preveía una
oportunidad el lunes 21, 09:00–09:20 America/Mexico_City, que no se lanzó.
Su heartbeat fue eliminado y la continuación del mismo día procede de la
petición posterior de Víctor, no de una extensión automática de ese plan.
Durante la preparación del 20 no hubo órdenes, OAuth nuevo ni activación
de operación continua/REAL.

### Actualización operativa — 2026-09-19

**Antecedente del runtime anterior; no describe la instalación vigente.**

El [estado operativo y preservación](docs/operational_status.md) y su
[receipt compacto versionado](reports/quality/mtf-operational-status-20260919.json)
conservan los resultados de ese cierre sin subir secretos, capturas ni corpus.
El código validado y publicado por fast-forward es
`b162e0f1a52e272268bf3bd9ae1631e5cae688a0`; `main==origin/main` fue verificado.
El gate terminal v2 está en
`runtime/market-evidence/canary-preparation-20260919T215251Z/quality-gate-v2.json`
(SHA-256 `fbcb473977577e5e76b73d8f4dc8c203184d7aba121b10350480644e072922eb`):
`ok=true`, 1,125/1,125 pruebas, 0 `failed`/`skipped`, 79.915% de líneas,
63.291% de ramas, 0 red/escrituras externas, `source_integrity=PASS` y suite de
1,567.154 s.
El runtime canónico está `ACTIVE_CANONICAL`, el process scan terminó en
`COMPLETE`, fue promovido con rollback conservado y su paquete de código conserva
128 archivos byte-equivalentes (`run_id=20260919T225409Z-4d1675aa`).
Los launchers de usuario quedaron instalados en
`~/.local/bin/mtf-lab` y `~/.local/bin/mtf-lab-ctrader-query`; ambos `--help`
se probaron desde un cwd ajeno, con HOME/XDG/TMP aislados. La canaria también
instaló `~/.local/bin/mtf-lab-demo-canary` con modo `0700`; su `--help` aislado
pasó y sus imports provienen del Python instalado (3.12.14, SQLite 3.53.1,
Protobuf 7.36.1). Los launchers usan sólo referencias privadas de credenciales:
ningún valor secreto está en el repositorio.
El portal de Spotware muestra **MTF Lab: Active**, con callback local configurado;
esto es un estado administrativo y no una autorización amplia de trading. La
lectura DEMO usa la autorización `accounts` existente. El servidor DEMO concedió
`TRADING` únicamente para la cuenta aprobada (sufijo `5097`) y el alcance VIEW
original permanece intacto; aún hay 0 órdenes. El histórico `201910` sigue
rechazado: una nueva descarga oficial devolvió exactamente los mismos bytes
inválidos. La prueba del sábado recibió precios del viernes; no prueba una
avería del feed ni ausencia de credenciales. La frescura debe observarse en una
sesión de mercado abierta.

**La campaña fue reanudada por Víctor el 2026-09-13 mediante una instrucción
humana explícita.** El [handoff de pausa](docs/handoffs/2026-09-13-market-evidence-pause.md)
conserva el punto anterior, los cambios sin commit, los artefactos y la ruta crítica.
La promoción técnica actual no equivale a una release de trading, aceptación
contractual ni conclusión de rentabilidad.

El cierre anterior del código `48ef12a687f74f7050147e3ea71d704b3ce8b358`, con su
gate de 1,057/1,057 pruebas, se conserva como antecedente; no representa el
`HEAD` vigente.

La nueva [campaña histórica de evidencia](docs/market_evidence_plan.md) está en
integración. Su [piloto real y límites](docs/market_pilot_status.md) registra
la semana/mes descriptivos de marzo de 2016, distintos de los fixtures y de la
entrega H0–H6. Se conserva el año 2016 completo de HistData (12 ZIPs y un
manifiesto compuesto con 19,026,438 ticks). Además, 2017 y 2018 están completos
y validados: 2017 contiene 14,125,996 cotizaciones y 2018 contiene 18,393,327.
En 2019 hay 11/12 meses válidos (`201901–201909`, `201911–201912`), con
26,877,692 cotizaciones; `201910` está preservado, pero falla validación por
una regresión de orden temporal y no se cuenta como válido. No existe todavía
manifiesto anual de 2019 ni compuesto `2016–2019`. El piloto descriptivo
documentado sigue siendo sólo marzo; el QA anual descriptivo de 2017 y 2018
terminó sin red ni estrategia y conserva receipts JSON/HTML separados. La QA
descriptiva estricta de los 11 meses válidos de 2019 (`201901–201909`,
`201911–201912`) también terminó por manifiesto mensual, sin componer un año ni
incluir `201910`; su receipt consolidado es
`runtime/market-evidence/qa-descriptive-2019-valid-months-20260919T133407Z.json`
(SHA-256 `1624c41736b423f334fa447d8b40545f7fa1df2701bbc76aebcb9994a94dbeab`).
El piloto `tp_fast_v1` cubrió la semana 2016-03-07→14 con 499,804 cotizaciones
por escenario: seis cierres base `2W/4L` dieron `net=-0.64 USD` de modelo,
el escenario `adverse14` `net=-1.73` y `extreme14` `net=-2.20`; cada ventana expirada
conservó un intent `UNKNOWN`. El resultado no se acepta globalmente: los costes
son de modelo explícito, no cargos históricos observados, y no demuestra
rentabilidad ni selección. El V17 previo tuvo 0 `RiskExit` fills evaluables;
2017–2019 conserva sólo QA descriptivo, sin estrategia.
La QA descriptiva no abrió WF/holdout ni evaluó estrategia. El
backtest anual V17 terminó en modo
`COMPLETED_DEVELOPMENT_REVIEW_ONLY`: procesó 19,026,438 cotizaciones y conserva
un receipt terminal; la auditoría de terminalidad quedó en
`runtime/market-evidence/development-2016-v17-terminal-audit-20260917.json`;
esto no es un resultado económico ni una release.
Antes de ese backtest se completó un canario de capacidad de 1,048,576
cotizaciones sobre el manifiesto 2016; midió 45:42 de pared y RSS máximo de
263,488 KiB con la cadencia durable de 65,536 cotizaciones, sin selección,
promoción ni conclusión económica.
La ingesta puede componer particiones mensuales de desarrollo 2016–2019
sin mezclar bytes ni saltarse meses. La adquisición validada de 2017 y 2018 no
abre WF 2020–2023: todavía no hay datos WF ni consumidor productivo habilitado.
La partición inválida de 2019 se conserva sin corregir, ordenar artificialmente
ni relabelar.
Los costes desconocidos se conservan como `UNKNOWN_NOT_ZERO`; una especificación
de costes no equivale a cargos observados y la cobertura aún no permite una
conclusión de ventaja neta;
ningún resultado descriptivo habilita órdenes ni abre el holdout reservado.
El backtest ya enlaza un guard de identidad y un plano compartido de datos;
ambos tienen pruebas focales, y ahora el checkpoint parcial/resume continuable
también está verificado. La adquisición cuenta además con un guard agregado
de raws y reserva de filesystem, probado sin tocar el corpus. El procesamiento conserva bloques de 2,048
cotizaciones y, por defecto, sólo serializa un snapshot durable cada 32 bloques
(65,536 cotizaciones); una parada parcial fuerza el snapshot inmediato y la
cadencia forma parte de la identidad que se valida al reanudar. Todavía no
equivale a la puerta global ni a la validación económica/DEMO. El servicio de campaña mantiene el holdout
cerrado: exige permiso tipado no secreto y no permite relabelar el contrato
HistData de desarrollo (incluido el año 2016) como datos 2024–2025.
El contrato aislado de WF 2020–2023 ya tiene tipos, identidad, límites causales
y receipts fail-closed; la fixture offline `WARMUP_ONLY → WF` con reanudación
byte-equivalente ya está validada en
`runtime/market-evidence/wf-warmup-resume-fixture-20260915.json`. Todavía no
hay datos WF ni consumidor productivo ejecutable: el runner sólo enlaza el
contrato en preflight y cierra antes de leer datos. El gate offline anterior del
código `48ef12a687f74f7050147e3ea71d704b3ce8b358` pasó 1,057/1,057 pruebas, sin
red ni escrituras externas, con 79.906% de líneas y 63.307% de ramas; su receipt
se conserva como antecedente en
`runtime/market-evidence/operational-closure-20260919T183148Z/quality-gate.json`.
La validación del código `b162e0f1a52e272268bf3bd9ae1631e5cae688a0` se
conserva como antecedente del cierre del 19 de septiembre; no se duplica aquí
su receipt.
La última referencia
global reproducible completa, previa a los cambios locales warmup/health y al
refactor de reporting, pasó 990/990 pruebas sin omisiones, con
líneas/ramas 79.871%/62.989%; su receipt está en
`runtime/market-evidence/quality-gate-20260915-ctrader-paper-tick-v3.json`. El gate
histórico bounded previo pasó 964/964 pruebas, con líneas/ramas
79.732%/62.750%; su receipt se conserva en
`runtime/market-evidence/quality-gate-20260915-historical-paper-gate-v3.json`.
La ruta económica conserva costes desconocidos como `UNKNOWN_NOT_ZERO` y sólo
acepta una especificación explícita para calcular neto; esa especificación no es
evidencia de cargos observados. Aún faltan desarrollo multiaño, holdout,
resistencia y validación DEMO de ejecución/forward.

El lifecycle del runtime privado está definido en
[`docs/runtime_lifecycle.md`](docs/runtime_lifecycle.md). El único runtime
canónico es `~/.local/share/mtf-lab/runtime`; las reviews se crean como
hermanas bajo `~/.local/share/mtf-lab/` (`runtime-review-.../runtime`), se
reconcilian con lock y se recogen de forma conservadora. El comando
`mtf-lab runtime inspect|gc|promote` es una superficie administrativa
complementaria; no toca el estado de investigación.

La evolución H0–H6 hacia una DEMO fiable se documenta en la
[matriz de implementación y aceptación](docs/demo_reliability_plan.md). Su
validación local, la instalación operativa y las ventanas de 72 horas/30
sesiones son estados distintos. Ningún comando nuevo activa trading por defecto.

El runtime privado canónico quedó reconstruido y promovido de forma explícita
desde una review validada: `/home/winterboss/.local/share/mtf-lab/runtime`.
En ese cierre quedó `ACTIVE_CANONICAL`, con process scan `COMPLETE`, rollback
conservado y `run_id=20260919T225409Z-4d1675aa`; Python 3.12.14, SQLite 3.53.1 y
Protobuf 7.36.1 pasan el smoke aislado y `pip check`. Las 28 reviews históricas
se limpiaron con el GC seguro; sus manifests y logs acotados quedaron en
`runtime/market-evidence/runtime-review-legacy-evidence-20260917/index.json`.
El runtime activo y el estado de investigación no forman parte de esa limpieza.

### IMPLEMENTADO Y COMPROBADO OFFLINE

- Ingesta local/sintética, normalización causal, M1/M5/M15, indicadores
  EMA/RSI/ATR y `trend_pullback_v1`, con calidad, calentamiento, huecos,
  duplicados, disponibilidad y procedencia separados.
- Capturas cTrader v1, replay `as_observed`, secuencia global, generación de
  conexión, checkpoints, restauración, idempotencia y consumidores explícitos
  de señales.
- Calidad bid/ask hasta el fill, base `native` para trendbars, cantidades y
  contabilidad `Decimal`, simulación CFD PAPER incremental y persistencia SQLite
  schema v4 con migración aditiva desde v3.
- Codec/framing Protobuf generado desde schemas oficiales y probado en un
  runtime aislado mantenido; transporte DEMO controlado,
  correlación, heartbeat, backpressure, respuestas de ejecución, reconciliación
  y rechazo fail-closed de REAL/LIVE. Las pruebas usan gateway/transporte
  local o falso; no contactan el bróker.
  El venv operativo anterior no se actualiza con estos cambios y debe pasar
  `ctrader doctor` antes de cualquier uso externo.
- Instalación editable y wheel, imports desde cwd ajeno, configuraciones TOML
  empaquetadas, lanzadores, fixtures CFD/cTrader, UI de sólo lectura, suite
  offline, lint/formato/tipos y auditoría arquitectónica.
- Runner `ctrader watch` de lectura, acotado y reanudable, con sink local
  `FOREX_CFD_LOCAL_PAPER`, persistencia/checkpoint de `cfd_trades`, fixture de
  fills deterministas y panel de procedencia/frescura/estado/errores. La base
  `native` admite warmup causal cerrado M1/M5/M15 y suscripción conjunta de
  spots/trendbars; PAPER sólo usa bid/ask válidos de SpotEvent. El
  [manual de observación continua](docs/ctrader_watch.md) separa el uso offline
  de la validación externa y operación aún pendientes.
- Lector `ProtoOAGetTickDataReq` para BID/ASK históricos independientes, con
  deltas acumulativos, ventana máxima de siete días y sin construir pares BBO
  entre solicitudes separadas.
- Guard agregado de raws HistData/Dukascopy con lock común, límite proyectado de
  40 GiB, reserva mínima de 20% y preservación de parciales; contrato aislado
  de walk-forward con warmup causal, ventanas exactas y holdout fail-closed;
  fixture causal offline y resume byte-equivalente en
  `mtf_lab/data/walk_forward_fixture.py`.

El alcance vigente de calidad está en [calidad global](docs/refactor_quality.md).
La evidencia histórica de integración offline está en el [informe de ingeniería](reports/engineering/latest/engineering_consolidation.md),
[engineering_results.json](reports/engineering/latest/engineering_results.json) y
[engineering_tooling.json](reports/engineering/latest/engineering_tooling.json).
La [matriz de fronteras A/B/C](docs/activation_boundaries.md) distingue lo que
se comprueba localmente de lo que sólo puede observar un servidor autenticado.

### IMPLEMENTADO, PENDIENTE DE VALIDACIÓN EXTERNA

Los contratos y la composición para autenticación de aplicación, descubrimiento
de cuentas, selección DEMO explícita, autenticación de cuenta, catálogo,
market data, RuntimeCoordinator, señal, gates de seguridad, intent durable,
`ProtoOANewOrderReq`, eventos, reconciliación, cierre y persistencia están
implementados y cubiertos localmente con Protobuf real y un gateway controlado.
La validación local no demuestra permisos, catálogo, límites, spreads,
comisiones, conversiones, fills, posiciones, historia ni respuestas de un
servidor cTrader real. Un timeout ambiguo permanece `UNKNOWN` y exige
reconciliación; no autoriza reintentar una orden.

### Estado vigente de lectura DEMO — 2026-09-19

La aplicación figura **Active** y la lectura DEMO conserva la autorización
`accounts`, la cuenta DEMO seleccionada y un catálogo observado de 1,940
símbolos. La consulta histórica del launcher conservó 10,000 barras M1 en 20
páginas, pero la fuente indicó `has_more=true`: es `PARTIAL`, no una serie
completa, y no contiene bid/ask ni fills. No se inició OAuth nuevo ni se enviaron
órdenes.

La ventana bounded vigente cubre 60/60 barras M1 nativas, `COMPLETE`/`CONTINUOUS`
y cero gaps. Esa cobertura sólo cierra la ventana solicitada: el `has_more=true`
de la consulta fuente no se convierte en histórico completo y la captura no
acredita frescura live, BBO, ejecución ni rentabilidad. Los launchers instalados
usan una referencia privada de credenciales; el launcher de consulta no habilita
trading. La canaria de software está instalada como
`~/.local/bin/mtf-lab-demo-canary`; su preflight de sólo lectura contra el servidor
real del 19 de septiembre pasó identidad, `TRADING` y catálogo, y cerró con
`NETWORK_PREFLIGHT_INPUTS_REQUIRED`. Las preparaciones del 21 de septiembre
ya observaron riesgo y mercado, pero se detuvieron antes de órdenes; conservan
high-water y ancla diaria no verificada, con journal vacío.

### Autorización de canaria técnica DEMO — 2026-09-21

La ventana siguiente fue la oportunidad original, que no se lanzó. La
continuación autorizada del mismo día se distingue en el estado actual de arriba.

Víctor aprobó una canaria manual acotada para EUR/USD en la cuenta DEMO de sufijo
`5097`, el lunes 2026-09-21 de 09:00 a 09:20 hora de México (inicio 15:00 UTC):
dos ciclos independientes: primero 1 BUY y su cierre, después 1 SELL y su
cierre, hasta 1,000 unidades (0.01 lot), una sola posición, SL de 1.5 ATR, TP de
3 ATR, riesgo planeado de hasta 0.05% por ciclo, stop de prueba de 0.1% del
equity, holding de hasta 300 s y cuatro mutaciones nominales (máximo seis).
El alcance `TRADING` fue concedido por el servidor DEMO sólo para esa cuenta; no
se repite OAuth ni se cambia la cuenta seleccionada, pero cada conexión exige
discovery fresco de lectura y verificación de esa cuenta. Esto no autoriza REAL,
estrategia, operación continua, extensión automática ni reintento de un resultado
ambiguo. Es un gate técnico previo a cualquier histórico completo o ventana de 72
horas, no una selección de estrategia. El software está instalado. El
preflight readonly del 19 de septiembre verificó identidad, `TRADING` y
catálogo; las preparaciones del 21 ya observaron riesgo y mercado, sin llegar
a órdenes. El heartbeat de la oportunidad original fue eliminado.
La continuación autorizada del mismo día mantiene los límites anteriores y
permite la referencia inicial de pérdida sólo para el trial. La autorización
específica de esta misma canaria también permite aceptar un `bid == ask`
genuinamente observado, únicamente dentro de la cuenta, sesión, generación,
ventana y límites tipados anteriores. La regla predeterminada estricta
permanece sin cambios fuera de esta canaria y `bid > ask` continúa rechazado.
La aprobación y el ledger impiden extender o repetir un trial reservado. El
estado operativo vigente conserva el resultado terminal sin órdenes y la
evidencia de instalación.

### Antecedente: validación conectada DEMO de sólo lectura — 2026-09-14

Este bloque conserva la evidencia histórica de esa captura; el estado vigente es
el de la sección anterior.

La evidencia privada, fuera del repositorio (SHA-256
`a2d6af011fa43b05187c2aace05f6b5c51e592dc9a5e506f872b4cf1f43ac72e`; no se
reproduce el identificador concreto), confirma OAuth vigente con
`SCOPE_VIEW`/`accounts`, una cuenta DEMO Winter, sin cuenta REAL ni scope de
trading. No se repitió OAuth.

La consulta conectada de sólo lectura verificó conexión/autenticación,
descubrimiento, catálogo de 1,940 símbolos y EUR/USD. El receipt
`runtime/market-evidence/connected-v18-20260914T195151Z.json` (SHA-256
`168ef960a9aa2ef0bf10263045bbd3652e5683ace036524bc4e17878c78faf49`) conserva
9,999 barras M1 nativas en 20 páginas, pero
`hasMore=true`: la captura es `PARTIAL`, no una serie completa, no contiene
bid/ask y produjo cero fills PAPER. El contexto de periodo a nivel de respuesta
para trendbars periodless quedó corregido y validado; las discrepancias siguen
siendo fail-closed.

La observación continua DEMO quedó bloqueada por evidencia real de
`available_at < event_time` y una cotización `CROSSED`/bid=ask. Se conservó un
evento como evidencia, sin reordenar, clamping, dato sintético, reintento ni
orden. La captura histórica parcial se rechaza antes de `cfd-paper`; el gate
V19 está en `runtime/market-evidence/connected-v19-partial-gate-20260914T2132Z/receipt.json`
(SHA-256 `cb18e425f07581b0c12a3724de2123f65b4ff00ebeb28f995637392312ba3d10`),
y el receipt V18 original se conserva como antecedente.

La ruta offline sí recorrió captura → indicadores/señal → PAPER → SQLite →
reporte y reanudación byte-equivalente (190 barras sintéticas, una señal y tres
trades PAPER cerrados), en
`runtime/market-evidence/paper-report-e2e-20260914T194812Z-v18/paper-report-e2e-receipt.json`.
Es evidencia de integración local, no de mercado, frescura, costes, fills o
rentabilidad reales.

#### Antecedente: diagnóstico de lectura acotado — 2026-09-15

Se reutilizó el OAuth DEMO existente (`accounts`, `SCOPE_VIEW`) sin repetir
OAuth, sin órdenes y sin abrir SQLite. La captura de 12 `SpotEvent` está en
`runtime/market-evidence/ctrader-diagnostic-20260915T004049Z/receipt.json`
(SHA-256 `bdac08161c9bfd726c43b30b5cf802092c79da805e7f6b3fc4e3afe934eadcf5`);
su evidencia de campos tiene SHA-256
`fd77a9c2d53aad880e4057c972688ba03e7f293bddf4e9f826c34319deb1f51f`.
Los 12 eventos conservaron `timestamp` original en ms, `symbolId`, `bid` y
`ask` crudos como enteros relativos (escala 100000, 5 dígitos, pip 4), sin
actualización parcial. Los 12 llegaron con `bid == ask`; ninguno con `bid > ask`.
La regla conjunta vigente `bid >= ask -> CROSSED` los dejó observados pero no
operables, sin relabelar ni alterar precios. La igualdad procede de los campos
crudos; no hay evidencia de que la composición de piernas o la normalización la
haya creado.

En esta ventana `available_at < event_time` no ocurrió (0/12), mientras que el
receipt V18 conserva el caso anterior de `-1.576856 s`. El reloj local reportó
NTP sincronizado antes/después y no se modificó. La evidencia descarta una
mutación de timestamps por el normalizador, pero no contiene una referencia
independiente del reloj del servidor; por ello la atribución servidor-versus
desfase local sigue sin desambiguar. No se aplicó corrección, clamping,
reordenamiento ni relajación del gate; las regresiones de igualdad y timestamp
futuro quedan en `tests/test_ctrader_quote_quality.py`.

#### Antecedente: captura histórica DEMO bounded V22 — 2026-09-15

La captura acotada existente se conserva en
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/window-receipt.json`;
su raw EUR/USD M1 `native` es
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/history-native-bounded.jsonl`,
SHA-256 `80022a3890ee6f3a867819e4c4b19b72cf5c2dfb71b18714d37464aa330f0dad`.
Registra 1,327 barras/3 páginas, ventana
`2026-09-09T22:52:00Z`–`2026-09-10T20:59:00Z`, sin gaps ni incidencias; no
contiene bid/ask ni quote events y produjo `0` fills PAPER. La captura fue de
sólo lectura DEMO: cero órdenes y SQLite no abierto.

La fuente original conserva `source_has_more=true` (3,000 barras/6 páginas),
pero la selección bounded es completa para esa ventana
(`bounded_complete=true`, `bounded_has_more=false`), sin convertir la fuente en
serie completa. El gate continuity/coverage exige marcador bounded en páginas y
metadatos y barras M1 adyacentes por `event_time`; de lo contrario permanece
`UNKNOWN`/fail-closed. El fix compatible se probó sin red sobre el raw legacy:
el reporte
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/cfd-paper-v22-final.json`
y su reanudación con el mismo input son byte-equivalentes (SHA-256
`3beab3d08ace024fd812a3812420db4aadaed943953f9cc87a65ccb1dc0cdda0`), con
`coverage_satisfied=true`, `CONTINUOUS`, 142 señales y `paper_capture_complete=true`.
No hubo bid/ask: los 426 intents PAPER quedaron `UNKNOWN` por falta de cotización
y hubo `0` fills `FILLED`; no se infiere rentabilidad ni ventaja. El receipt
compacto es
`runtime/market-evidence/ctrader-real-history-paper-e2e-20260915.json`.

### ETAPAS EXTERNAS PENDIENTES

La lista queda limitada a hechos, decisiones y autorizaciones externas:

1. La observación administrativa del 2026-09-19 muestra `Active` y el callback
   local correcto, sustituyendo `Submitted` como estado vigente. El servidor DEMO
   concedió `TRADING` sólo para la cuenta aprobada (sufijo `5097`), sin órdenes;
   esto no autoriza REAL ni amplía el alcance de la canaria manual.
2. Verificar frescura, continuidad y calentamiento en una sesión de mercado
   abierta; la ventana bounded 60/60 y la consulta fuente `has_more=true` no
   acreditan por sí mismas una serie completa ni frescura live.
3. Verificar límites y condiciones efectivas del bróker para la cuenta/símbolo;
   la lectura administrativa y de catálogo no sustituye la reconciliación de la
   canaria técnica.
4. Satisfacer los gates de mercado y riesgo frescos y ejecutar los dos ciclos
   técnicos dentro de una ventana aprobada. El software está instalado; la
   oportunidad 09:00–09:20 terminó sin lanzamiento y su heartbeat fue eliminado.
   La autorización acotada para `bid == ask` ya existe sólo para esta canaria;
   conserva todos los gates de frescura, continuidad, causalidad, costes,
   margen, ATR, stops, límites y conciliación. La aprobación y el ledger
   impiden extender o repetir un trial reservado; una orden ambigua nunca se
   reintenta. El estado actual y el resultado terminal se conservan en el [estado operativo vigente](docs/operational_status.md#estado-vigente--2026-09-21).
5. Resolver las condiciones contractuales de Pepperstone relevantes para
   México, almacenamiento/redistribución de datos y costes. Una especificación
   de costes no equivale a cargos observados.
6. Sólo después evaluar resistencia, shadow/72 horas y cualquier canary DEMO
   con autorización separada de cuenta, símbolo, volumen, ventana y límites.

La validación conectada no es una autorización general de ejecución; la canaria
tiene su autorización acotada y sus gates propios. Pepperstone no está
validado contractualmente y ninguna simulación/fixture demuestra rentabilidad ni
condiciones comerciales reales.

## Instalación reproducible desde un clone

Requiere Python **3.11 o posterior**. El modo offline del proyecto no tiene
dependencias de ejecución obligatorias; los extras cTrader y dev se instalan en
entornos separados. Los comandos siguientes no abren navegador, no inician
OAuth y no usan cuentas.

```bash
git clone https://github.com/victor982721-lab/MTF.git
cd MTF
python3 -m venv .venv-dev
.venv-dev/bin/python -m pip install --requirement requirements-dev.lock
./install-mtf-lab-wheel.sh --python .venv-dev/bin/python --venv .venv
```

La instalación anterior es la instalación **base**. Si no se desea instalar el
paquete, los lanzadores `./mtf-lab` y `./bin/mtf-lab` ejecutan directamente el
checkout con `python3` (o con el intérprete indicado por `PYTHON`).

### Wheel oficial del proyecto

Para instalar el artefacto PEP 517 fuera del checkout, prepara primero el
entorno de tooling y usa el instalador local. El instalador construye un wheel
con el backend fijado en una copia temporal de las fuentes; no publica nada ni
consulta un índice de paquetes. No modifica `build/` ni la metadata del checkout:

```bash
.venv-dev/bin/python -m pip install --requirement requirements-dev.lock
./install-mtf-lab-wheel.sh --python .venv-dev/bin/python --venv "$HOME/.venvs/mtf-lab"
"$HOME/.venvs/mtf-lab/bin/python" -m mtf_lab --help
```

También puede instalarse un wheel ya construido sin recompilarlo:

```bash
SOURCE_DATE_EPOCH=0 .venv-dev/bin/python -m build --wheel --no-isolation --outdir dist
./install-mtf-lab-wheel.sh --wheel dist/mtf_lab-0.1.0-py3-none-any.whl \
  --venv "$HOME/.venvs/mtf-lab"
```

El script imprime nombre, SHA-256 y `WHEEL_PATH`. Los wheels generados se
conservan en `dist/<sha256>/` (o `--wheel-dir`), sin sobreescribir artefactos
homónimos diferentes. El smoke confirma que importa desde el venv destino,
no desde el checkout. El estado de
ejecución queda fuera de `site-packages`, en `MTF_LAB_STATE_DIR` o en la ruta
XDG del usuario; las credenciales y OAuth no forman parte de esta instalación.

### Extra cTrader reproducible

El codec usa mensajes generados desde los schemas oficiales Spotware release
91, conservados en el paquete. Protobuf es opcional para el núcleo; el SDK
antiguo no es una dependencia ni un fallback. Para una instalación autorizada
en un entorno nuevo, el lock fija el runtime y sus hashes:

```bash
.venv/bin/python -m pip install --require-hashes --requirement requirements-ctrader.lock
.venv/bin/python -m pip check
```

`requirements-ctrader.lock` fija `protobuf==7.36.1`. El
[manifest del codec](manifests/ctrader-protobuf-91.json) conserva schemas,
generador, licencias y hashes. No se descargan schemas en ejecución ni se usa
`--no-deps` para forzar combinaciones incompatibles. La implementación no
actualiza el venv operativo existente. `ctrader doctor` separa presencia de
dependencias, codec utilizable y preparación externa no verificada.

### Herramientas de desarrollo

Mantén Ruff, mypy, Pyright, Coverage.py y el frontend de build en `.venv-dev`,
no en el entorno de runtime:

```bash
python3 -m venv .venv-dev
.venv-dev/bin/python -m pip install --requirement requirements-dev.lock
```

`.venv/` y `.venv-dev/` no se versionan. `requirements-dev.lock` fija Ruff,
mypy, Pyright, Coverage.py y el frontend `build` con sus transitivas para el
tooling; no sustituye el lock cTrader.

### Calidad local

El alcance mantenido de lint y tipos está declarado en el propio checkout:

```bash
.venv-dev/bin/python tools/quality_gate.py \
  --runtime-python .venv/bin/python \
  --dev-python .venv-dev/bin/python \
  --json runtime/verification/quality.json
```

La puerta de calidad descubre todas las fuentes: Ruff/formato sobre
`mtf_lab`, `tests` y `tools`; mypy estricto y Pyright sobre todo `mtf_lab` y
`tools`. No hay una lista de módulos heredados exentos ni un resultado verde
basado en `--exit-zero` o `--follow-imports=skip/silent`.

La suite usa el runner offline con HOME/XDG/TMP/estado aislados y red externa
bloqueada. La cobertura informa líneas y ramas con sus denominadores; un
porcentaje combinado no se presenta como cobertura exclusiva de ramas.
El detalle y los resultados verificables están en
[`docs/refactor_quality.md`](docs/refactor_quality.md).

## Arranque y comandos locales

Todas las salidas de ejecución deben dirigirse a `runtime/` o a otra ruta
ignorada. Para un smoke reproducible desde el checkout:

```bash
mkdir -p runtime/quickstart
./mtf-lab doctor --db runtime/quickstart/doctor.sqlite3
./mtf-lab demo --seed 42 --minutes 720 \
  --db runtime/quickstart/demo.sqlite3 \
  --report runtime/quickstart/demo-report.md
./mtf-lab report --db runtime/quickstart/demo.sqlite3 --latest \
  --output runtime/quickstart/demo-report-latest.md
```

La demo usa reloj virtual y datos sintéticos identificados como tales. No
intercambia silenciosamente Kraken por sintéticos ni ejecuta órdenes.

```text
mtf-lab doctor [--config CONFIG] [--check-network]
mtf-lab demo [--seed N] [--minutes N] [--config CONFIG] [--db PATH]
mtf-lab import DATA.{csv,jsonl} [--db PATH]
mtf-lab replay [--input DATA] [--db PATH] [--checkpoint-every N]
mtf-lab backtest --db PATH [--session ID] [--partition all|exploration|evaluation]
mtf-lab report [--db PATH] [--latest] [--output PATH]
mtf-lab watch [--instrument BTC/USD] [--duration SECONDS] [--max-events N]
mtf-lab ui [--host 127.0.0.1] [--port 8765] [--db PATH] [--duration SECONDS]
mtf-lab ctrader {doctor,query,watch,fixture,...}
mtf-lab cfd-paper [--config CONFIG] [--db PATH] [--chunk-size N]
```

`watch` y `replay` pasan por `RuntimeCoordinator`; conservan buckets,
indicadores, episodios, simulaciones pendientes y checkpoints. La captura
parcial es `PARTIAL`, una reconexión requiere conciliación y una ventana sin
precio admisible queda `INDETERMINATE` en replay completo o `PENDING` durante
observación continua.

## Reproducción de la fase offline

### Suite, auditoría y benchmark

Usa la suite con el intérprete que tenga el SDK cTrader instalado. El runner
crea HOME/XDG/TMP/stores temporales, bloquea red externa y separa
`discovered`, `executed`, `passed`, `failed` y `skipped`:

```bash
mkdir -p runtime/verification
MTF_UI_JS_DEV=1 MTF_NODE_BIN="$(command -v node)" \
  .venv/bin/python tools/offline_tests.py \
  --json runtime/verification/offline_tests.json
.venv-dev/bin/python tools/engineering_audit.py --strict \
  --json runtime/verification/architecture.json
.venv/bin/python -m tools.benchmark_paper --events 7500 \
  > runtime/verification/benchmark.json
```

Para ejecutar el gate completo y regenerar los manifests canónicos desde el
árbol final:

```bash
.venv/bin/python tools/verify_offline.py \
  --runtime-python .venv/bin/python \
  --dev-python .venv-dev/bin/python \
  --node "$(command -v node)" \
  --write-reports \
  --json runtime/verification/final.json
```

El comando no instala dependencias; verifica que ambos entornos y Node ya
existan, bloquea la red externa y escribe `reports/engineering/latest/` sólo
con `--write-reports`.
No confundas el benchmark con rentabilidad o desempeño futuro. Node es una
herramienta de QA de la UI, no una dependencia de runtime; con
`MTF_UI_JS_DEV=1` su ausencia falla explícitamente.

### cTrader fixture y CFD PAPER fixture

Estos comandos son estrictamente locales y no requieren el extra si el fixture
no carga el SDK; con el extra instalado además validan el codec oficial:

```bash
./mtf-lab ctrader doctor --config config/ctrader_query.toml
./mtf-lab ctrader query --fixture
./mtf-lab ctrader fixture --report runtime/verification/ctrader-fixture.json
./mtf-lab cfd-paper \
  --config config/ctrader_pipeline_fixture.toml \
  --db runtime/verification/cfd-paper.sqlite3 \
  --report runtime/verification/cfd-paper.json \
  --chunk-size 128
```

`cfd-paper` genera una captura sintética, la pasa por
`RuntimeCoordinator`/consumidores y persiste el producto CFD. Para reanudar
una sesión, usa el `session_id` del JSON de salida:

```bash
./mtf-lab cfd-paper \
  --config config/ctrader_pipeline_fixture.toml \
  --db runtime/verification/cfd-paper.sqlite3 \
  --session ID
```

`--input` acepta capturas JSON/JSONL locales; `--order market_time_corrected`
es sólo inspección y no puede autorizar un fill observado. Los reportes son
compactos; `--include-payloads` sólo debe usarse cuando se necesite revisar
payloads sintéticos completos.

Después de una consulta cTrader DEMO autorizada, `query --capture` puede
exportar las respuestas históricas como envelopes de captura versionados:

```bash
./mtf-lab ctrader query \
  --config config/ctrader_query.toml \
  --network \
  --capture runtime/ctrader-history.jsonl \
  --report runtime/ctrader-query.json
./mtf-lab cfd-paper \
  --config config/ctrader_pipeline_fixture.toml \
  --input runtime/ctrader-history.jsonl \
  --price-base native \
  --order market_time_corrected \
  --db runtime/ctrader-native-paper.sqlite3 \
  --report runtime/ctrader-native-paper.json
```

La exportación conserva payload, procedencia y barras nativas; no inventa
bid/ask ni fills. `--price-base native` es obligatorio para analizar esa
captura histórica: el resultado puede contener señales MTF, pero no fills
PAPER porque las respuestas históricas no son cotizaciones bid/ask.

### UI fixture

La UI es de sólo lectura y no recalcula reglas financieras. Genera primero la
base CFD y luego sirve el visor local durante un intervalo finito; no abre un
navegador ni contacta servicios externos:

```bash
./mtf-lab ui \
  --db runtime/verification/cfd-paper.sqlite3 \
  --host 127.0.0.1 --port 8765 --duration 1
```

El gate JavaScript reproducible usa el Node local:

```bash
MTF_UI_JS_DEV=1 MTF_NODE_BIN="$(command -v node)" \
  .venv/bin/python -m unittest -q tests.test_ui_javascript
```

## Configuración y datos

Los seis perfiles TOML de `config/` se conservan también como recursos
byte-for-byte en instalaciones wheel. `--config` permite elegir un perfil; si
no se especifica, el cargador busca primero el checkout y después el recurso
empaquetado. Los defaults de un wheel nunca escriben en `site-packages`: usan
`MTF_LAB_STATE_DIR` si existe o el estado XDG del usuario.

El importador local requiere mapeo explícito de columnas y valida timestamps,
OHLC, finitud, orden y duplicados. Los datos de `examples/` son sintéticos y
versionados sólo como fixtures reproducibles; no son cotizaciones reales.

La persistencia usa schema SQLite v4 y migración aditiva desde v3. Los
checkpoints se separan por `analysis_id`; `received_at` y `available_at` no se
confunden. `cfd_trades` permanece separado de `simulations` binarias. Economía
desconocida conserva `None`; `commission_known=false` no significa comisión
cero.

Timestamps con zona horaria se normalizan a UTC y los intervalos son
`[inicio, fin)`. Las velas abiertas sólo se visualizan. No se interpolan ticks
ni precios en huecos; datos atrasados, discontinuos o duplicados bloquean la
evaluación afectada. Kraken REST/WebSocket sigue siendo una ruta opcional y
requiere red explícita; la última vela REST no se trata como cerrada sin la
evidencia correspondiente.

## Integración cTrader / Forex-CFD

La integración conserva una fachada compatible en `mtf_lab.data.ctrader` y
módulos separados de cuentas, configuración, protocolo, codec, transporte,
sesión, mercado y fixtures. `CTraderClientGateway` conecta el único cliente
SDK/lector y no se crea un puente Twisted paralelo. La ruta externa exige
evidencia del cliente autenticado ligada a generación y vigencia; una bandera
`verified=true` suministrada por un caller no autentica el gateway.

El fixture local recorre mensajes Protobuf generados, auth simulada,
correlación, construcción de órdenes, eventos y reconciliación, pero no es una
sesión DEMO observada. La ejecución externa está deshabilitada; REAL/LIVE se
rechaza fail-closed. Los comandos `auth-url`, `callback-listen`,
`token-exchange` y `token-refresh` se conservan para activación futura y no se
usan en el gate offline. Los secretos sólo se reciben mediante referencias o
entradas privadas fuera del repositorio, nunca por argumentos visibles.

Consulta el [estado de integración cTrader](docs/ctrader_integration_status.md)
para límites técnicos y la [matriz de activación](docs/activation_boundaries.md)
para la lista A/B/C. La elegibilidad de Pepperstone, entidad aplicable a
México, permisos, almacenamiento de datos, costes y condiciones contractuales
**no está verificada**.

## Comparación, trazabilidad y límites científicos

`backtest` conserva la referencia `m1_trigger_reference` junto a las señales
MTF, con hash de dataset, configuración, variante, partición y contrato.
Repetir la misma corrida es idempotente; cambiar una dimensión crea otra
identidad. `independent_sample_count` es una agrupación temporal conservadora,
no prueba independencia estadística.

EMA/RSI/ATR y los umbrales iniciales son decisiones de investigación; no se
hace búsqueda masiva de parámetros en este cierre. Los resultados sintéticos,
backtests y PAPER fixtures no prueban rentabilidad, ventaja estadística,
liquidez, spreads, comisiones, ejecución, disponibilidad futura ni
cumplimiento contractual.

## Fuentes públicas de protocolo

- [Trades Spot WebSocket v2 de Kraken](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade), [Candles Spot WebSocket v2](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ohlc) y [OHLC REST](https://docs.kraken.com/api/docs/rest-api/get-ohlc-data).
- [Open API cTrader](https://help.ctrader.com/open-api/), [endpoints](https://help.ctrader.com/open-api/proxies-endpoints/), [conexión](https://help.ctrader.com/open-api/connection/), [autenticación](https://help.ctrader.com/open-api/account-authentication/) y [datos de símbolos](https://help.ctrader.com/open-api/symbol-data/).
- [Presencia de campos Protobuf](https://protobuf.dev/programming-guides/field_presence/) y [contrato `Decimal` de Python](https://docs.python.org/3/library/decimal.html).

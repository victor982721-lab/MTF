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

**La campaña fue reanudada por Víctor el 2026-09-13 mediante una instrucción
humana explícita.** El [handoff de pausa](docs/handoffs/2026-09-13-market-evidence-pause.md)
conserva el punto anterior, los cambios sin commit, los artefactos y la ruta crítica;
no se deben tratar como una release hasta completar sus gates.

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
terminó sin red ni estrategia y conserva receipts JSON/HTML separados. El
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
Los costes y la cobertura aún no permiten una conclusión de ventaja neta;
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
contrato en preflight y cierra antes de leer datos. La última referencia
global reproducible completa, previa a los cambios locales warmup/health y al
refactor de reporting, pasó 990/990 pruebas sin omisiones, con
líneas/ramas 79.871%/62.989%; su receipt está en
`runtime/market-evidence/quality-gate-20260915-ctrader-paper-tick-v3.json`. El gate
histórico bounded previo pasó 964/964 pruebas, con líneas/ramas
79.732%/62.750%; su receipt se conserva en
`runtime/market-evidence/quality-gate-20260915-historical-paper-gate-v3.json`.
La ruta económica
ya conserva costes desconocidos como `UNKNOWN_NOT_ZERO` y sólo acepta una
especificación explícita para calcular neto; aún faltan desarrollo multiaño,
holdout, resistencia y validación DEMO.

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
Su manifiesto está en `state=ACTIVE_RUNTIME`, `promotion_state=ACTIVE` y
`current_pointer` apunta al propio destino; Python 3.12.14, SQLite 3.53.1 y
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

### Validación conectada DEMO de sólo lectura — 2026-09-14

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

#### Diagnóstico de lectura acotado — 2026-09-15

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

#### Captura histórica DEMO bounded V22 — 2026-09-15

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

1. El portal conserva una observación administrativa histórica `Submitted`, no
   `Approved`; no se usa para inferir autorización de trading. La ruta OAuth
   DEMO de sólo lectura ya está verificada en el receipt anterior.
2. La ventana bounded V22 satisface su cobertura acotada, pero la consulta
   fuente conserva `source_has_more=true`. El pipeline/reanudación local ya fue
   verificado sobre el raw existente; no presentar la consulta fuente como serie
   completa ni inferir ventaja.
3. Integrar las referencias privadas de credenciales en el launcher/runtime
   final autorizado, sin copiar secretos, sustituir el runtime operativo ni
   habilitar órdenes.
4. Resolver las condiciones contractuales de Pepperstone relevantes para
   México, almacenamiento/redistribución de datos y costes.
5. Sólo después evaluar resistencia, shadow/72 horas y cualquier canary DEMO
   con autorización separada de cuenta, símbolo, volumen, ventana y límites.

La validación conectada actual no autoriza ejecución. Pepperstone no está
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

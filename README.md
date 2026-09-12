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
- Codec/framing Protobuf oficial instalado, transporte DEMO controlado,
  correlación, heartbeat, backpressure, respuestas de ejecución, reconciliación
  y rechazo fail-closed de REAL/LIVE. Las pruebas usan gateway/transporte
  local o falso; no contactan el bróker.
- Instalación editable y wheel, imports desde cwd ajeno, configuraciones TOML
  empaquetadas, lanzadores, fixtures CFD/cTrader, UI de sólo lectura, suite
  offline, lint/formato/tipos y auditoría arquitectónica.

La evidencia versionada y reproducible está en el [informe de ingeniería](reports/engineering/latest/engineering_consolidation.md),
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

### INTERVENCIÓN DEL USUARIO PENDIENTE

La lista queda limitada a hechos, decisiones y autorizaciones externas:

1. Registrar/aprobar la aplicación cTrader.
2. Disponer fuera del repositorio de `CTRADER_CLIENT_ID` y
   `CTRADER_CLIENT_SECRET`.
3. Completar OAuth y consentir el alcance aplicable.
4. Descubrir cuentas autorizadas y seleccionar explícitamente una cuenta DEMO.
5. Verificar con el servidor DEMO el catálogo, condiciones, permisos, ruta de
   ejecución y reconciliación.
6. Confirmar las condiciones contractuales de Pepperstone relevantes para
   México, almacenamiento/redistribución de datos y costes.

No quedan tareas de programación en esta lista. Pepperstone no está validado
contractualmente y ninguna simulación/fixture demuestra rentabilidad ni
condiciones comerciales reales.

## Instalación reproducible desde un clone

Requiere Python **3.11 o posterior**. El modo offline del proyecto no tiene
dependencias de ejecución obligatorias; los extras cTrader y dev se instalan en
entornos separados. Los comandos siguientes no abren navegador, no inician
OAuth y no usan cuentas.

```bash
git clone https://github.com/victor982721-lab/MTF.git
cd MTF
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools
.venv/bin/python -m pip install --no-build-isolation --no-deps --editable .
```

La instalación anterior es la instalación **base**. Si no se desea instalar el
paquete, los lanzadores `./mtf-lab` y `./bin/mtf-lab` ejecutan directamente el
checkout con `python3` (o con el intérprete indicado por `PYTHON`).

### Wheel oficial del proyecto

Para instalar el artefacto PEP 517 fuera del checkout, prepara primero el
entorno de tooling y usa el instalador local. El instalador construye un wheel
con el backend fijado, no publica nada ni consulta un índice de paquetes:

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

El script imprime el nombre y SHA-256 del wheel instalado. El estado de
ejecución queda fuera de `site-packages`, en `MTF_LAB_STATE_DIR` o en la ruta
XDG del usuario; las credenciales y OAuth no forman parte de esta instalación.

### Extra cTrader reproducible

El SDK oficial es opcional para el núcleo. Para reproducir exactamente el
entorno validado, primero instala el lock completo y después registra el extra
sin resolver versiones nuevas:

```bash
.venv/bin/python -m pip install --requirement requirements-ctrader.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps --editable '.[ctrader]'
.venv/bin/python -m pip check
```

`requirements-ctrader.lock` fija `ctrader-open-api==0.9.2`, Protobuf, Twisted,
TLS y transitivas observadas. Las versiones del entorno vivo y sus hashes de
archivos están en los manifests de ingeniería; no se incluye ningún token o
store.

### Herramientas de desarrollo

Mantén Ruff y mypy en `.venv-dev`, no en el entorno de runtime:

```bash
python3 -m venv .venv-dev
.venv-dev/bin/python -m pip install --upgrade pip setuptools
.venv-dev/bin/python -m pip install --requirement requirements-dev.lock
.venv-dev/bin/python -m pip install --no-build-isolation --no-deps --editable '.[dev]'
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

El gate ejecuta Ruff y formato sobre los módulos refactorizados, mypy estricto
en el contrato tipado, Pyright según `pyrightconfig.json`, la auditoría de
arquitectura y una cobertura de ramas mínima del 60 %. El informe completo de
Ruff sobre el legado se conserva como diagnóstico advisory; no se relajan sus
hallazgos para ocultarlos ni se confunden con la puerta mantenida.

La corrida de esta revisión observó 342 pruebas (3 omitidas), 74 % de cobertura
de ramas y cero fallos en todas esas puertas. El detalle, los límites y la deuda
advisory se conservan en [`docs/refactor_quality.md`](docs/refactor_quality.md).

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
mtf-lab ctrader {doctor,query,fixture,...}
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

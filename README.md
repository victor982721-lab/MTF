# MTF Lab

Primera versión funcional, local y determinista para investigar la relación
entre un mismo instrumento en varias temporalidades. El recorrido completo es:

`fuente -> normalización/calidad -> M1/M5/M15 -> EMA/RSI/ATR -> trend_pullback_v1 -> registro -> simulación virtual -> backtest/reporte -> consulta local`.

El núcleo no llama a un LLM ni a OpenAI. Las señales son hipótesis
experimentales, no recomendaciones ni evidencia de rentabilidad.

## Requisitos comprobados

- Python **3.14.4** en el entorno de desarrollo.
- Sólo biblioteca estándar para el modo offline. `requests`, `websockets` y
  `rich` son opcionales para Kraken/UI enriquecida; si no están disponibles,
  el modo offline continúa funcionando.
- No se requiere bróker, cuenta ni clave.

## Arranque rápido (sin instalar paquetes)

```bash
cd /home/winterboss/MTF
./mtf-lab doctor
./mtf-lab demo --seed 20260909
./mtf-lab report --latest
./mtf-lab ui --port 8765
```

La demostración usa reloj virtual acelerado, crea un dataset sintético
identificado como tal, persiste eventos/velas/decisiones/simulaciones y genera
un informe en `reports/`. Los datos de Kraken nunca sustituyen silenciosamente
al modo sintético.

## Comandos

```text
mtf-lab doctor [--config CONFIG] [--check-network]
mtf-lab demo [--seed N] [--config CONFIG] [--db PATH]
mtf-lab import DATA.{csv,jsonl} [--db PATH]
mtf-lab watch [--instrument BTC/USD] [--duration SECONDS] [--max-events N] [--session ID] [--checkpoint-every N] [--offline-demo]
mtf-lab replay --input DATA [--db PATH] [--checkpoint-every N] [--no-resume]
mtf-lab backtest --db PATH [--session ID] [--config CONFIG] [--partition all|exploration|evaluation] [--boundary UTC] [--report REPORT]
mtf-lab report [--db PATH] [--latest]
mtf-lab ui [--host 127.0.0.1] [--port 8765] [--db PATH]
```

Todos los comandos muestran si el modo es `SINTÉTICO`, `REPLAY` u
`OBSERVACIÓN EN DIRECTO`, la procedencia, la calidad y el estado de
calentamiento. `watch` tiene duración acotada para pruebas; no ejecuta órdenes.

`watch` y `replay` pasan por `RuntimeCoordinator`: el mismo procesador
incremental conserva buckets abiertos, indicadores, episodios y simulaciones
`PENDING`, persiste checkpoints periódicos y reanuda por identidad sin duplicar
la captura. `replay` acepta velas nativas o eventos, deriva sólo temporalidades
compatibles y da precedencia a una vela nativa sobre su OHLC derivada. En un
replay completo, un horizonte sin precio admisible queda `INDETERMINATE`; en
observación continua permanece `PENDING` hasta que expire la tolerancia.

Para reanudar una observación acotada, conserva el `session_id` que devuelve
`watch` y vuelve a ejecutar `mtf-lab watch --session ID --resume` (el valor
predeterminado ya es reanudable), o usa `--no-resume` para iniciar otro estado
incremental. Los checkpoints incluyen el hash de configuración, el cursor,
la identidad del último registro y el estado serializable de agregadores,
indicadores, episodios y liquidaciones pendientes.

## Comparación y trazabilidad

`backtest` genera la referencia `m1_trigger_reference` con el mismo disparador
M1 y la estrategia `trend_pullback_v1` con contexto M15/preparación M5, sobre
la misma captura, base de precio y contrato virtual. La referencia no es un
subconjunto de las señales MTF y se conserva aun cuando MTF emita cero. Cada
corrida identifica `dataset_hash`, `analysis_id`, hash de configuración,
variante, partición cronológica y hash del contrato; cambiar cualquiera de
ellos crea una identidad de análisis separada, mientras que repetir exactamente
la misma corrida es idempotente. `--partition exploration|evaluation` y
`--boundary UTC` excluyen señales cuyo ingreso o liquidación cruza la frontera.

Los informes segmentan por análisis, variante, instrumento, horizonte,
partición y contrato. En `status`, `counts.signals` es el detector MTF
primario para compatibilidad terminal y `counts.signals_total` incluye también
la referencia M1; el informe conserva ambos linajes y no mezcla sus resultados.
Los resultados son simulaciones virtuales y no son
órdenes, rentabilidad demostrada ni recomendación; `independent_sample_count`
es una agrupación temporal conservadora, no una prueba de independencia
estadística.

## Configuración

Los ejemplos están en `config/`. Las claves relevantes (instrumento,
temporalidades, períodos, tolerancias, caducidad, latencia, horizontes y
contrato virtual) son TOML. El cargador rechaza claves desconocidas y
combinaciones incompatibles, en vez de ignorarlas.

## Datos y causalidad

- Timestamps con zona horaria se normalizan a UTC y los intervalos son
  `[inicio, fin)`.
- Las velas cerradas son las únicas que evalúan la estrategia; una vela en
  formación sólo se visualiza.
- El instante de recepción y la disponibilidad para el detector se conservan
  aparte del instante de mercado.
- No se interpolan ticks ni se inventan precios en huecos. Duplicados,
  discontinuidades y datos atrasados bloquean las evaluaciones afectadas.
- El backfill se registra como revisión separada y no cambia lo afirmado en
  directo.

## Importador local

CSV/JSONL requieren mapeo explícito (`timestamp`, `open`, `high`, `low`,
`close`, opcional `volume`, `price_type`, `instrument`, `timeframe`). Se
validan zona horaria, finitud, OHLC, orden y duplicados. `price_type` conserva
`trade`, `bid`, `ask` o `mid`; no se intercambian silenciosamente.

## Kraken público

El adaptador usa el REST `/0/public/OHLC` para arranque/recuperación y el
WebSocket Spot v2 para `trade`/`ohlc`, sin autenticación. Maneja snapshots,
updates, lotes de operaciones, corazones, backoff y errores presentes en el
cuerpo. BTC/USD es el símbolo inicial; se registra el mapeo a nombres internos.
El REST devuelve como máximo 720 entradas y la última está sin comprometer,
por lo que se excluye del conjunto cerrado hasta su cierre. Una reconexión no
se considera recuperación completa sin conciliación explícita.

## UI local de observación

`mtf-lab ui` sirve un visor de sólo lectura en `127.0.0.1` con paginación y
rangos UTC, revisiones, huecos, decisiones, condiciones, descartes y
simulaciones. Incluye sondeo acotado, gráficos SVG de close/EMA/RSI/ATR usando
los valores persistidos y tarjetas de conexión, cobertura, calentamiento,
calidad y resultados pendientes; el frontend no recalcula reglas financieras.

## Extender proveedores

Implemente el contrato `MarketDataProvider` de `mtf_lab.data` y devuelva
`MarketEvent` normalizados (hora de evento, recepción, identidad estable,
precio/base, procedencia y calidad). No coloque reglas ni indicadores en el
adaptador. La agregación, estrategia, simulador y persistencia se reutilizan
sin cambios; el proveedor declara cobertura y revisiones.

## Limitaciones conocidas

- El adaptador Kraken depende de red y de la disponibilidad del endpoint; la
  prueba puede quedar `OMITIDA_BLOQUEO_EXTERNO` y el modo offline sigue siendo
  válido.
- La primera versión no conecta cuentas ni ejecuta órdenes, y no incorpora
  calendario de noticias, spreads o comisiones no configurados.
- La evaluación basada sólo en velas no observa el intraminuto. Resultados
  indeterminados permanecen indeterminados.
- EMA/RSI/ATR y los umbrales iniciales son decisiones de investigación sin
  ventaja demostrada; no se hace búsqueda masiva de parámetros.


### Ejemplo de importación local

```bash
./mtf-lab import examples/data/synthetic_m1.csv \
  --instrument SYNTH/USD --timeframe M1 --price-base close \
  --db runtime/import.sqlite3
./mtf-lab replay --db runtime/import.sqlite3 --session <session_id>
```

Para eventos JSONL use, por ejemplo,
`--mapping timestamp=timestamp,price=price,event_id=event_id` y una base
`trade`; un `bid`/`ask`/`mid` requiere la columna correspondiente. Los
fixtures problemáticos se prueban con el importador de `mtf_lab.data` en modo
estricto o tolerante para conservar sus `issues`.

El informe canónico de la última demostración se genera bajo
`reports/mtf_lab_demo_report.md` cuando se ejecuta el comando de entrega; los
SQLite/logs de operación son derivados y deben elegirse mediante `--db`/`--log`.

## Versiones observadas en la validación

| Componente | Versión | Uso |
|---|---:|---|
| Python | 3.14.4 | runtime comprobado |
| SQLite | 3.46.1 | persistencia |
| websockets | 15.0.1 | WS público Kraken (opcional) |
| requests | 2.32.5 | HTTP opcional; el adaptador usa urllib estándar |
| rich | 13.9.4 | salida opcional |
| numpy | 2.3.5 | disponible, no requerido por el núcleo |

La suite se ejecutó con `python3 -m unittest`; `pytest` no está instalado en
este entorno y no se añadió ninguna dependencia para suplirlo.

`BacktestRunner.run` evalúa cada señal/horizonte de forma independiente. Para
una cartera virtual separada, `BacktestRunner.run_portfolio(...)` usa stake y
saldo fijos, omite explícitamente señales solapadas (`skipped_overlap`) y
calcula caída máxima; no modifica los resultados independientes ni conecta un
motor de ejecución.

La función `mtf_lab.core.assess_freshness` calcula por separado `feed_age_seconds`
y `closed_candle_age_seconds`; sólo marca `STALE`/`LATE` cuando sus tolerancias
explícitas se exceden, de modo que la edad normal de una vela M15 no se confunde
con atraso de recepción.

También está disponible el mismo lanzador en `bin/mtf-lab` para integrarlo en
un `PATH` local, sin instalar paquetes.

## Decisiones pendientes

- Elegir futuro bróker y revisar su fuente de precios, sesiones, contratos y
  restricciones antes de implementar un adaptador adicional.
- Definir una partición fuera de muestra y un protocolo de investigación antes
  de interpretar resultados; esta versión no optimiza parámetros.
- Decidir si una futura cartera permitirá posiciones solapadas y qué fricciones
  explícitas (spread/comisión) aplican al producto elegido.

Durante el arranque Kraken se guardan por separado las velas nativas REST y los
trades/updates WS, con `revision` y procedencia; una reconexión sólo marca
`needs_reconciliation`. La v1 no reemplaza retroactivamente una observación ni
afirma conciliación automática, y permite revisar esa diferencia en una sesión
posterior.

## Indicadores usados

`mtf_lab.core.indicators` usa EMA con semilla SMA de `n` cierres y
`alpha=2/(n+1)`. RSI y ATR usan Wilder: media inicial simple de `n` muestras,
después `(prev*(n-1)+actual)/n`; RSI aparece tras `n+1` cierres y ATR tras `n`
true ranges (el primero usa `high-low`). Una pérdida media cero da RSI 100
(si la ganancia es positiva) o 50 (serie constante), ganancia media cero da 0,
y ATR cero permanece cero. Valores ausentes, velas abiertas y huecos reinician
calentamiento y no alimentan señales.

## Fuentes públicas consultadas

El contrato de integración Kraken se contrastó con la documentación vigente:
[Trades Spot WebSocket v2](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade), [Candles (OHLC) Spot WebSocket v2](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ohlc) y [Get OHLC Data Spot REST](https://docs.kraken.com/api/docs/rest-api/get-ohlc-data). La documentación confirma que `trade` puede agrupar varias operaciones, que el snapshot refleja las últimas 50, que `ohlc` se actualiza con operaciones y que REST devuelve hasta 720 entradas dejando la última como intervalo no comprometido; el adaptador conserva esas limitaciones en procedencia y no afirma recuperación completa sólo por reconectar.

# MTF Lab: proveedores e ingesta

El núcleo consume dos registros neutros:

- `Event`: evento de mercado (`event_time`, `received_at` y `available_at`
  separados), identidad estable por proveedor/event id y `price_basis` explícito
  (`traded`, `bid`, `ask` o `mid`).
- `Bar`: vela OHLC sobre intervalo `[interval_start, interval_end)`, con
  resolución, cierre y revisión explícitos.

## Generador offline

```python
from mtf_lab.data import SyntheticGenerator

fixture = SyntheticGenerator(seed=7).generate(
    periods=240,
    scenario="pullback",
    anomalies=("gap", "duplicate", "out_of_order"),
)
# fixture.bars / fixture.events, fixture.provenance.synthetic is True
```

Escenarios: `trend`, `pullback`, `sideways` y `volatility_change`. El seed se
registra en la procedencia y no se usa la RNG global. Las anomalías no se
corrigen ni rellenan: aparecen como `ValidationIssue`/calidad.

## Importación local

`import_csv(path, mapping, config=...)` e `import_jsonl(...)` requieren OHLC
para una vela y una columna de precio correspondiente a la base elegida para
un evento. Timestamp numérico requiere una unidad explícita (`s`, `ms`, `us` o
`ns`); timestamp ISO sin zona requiere `assume_timezone` explícito. En modo
estricto cualquier error de fila produce `DataValidationError`. En modo
leniente (`ImportConfig(strict=False)`) se devuelven registros válidos junto
con `DataSet.issues`; `reorder` y `drop_duplicates` son opt-ins explícitos.

No se derivan eventos intrabar desde velas, ni se elige bid/ask/mid por
proximidad o conveniencia. La procedencia incluye SHA-256, instrumento, base y
cobertura UTC.

## Kraken público

```python
from mtf_lab.data import KrakenPublicAdapter

adapter = KrakenPublicAdapter("BTC/USD")
result = adapter.fetch_ohlc(interval=1, include_open=False)
for closed_bar in result: ...
open_bar = result.open_bar       # siempre separado
for event in adapter.iter_trades(duration_seconds=30, max_events=10): ...
```

REST es `https://api.kraken.com/0/public/OHLC` con `assetVersion=1`; WebSocket
es `wss://ws.kraken.com/v2`, canal público `trade`/`ohlc`. Se procesan arrays de
trades, snapshots y actualizaciones, errores del cuerpo (`error`) y códigos de
respuesta, heartbeat, timeout, reconexión con backoff y estado observable.
Una reconexión marca `adapter.status.needs_reconciliation=True` y
`discontinuity=True`; nunca se afirma que se recuperaron eventos perdidos.
El adaptador no tiene autenticación, órdenes ni lógica de ejecución.

La respuesta REST se limita a 720 filas y su última fila es la vela actual no
comprometida, por eso se expone como `open_bar`. El MTF core debe calentar cada
temporalidad con datos suficientes y conciliar las velas nativas de Kraken con
las construidas de eventos, sin mezclar la vela abierta con las cerradas.

Fuentes consultadas el 2026-09-09:

- https://docs.kraken.com/exchange/guides/websockets/introduction
- https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade
- https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ohlc
- https://docs.kraken.com/api-reference/market-data/get-ohlc-data

No se publican datasets descargados de Kraken por defecto. El fixture de
`examples/data` es sintético y offline.

## Contrato de proveedor

`MarketDataProvider` (en `provider.py`) es un `Protocol` pequeño con
`fetch(...)` y `stream(...)`; `KrakenPublicAdapter` expone además estos alias.
Un proveedor futuro debe devolver `Event`/`Bar` normalizados y declarar
procedencia/calidad, sin copiar reglas de `mtf_lab.core`.

Durante el arranque a mitad de intervalo se conservan dos evidencias: la vela
nativa REST (incluida su marca `closed`/`open`) y los eventos WS posteriores.
La v1 no sobreescribe una observación con la otra: las revisiones llevan
`revision`/`source_record_id` y una discontinuidad de stream queda marcada como
`needs_reconciliation`; la conciliación automática de cada OHLC nativa contra
la vela agregada queda pendiente y debe ejecutarse en una sesión de revisión.

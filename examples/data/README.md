# Fixtures de datos (solo offline)

Estos archivos fueron generados por `SyntheticGenerator(seed=20260909)` y
están marcados como **SINTÉTICOS**. No son cotizaciones reales de BTC/USD,
EUR/USD ni de otro instrumento, y no constituyen evidencia de rentabilidad.

- `synthetic_m1.csv`: 180 velas M1 del escenario `pullback`, aptas para una
  importación estricta con el mapeo convencional de `ColumnMapping`.
- `synthetic_events.jsonl`: 48 eventos de precio `traded`, con `event_id`
  explícito. No se fabricaron ticks intrabar para representar un histórico
  real; son únicamente una fixture sintética de la ruta de eventos.
- `synthetic_anomalies.jsonl`: fixture de validación que contiene hueco,
  duplicado y desorden; debe producir advertencias en modo tolerante o ser
  rechazada en modo estricto.

Ejemplo:

```python
from mtf_lab.data import ColumnMapping, ImportConfig, import_csv, import_jsonl
bars = import_csv("examples/data/synthetic_m1.csv")
events = import_jsonl(
    "examples/data/synthetic_events.jsonl",
    ColumnMapping.from_dict({
        "record_kind": "event", "timestamp": "timestamp",
        "instrument": "instrument", "price": "price", "quantity": "quantity",
        "event_id": "event_id", "side": "side",
    }),
    config=ImportConfig(price_basis="traded"),
)
```

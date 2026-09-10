# MTF Lab — informe de ejecución

- Generado: `2026-09-10T20:06:00Z`
- Modo: **SYNTHETIC** (`SINTETICO`)
- Proveedor: `synthetic`
- Instrumento de sesión: `SYNTH/USD`
- Estado de sesión: `COMPLETED`

## Conteos persistidos

| Señales | Descartes | Simulaciones | Velas |
|---:|---:|---:|---:|
| 96 | 5548 | 288 | 3040 |

## Agregado de simulaciones

- Resultado neto virtual: `-8`
- Resueltos: `287`; indeterminados/pending: `1` (PENDING: `0`)
- Caída máxima de la secuencia registrada: `22.799999999999994`

## Segmentos controlados

| Análisis | Variante | Instrumento | Horizonte s | Partición | Contrato | N | W/L/T/I/P | Neto | DD |
|---|---|---|---:|---|---|---:|---|---:|---:|
| `m1_trigger_reference` | `m1_trigger_reference` | `SYNTH/USD` | 180 | `all` | `VIRTUAL_CONTRACT` | 89 | 46/43/0/0/0 | -6.2 | 10.2 |
| `m1_trigger_reference` | `m1_trigger_reference` | `SYNTH/USD` | 300 | `all` | `VIRTUAL_CONTRACT` | 89 | 46/42/0/1/0 | -5.2 | 8 |
| `m1_trigger_reference` | `m1_trigger_reference` | `SYNTH/USD` | 60 | `all` | `VIRTUAL_CONTRACT` | 89 | 51/38/0/0/0 | 2.8 | 11 |
| `trend_pullback_v1` | `trend_pullback_v1` | `SYNTH/USD` | 180 | `all` | `VIRTUAL_CONTRACT` | 7 | 4/3/0/0/0 | 0.2 | 1 |
| `trend_pullback_v1` | `trend_pullback_v1` | `SYNTH/USD` | 300 | `all` | `VIRTUAL_CONTRACT` | 7 | 4/3/0/0/0 | 0.2 | 2 |
| `trend_pullback_v1` | `trend_pullback_v1` | `SYNTH/USD` | 60 | `all` | `VIRTUAL_CONTRACT` | 7 | 4/3/0/0/0 | 0.2 | 2.2 |

## Motivos de descarte

- `context_indicator_not_ready`: 921
- `context_invalidated`: 182
- `context_neutral`: 54
- `direction_not_known`: 372
- `directional_pullback_failed`: 535
- `distance_exceeded`: 552
- `no_directional_cross`: 446
- `preparation_lookback_insufficient`: 632
- `preparation_not_registered`: 899
- `rsi_not_ready`: 372
- `rsi_threshold_failed`: 317
- `signal_duplicate`: 28
- `trigger_previous_bar_unavailable`: 238

## Calidad y límites

- Calidad: `{"synthetic": 3040}`
- Resoluciones: `{"M1": 2400, "M15": 160, "M5": 480}`
- Los resultados son simulaciones virtuales y no constituyen órdenes ni recomendación.
- La tasa de acierto no es una probabilidad de éxito; señales cercanas pueden depender entre sí.
- La muestra bruta y las particiones no eliminan dependencia entre señales cercanas.
- Las velas no permiten inferir movimientos intrabar que no estén observados.
- La ausencia de una fuente de noticias o de un bróker no se interpreta como comprobación.
- Una simulación INDETERMINATE o PENDING no se cuenta como ganancia, pérdida ni operación resuelta.

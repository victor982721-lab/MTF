# MTF Lab — informe de ejecución

- Generado: `2026-09-10T04:29:56Z`
- Modo: **SYNTHETIC** (`SINTETICO`)
- Proveedor: `synthetic`
- Instrumento: `SYNTH/USD`
- Estado de sesión: `COMPLETED`

## Conteos persistidos

| Señales | Descartes | Simulaciones | Velas |
|---:|---:|---:|---:|
| 14 | 2837 | 321 | 3040 |

## Resultados por horizonte

| Horizonte (s) | Resultados | Neto virtual |
|---:|---|---:|
| 180.0 | `{"LOSS": 63, "WIN": 44}` | -27.8 |
| 300.0 | `{"LOSS": 61, "WIN": 46}` | -24.2 |
| 60.0 | `{"LOSS": 64, "WIN": 43}` | -29.6 |

## Variantes controladas

| Variante | Resultados | Neto virtual | Hash de configuración |
|---|---|---:|---|
| `m1_trigger_reference` | `{"LOSS": 163, "WIN": 116}` | -70.2 | `1a9728a0821117e49f67004867b86a0beb26e4eb8c4ee98ca19adb0e90a0234c` |
| `trend_pullback_v1` | `{"LOSS": 25, "WIN": 17}` | -11.4 | `cacc8626bbefbf0ca224de069c682f812819968a7db6283515b106a9422a382c` |

## Motivos de descarte

- `context_indicator_not_ready`: 936
- `context_invalidated`: 270
- `context_neutral`: 54
- `context_not_available`: 16
- `directional_pullback_failed`: 147
- `distance_exceeded`: 92
- `no_directional_cross`: 135
- `preparation_expired`: 335
- `preparation_not_registered`: 726
- `rsi_threshold_failed`: 1
- `signal_duplicate`: 125

## Calidad y límites

- Calidad: `{"synthetic": 3040}`
- Resoluciones: `{"M1": 2400, "M15": 160, "M5": 480}`
- Los resultados son simulaciones virtuales y no constituyen órdenes ni recomendación.
- La tasa de acierto no es una probabilidad de éxito; señales cercanas pueden depender entre sí.
- Las velas no permiten inferir movimientos intrabar que no estén observados.
- La ausencia de una fuente de noticias o de un bróker no se interpreta como comprobación.

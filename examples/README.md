# Fixtures offline

- `synthetic_m1_seed42.jsonl`: 60 velas M1 reproducibles (`seed=42`, escenario `pullback`). Cada registro está etiquetado `synthetic=true`; no representa un mercado real.
- `synthetic_problematic_seed42.jsonl`: incluye hueco, duplicado y fuera de orden para probar validación; no se corrige ni se usa para afirmar continuidad.

El recorrido completo usa `./mtf-lab demo`, que genera 4 escenarios y calienta M1/M5/M15 sin internet.

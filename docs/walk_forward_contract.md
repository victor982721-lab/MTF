# Contrato de walk-forward 2020–2023

**Estado:** contrato, guards y fixture causal `WARMUP_ONLY → WF` con resume
implementados/validados offline; no habilita adquisición, ejecución ni acceso
al holdout hasta completar los gates de datos.

## Separación de fuentes

Los manifiestos actuales permanecen inmutables:

- `DatasetManifest`: desarrollo HistData 2016–2019.
- `DukascopyManifest`: sólo el piloto autorizado del 7–14 de marzo de 2016.

El walk-forward usa un tipo independiente (`WalkForwardManifest`) y no
re-etiqueta ninguno de los anteriores. Un manifiesto HistData WF requiere las
48 particiones mensuales `202001`…`202312`, con raw, miembro, SHA-256, tamaño,
URI/términos y cobertura verificables. Una fuente Dukascopy WF requeriría otro
contrato explícito de formato, periodo, URI y términos; no está autorizada hoy.

## Entrada compuesta

El sidecar `mtf-lab.walk-forward-input.v1` debe referenciar por hash:

- manifiesto de desarrollo 2016–2019;
- manifiesto WF 2020–2023;
- protocolo, riesgo, calendario, costes, código y runtime;
- exclusión de holdout `[2024-01-01, 2026-01-01)` (`CLOSED`).

Las ventanas son exactas y half-open:

```text
WF_2020 [2020-01-01, 2021-01-01)
WF_2021 [2021-01-01, 2022-01-01)
WF_2022 [2022-01-01, 2023-01-01)
WF_2023 [2023-01-01, 2024-01-01)
```

El stream debe consumir primero el desarrollo como `WARMUP_ONLY`: sólo el
estado causal de indicadores puede cruzar la frontera; estrategia, riesgo,
señales y trades no. Después evalúa la ventana WF y rechaza cualquier evento
`>= 2024-01-01`. El checkpoint debe enlazar ambos manifiestos, el contrato
compuesto, la ventana, cursor, offsets, runtime/código/protocolo y revisión del
registry; un checkpoint de desarrollo no se puede reanudar con otro contrato.

## Guards, registry y receipts

El guard implementado rechaza roles mezclados, meses fuera de 2020–2023, gaps,
colisiones de ruta/hash/miembro, tamaños o hashes incorrectos, orden alterado,
clamping y cualquier dato 2024+. El holdout no puede abrirse para un
`WalkForwardManifest`.

Registrar antes de leer cada combinación candidato × ventana × escenario
(`6 × 4 × 3 = 72` intentos), conservando fallos, insuficientes y reintentos.
Cada intento incluye `stage=walk-forward`, `window_id`, hashes de ambos
manifiestos y del contrato, `holdout=CLOSED`,
`selection_performed=false`, `promotion_performed=false` y
`trading_enabled=false`.

Separar receipts de validación de datos, contrato, corrida por ventana e
informe agregado. Todos deben indicar `auto_promote=false`,
`network_performed=false` y `data_acquisition=false`. Costes desconocidos
siguen siendo `UNKNOWN_NOT_ZERO` y no permiten una conclusión favorable.

## Preflight del runner

El runner histórico expone `bind_walk_forward_contract` y los argumentos
`--walk-forward-manifest`, `--walk-forward-input` y `--window` como una unión
de contratos de sólo lectura. El preflight carga los sidecars, enlaza
`WalkForwardStreamGuard` y comprueba la ventana exacta sin escanear raws ni
crear registry/output. `stage=walk-forward` termina entonces con
`WALK_FORWARD_CLOSED`: el consumidor productivo causal `WARMUP_ONLY → WF`
todavía no está integrado; la fixture offline de regresión sí está validada.
Esta barrera es deliberada y conserva cerrado el holdout; no es una ejecución WF
ni una autorización de adquisición.

## Gate de habilitación

No iniciar WF hasta que exista, en este orden:

1. receipt terminal íntegro del desarrollo 2016 activo;
2. desarrollo 2016–2019 completo y sus datos validados;
3. guard agregado de 40 GiB y reserva mínima de 20%;
4. manifiesto WF independiente, validado y sin 2024+;
5. fixture offline warmup→WF con resume byte-equivalente (validada; sin datos
   reales);
6. registry completo y runtime/código/protocolo congelados.

La implementación offline está en `mtf_lab/data/walk_forward.py` y
`mtf_lab/data/walk_forward_fixture.py`, el preflight en
`tools/run_historical_campaign.py` y las regresiones en
`tests/test_walk_forward_contract.py`,
`tests/test_historical_campaign_walk_forward_contract.py` y
`tests/test_walk_forward_warmup_resume.py`; la última referencia del gate global
anterior a los bytes actuales, `quality-gate-20260915-ctrader-paper-tick-v3.json`,
ejecutó 990/990 pruebas. El receipt
`runtime/market-evidence/wf-warmup-resume-fixture-20260915.json` cubre las
cuatro ventanas y cortes de reanudación, con salidas byte-equivalentes,
holdout `CLOSED` y sin red/adquisición.
Al 2026-09-15 no existen raws 2017–2019 ni datos WF, por lo que no existe
corrida WF habilitada: el runner sólo puede enlazar el contrato y cerrar antes
de leer datos. El modelo vigente sigue rechazando `DatasetPartition(202001)`
como holdout; esta implementación conserva esa barrera.

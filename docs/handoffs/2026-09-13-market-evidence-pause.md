# Handoff — campaña histórica MTF pausada por Víctor

## 1. Orden vigente y punto de parada

**PAUSA EXPRESA DEL USUARIO, 2026-09-13 13:11:26 America/Mexico_City
(19:11:26 UTC). No reanudar por reloj, heartbeat, goal activo, mensaje antiguo
de un agente o continuación técnica. Se requiere una nueva indicación humana.**

Víctor pidió detener el trabajo, conservarlo reanudable y cerrar en un punto
seguro sin perder información. Se interrumpieron los tres frentes activos
(`quant_audit`, `strategy_impl`, `research_methods`); los otros cuatro ya
habían terminado. No se encargaron nuevos trabajos tras la orden. Después de
la parada sólo se inspeccionó estado, preservaron bytes y redactó este handoff.
No se iniciaron nuevas pruebas, descargas, benchmarks o correcciones de código.

Las cuatro corridas reales de la raíz ya habían terminado: semana y mes
descriptivos, más los prefijos de backtest de 2,048 y 8,192 ticks. No quedó una
corrida real que cancelar. La inspección de procesos no encontró workers propios
de pruebas/backtest/descarga; se conservaron los procesos de infraestructura CUA
de la aplicación, que no son workers de esta investigación. No se creó una
automatización para esta campaña; el inventario local por ID exacto de esta
tarea no encontró una asociada. No se intervinieron tareas ajenas.

El objetivo de ventaja neta **no está cumplido**. La API de goals disponible
no ofrece pausa/cancelación; no se falseó `complete` ni `blocked` para simularla.
La parada humana de esta sección prevalece sobre cualquier estado técnico del
goal. El compromiso `MTF-VAL-001` queda en `ESPERA_VICTOR`, por pausa solicitada,
no por un fallo de permisos. `MTF-DEM-001` conserva su observación histórica
externa; no se reabrió el portal.

## 2. Fuentes canónicas y conservación

- Proyecto: `/home/winterboss/MTF`.
- Rama local: `main`; HEAD/base publicada previa:
  `bea3303a6e497d7a70cfd607e73037f4182e211e`.
- **Los cambios de esta campaña permanecen sin commit. El árbol está sucio
  deliberadamente; no es una release validada.** No se hizo stash, reset,
  checkout, limpieza, rebase, merge, push ni instalación operativa al pausar.
- Plan aprobado: [market_evidence_plan.md](../market_evidence_plan.md).
- Evidencia y medidas: [market_pilot_status.md](../market_pilot_status.md).
- Costes/calendario: [research_cost_provenance.md](../research_cost_provenance.md).
- Acceso/tiempos: [historical_time_validation.md](../historical_time_validation.md).
- Registro global: `/home/winterboss/.local/state/mtf-lab/research/global-trials.jsonl`.
- Datos originales: `/home/winterboss/.local/share/mtf-lab/market-data`.
- Handoff/backup privado de la pausa:
  `/home/winterboss/.local/state/mtf-lab/handoffs/20260913T191126Z/`.
  `SNAPSHOT.json` identifica bytes, hashes y archivos; `worktree-files.tar.gz`
  conserva cambios rastreados y archivos nuevos, y `tracked.patch` conserva
  el diff contra HEAD. `external-evidence.json` apunta a los artefactos
  canónicos, sin duplicar el raw ni el runtime completo.

El checkout sigue siendo la fuente de trabajo. El backup es un checkpoint de
recuperación, **no algo que deba extraerse encima del árbol vivo**. Al retomar,
comparar primero hashes y cambios posteriores; preservar cualquier trabajo
ajeno y resolver diferencias. Nunca usar `git reset --hard`, `git clean` ni una
restauración ciega como paso de arranque.

## 3. Decisiones que no deben reabrirse por conveniencia

EUR/USD, local/gratuito, DEMO solamente. Sin compras/cloud/REAL/GitHub Actions,
sin nuevas cuentas/OAuth/scopes/órdenes por inferencia y sin modificar energía,
red, privacidad o trabajos ajenos. Datos fuera de Git: máximo inicial 40 GiB y
reserva de disco mínima 20%; no borrar originales para ganar espacio.

Seis candidatos canónicos, no búsqueda ilimitada:

| ID | Temporalidades | Permanencia |
|---|---|---|
| `tp_fast_v1` | M15/M5/M1 | intradía |
| `tp_intraday_slow_v1` | H4/H1/M15 | intradía |
| `tp_multiday_v1` | D1/H4/H1 | multidía |
| `dc_m5_v1` | Donchian 20 barras previas M5 | intradía |
| `dc_m15_v1` | Donchian 20 barras previas M15 | intradía |
| `dc_h1_v1` | Donchian 20 barras previas H1 | multidía |

EMA 20/50, RSI 14, ATR 14 y umbrales existentes. Mid para indicadores; Bid/Ask
para ejecución. Baseline y horizontes legacy 60/180/300 s son controles, no una
cartera sumable. Riesgo previsto máximo 0.25% equity por operación, paradas por
pérdida diaria 1% y DD 5%; no garantizan pérdida máxima ante gaps/fallos.
SL 1.5 ATR, TP 3 ATR, intradía cinco barras y precorte 30 min; multidía 72 h y
precierre semanal/festivo prolongado 60 min. Una posición y una intención.

Periodos: 2015 calentamiento; 2016–2019 desarrollo; WF 2020, 2021, 2022, 2023;
2024–2025 reserva cerrada. La cuenta USD 10,000 es **virtual**, no de Víctor.
Modelos base 5 s/0.1 pip por fill, adverso 10 s/0.2 pip y spread/costes 1.5×,
extremo 2× diagnóstico. Los costes desconocidos no son cero ni permiten una
conclusión favorable.

Criterios posteriores: media neta estrés >=0.05 R, LCB unilateral >0 en base
y adverso, DD <=5%, quitar las cinco mejores no vuelve negativo el neto;
120 episodios holdout, 100 sesiones, 20 bloques completos de dos semanas,
20 episodios por WF anual. Bootstrap 100,000, semilla 20260913, bloques de dos
semanas con sensibilidad de una/cuatro y α=0.05/12. No seleccionar ni ajustar
con holdout. Las 72 horas, shadow y 30/100/200 sesiones futuras son tiempo real,
no fixtures. Faltan todos los gates externos y la decisión humana final.

## 4. Datos y resultados reales conservados

### HistData

Manifiesto `/home/winterboss/.local/share/mtf-lab/market-data/manifests/histdata-eurusd-201603.json`.
Raw `raw/HISTDATA_COM_ASCII_EURUSD_T_201603.zip` relativo a market-data,
10,160,571 bytes, SHA-256:
`8357ef823ac27d9c53da9acff6f79b6e8fc058cea9fdf8f32be08bf06dbe3e1b`.
Se conservan 1,979,243 ticks; cobertura 2016-03-01 05:00:00.400 a
2016-04-01 04:59:59.277 UTC. Semana 7–14 de marzo: 499,804 ticks.
Formato/orden/hash se validaron sin incidencias; esto no acredita cobertura
completa ni liquidez del bróker. EST fijo UTC−5 no se retoca por DST.

El mes muestra cierres/aperturas semanales alrededor de 22 UTC incluso después
del DST estadounidense. El domingo 13 abre 22:00:34.657 UTC frente a 21:00 UTC
del modelo aproximado NY 17:00. No imputar esa hora, no desplazar timestamps y
no convertir días con quotes en sesiones completas. H4/D1 requieren más
calentamiento/continuidad. El calendario numérico FX es modelado, no de cuenta.

### Descripción de semana/mes

Raíz de resultados: `/home/winterboss/.local/state/mtf-lab/research/market-structure/`.

| Carpeta | Ticks seleccionados | Pared / CPU | RSS |
|---|---:|---:|---:|
| `201603-week-a39c3705472b` | 499,804 | 173.881 / 173.297 s | 187,445,248 B |
| `201603-month-e4d9bb3d95d4` | 1,979,243 | 362.122 / 361.648 s | 187,965,440 B |

Ambas lecturas decodificaron el **mes completo** antes de filtrar. La relación
de pared no prueba aceleración ni capacidad anual. Fueron descripciones, no
backtests. Spread medio por tick aproximadamente 0.3683/0.3678 pip; 8/54 gaps
>90 s, incluidos cierres de mercado.

Informes actuales, provenientes de los originales y sin recursos remotos:

- Semana: `201603-week-a39c3705472b/report-v4/report.html` y `report.json`.
  HTML SHA `e29de517b47f68607c269f519fbe32053fb678583a60a3b31e250a1db1bcfc4b`;
  JSON SHA `2779a906621d012a5cf1d026e65e87d2d3ae32f457e53f6f82dfdb2edc492f0a`.
- Mes: `201603-month-e4d9bb3d95d4/report-v2/report.html` y `report.json`.
  HTML SHA `05c5ed6f6db1f5c6f55bb37e9fb469657b3fff3785cf49e07d5da33b71b86eb8`;
  JSON SHA `3b1d6e2702b3867c723a4d43709df474b31a56a7547e697209bb8d2c8e0131bf`.

Cuatro SVG, n por TF, truncamiento de gaps explícito, calidad
`SOURCE_VALIDATED_COVERAGE_LIMITS`, costes/fills desconocidos y cautelas visibles.
La URI pública de procedencia se conserva como dato inerte; autocontenido no
significa eliminar las fuentes. Las variantes anteriores permanecen.
El último cambio restauró esa URI: auditoría programática y pruebas focales
pasaron, pero no se hizo nueva revisión visual integral de esos bytes finales
por la raíz antes de la pausa. La versión previa tenía QA Chrome aislado.

**Limitación preservada:** las primeras corridas descriptivas conservaron
hashes, pero eliminaron la copia temporal completa del código. Son evidencia
intermedia, no replay íntegro desde una release retenida. El helper se corrigió
para futuros runs, que todavía no se repitieron.

### Prefijos reales del backtest, baseline de rendimiento

Raíz: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/`.
Fuente común **retenida**:
`201603-baseline-2048-6e0bca15b050/source/`, identidad
`561f61c8263a9f0ec92dded23fa0d9830e13bbfddfa6bed517c0f62b0bbc8f4c`.
La carpeta de la fuente conserva un fallo de preparación JSON del helper,
anterior a leer mercado. No confundirlo con los runs exitosos siguientes.

| Carpeta | Seleccionados / decodificados | Pared / CPU | RSS | Checkpoint |
|---|---:|---:|---:|---:|
| `201603-baseline-2048-e49ae3895855` | 2,048 / 378,623 | 22.630 / 22.507 s | 194,236,416 B | 34,026,934 B |
| `201603-baseline-8192-fd978d2fc173` | 8,192 / 384,767 | 51.302 / 51.040 s | 336,494,592 B | 58,481,547 B |

Ambos seleccionan desde 2016-03-07 00:00:00.490 UTC; terminan 00:47:42.460 y
03:40:27.180 UTC. Hashes de secuencia fuente:
`412424ab094f5b32fe371a8dee5a425e7b0fe200de94a0cc537a64bf61d21b70` y
`1265c9fefda69cbf3d618ea15718c57400507c75054d793b8264a2f18af1b068`.
Seis intentos preregistrados por corrida, sin selección; cero trades en estos
prefijos. Equity/funnel: 62/10 y 298/58 filas. Guardas sin red/escrituras fuera
del estado aislado y raw/manifiesto/copia estables. **No son prueba de economía
con operaciones ni de capacidad anual.**

La baseline retiene listas de ticks en barras y replica indicadores por
candidato. Su campo `ticks_retained_in_memory=0` es incorrecto y no se usa como
evidencia. Se preserva el resultado original en vez de reescribirlo.

### Dukascopy

**BLOCKED_ACCESS_TERMS, cero ticks/cero raw. No hacer nuevas solicitudes.**
El widget público indicó `https://jetta.test.dukascopy.com/v1`; metadata EUR/USD
terminó en timeout. Es un hecho técnico distinto del gate documental posterior:
los [términos oficiales](https://www.dukascopy.com/swiss/english/legal-pages/terms-of-use/)
exigen consentimiento escrito para adquisición automatizada y restringen la
construcción de bases de datos. Una exportación manual no se presume suficiente
para resolver todo el uso. No usar BI5, otro endpoint, cuenta, proxy o navegador
como evasión. No se necesita este acceso para terminar la integración local.

Receipts en `market-data/dukascopy/receipts/dukascopy-eurusd-20160307-20160314.json`
y `dukascopy-eurusd-20160307-20160314-attempt-1.json`, preservados. La API/CLI
exige `provider_authorization_ref` además de autorización del usuario; una
referencia local no acredita por sí sola el consentimiento del proveedor.

## 5. Estado de cada frente al detenerse

Los nombres siguientes son subagentes internos de la misma tarea, no tareas
del usuario. Principal solicitado: Astra/MAX; hijos Luna/MAX. No cambiar modelos
silenciosamente. La verificación previa de runtime de la principal fue Astra/MAX;
hubo discrepancias históricas de ajustes/global config Luna, de origen no
resuelto. La consulta de Víctor sólo pidió confirmar, no autorizó reconfigurar.

| Frente | Estado real guardado | Archivos / detalle de reanudación |
|---|---|---|
| `dependency_path` | entregado, focal; acceso externo detenido | `mtf_lab/data/historical.py`, `histdata_acquisition.py`, `dukascopy.py`, `dukascopy_acquisition.py`, tools y tests. Dispatch lazy tipado `HistoricalManifest`/`HistoricalValidation` unions; parser HistData y originales preservados. Revisar consumidores que aún esperan sólo `DatasetManifest`. |
| `quant_audit` | **interrumpido a mitad del acumulador** | `mtf_lab/core/aggregation.py`: acumulador online sólo `continuous_quotes`, legacy `strict` conservado. Nuevos `export_bucket_state()`/`restore_bucket_state()`; aún falta terminar la regresión específica y medir equivalencia/memoria. El archivo `tests/test_streaming_quote_aggregation.py` NO existía al pausar. El riesgo/CFD anterior de este agente sí fue entregado focalmente. |
| `strategy_impl` | **interrumpido a mitad del data plane compartido** | `mtf_lab/runtime/processor.py` contiene `DataPlaneResult`, `SharedDataPlane`, parámetro `data_plane` y nuevas rutas/checkpoints. NO hay aceptación. `tests/test_historical_data_plane.py` NO existía. El backtest todavía requiere integrar el plane, guard y checkpoints coherentes. |
| `research_methods` | protocolo/evidencia/binding entregados; nuevo frente **apenas inspeccionado** | `market_protocol.py`, `global_trial_registry.py`, `market_research.py`, `market_evidence.py` y pruebas. Se acababa de asignar preregistro de `market-data describe` en `market_data_service.py`; no se implementó todavía y `tests/test_market_data_registry.py` NO existía. La raíz tampoco añadió el flag `--registry` a `describe`. |
| `safety_audit` | entregado focalmente | riesgo/equity/ejecutor/supervisión y configuración DC de una TF. `StrategyConfig` sigue siendo envolvente legacy; la TF operativa verdadera está en `market_candidate_id` y `config.timeframes`. V2/Pyright verificados, ver sección siguiente. |
| `market_comparators` | entregado focalmente | reportes HTML/JSON y UI snapshot; versiones vigentes arriba. Falta revisión final global/visual integrada. |
| `supervisor_impl` | entregado focalmente, **guard aún no integrado** | `mtf_lab/data/historical_stream_guard.py` + `tests/test_historical_stream_guard.py`; helper durable de descripción y runtime V2. 9 tests del guard y 34 históricos combinados reportados; no equivalen a global final. |
| Raíz | integración y documentación guardadas | CLI, runtime/supervisión bridge, indicadores/calendario, pilotos, SSOT. `tests/test_market_cli.py` pasó 9/9 aislado antes de pausar; no valida los últimos cambios parciales de B/E. |

### Defecto concreto conocido del código parcial

La última prueba manual del nuevo `SharedDataPlane.checkpoint()` falló en modo
`strict`: `_aggregator_checkpoint` llama `export_bucket_state()` porque el
método existe, pero ese export **sólo admite `continuous_quotes`** y lanza
`ValueError`. El punto está alrededor de `processor.py:560` al pausar. No se
corrigió después del STOP. Seleccionar la ruta por modo, preservar checkpoint
legacy y probar ambas rutas es parte del siguiente trabajo autorizado, no un
motivo para ejecutar el árbol actual como si fuera una release.

### Contratos ya introducidos que deben conservarse

- `RiskExitPolicy`/`plan_entry`/`evaluate_exit` comparten riesgo y salidas. R
  monetario inicial es `filled_quantity * stop_distance * unit_value`, inmutable;
  el envelope de pérdida prevista incluye costes conocidos estimados. Mínimo de
  stop se mide desde el lado liquidable (`bid-SL` LONG, `SL-ask` SHORT).
- Costes esperados requieren fuente/moneda y componentes explícitos. Los ceros
  de fixtures son explícitos, no inferidos para DEMO. El ejecutor requiere
  `allowed && eligible_for_demo && mode=DEMO_GATED`; un plan virtual no opera.
- `net_r` se reconstruye de net PnL / R, sólo cerrado y con costes íntegros.
  `r_multiple` canónico v2 es alias neto, legacy puede ser excursión; nunca usar
  un R opaco/MFE como autoridad económica. Mismatch es INVALID, desconocido es
  INSUFFICIENT. Spread/slippage del fill no se descuentan otra vez.
- Equity de inicio UTC persistida: arrancar a mediodía no inventa SOD. Conserva
  continuidad/mark age/generación y cashflows, no reinicia el presupuesto.
- Ejecutor usa contexto de barras UTC publicado en memoria antes de señales;
  la entrada congela ordinal al fill. Cien ticks no son cien barras. Stops del
  servidor son observacionales; no duplicar cierre cliente al cruzar SL/TP.
- Modelos opt-in: `eurusd_virtual_10k_unit_value_v1` y
  `pepperstone_public_calendar_template_v1`. Contrato `known=False`, fees
  desconocidos, protecciones del servidor modeladas/no observadas. Break
  correcto NY 16:59–17:01, no medianoche NY. No mapear break diario a
  `market_cut_at`: forzaría a cerrar MULTIDAY diariamente. Usa `daily_cut_at`;
  multidía no tiene precorte de financiación diario, sí cierre semanal.
- `HistoricalStreamIdentityGuard(manifest)`, `validate(quote)`, `snapshot()` y
  `from_snapshot(manifest, snapshot)` conservan cursor O(1) y tabla O(particiones),
  no un set O(ticks). Rechazan retorno de partición y repetición de locator aunque
  cambie sequence. Igual timestamp con distinta posición fuente sí es válido.
  Fresh exige primera partición/member: un suffix necesita snapshot o validación
  del prefijo completo. No verifica raw: depende del reader/manifest y fences.
- UI `--snapshot` evita resolver configuración/SQLite y es excluyente de `--db`.
  Durante una corrida cercada no abrir bases vigiladas, ni siquiera `mode=ro`.

## 6. Runtime y herramientas

V2 privado, **STAGED, NO PROMOVIDO**:
`/home/winterboss/.local/share/mtf-lab/runtime-review-20260913-1638/runtime/`.

- Desarrollo: `dev-python/bin/python`.
- Ejecución candidato: `runtime-python/bin/python`.
- Python 3.12.14, SQLite 3.53.1 efectivos en proceso nuevo con `-I -B`.
- Receipt `STAGED_RUNTIME.json`, SHA
  `e3326fdbf975fac5de341e33cbd69e9024b9389b87c7cb3f9512b89576babbed`.
- Protobuf 7.36.1 local fijado; las licencias/bytes del runtime fueron retenidos
  fuera del caché de Codex. El wheel dentro de V2 es **intermedio**, no el código
  final de esta campaña. Para QA se usó el source explícito/copia cercada.

V1 en `/home/winterboss/.local/share/mtf-lab/runtime/` es **rechazado, no usar**:
`pyvenv.cfg home` relativo rompía `-I` (`/install`, encodings ausente) y el wrapper
eliminaba PYTHONPATH/guard. V2 usa rutas absolutas y conserva el guard. El supuesto
fallo Pyright V2 resultó no reproducible: se había usado V1. `runtime-python` no
tiene Pyright por diseño; debe ejecutarse con el intérprete de desarrollo.

Para Pyright offline, con `/usr/bin/node` presente, la comprobación V2 usó
`PYRIGHT_PYTHON_IGNORE_WARNINGS=1`, `PYRIGHT_PYTHON_USE_BUNDLED_PYRIGHT=1`,
`PYRIGHT_PYTHON_GLOBAL_NODE=1`, estado HOME/XDG/TMP aislado. No permitir que el
wrapper descargue Node, paquetes o consulte versiones remotas durante QA.

El runtime operativo `.venv`, launchers públicos, servicios y host permanecen
sin promoción/sustitución por esta campaña. No hay resistencia 72h ni shadow
iniciados. La revisión APT y cualquier cambio operativo son fases posteriores.

## 7. Ruta crítica para cuando Víctor reanude

1. Confirmar reanudación humana y comparar snapshot/árbol/HEAD. Leer README,
   este handoff y `MTF-VAL-001`; verificar runtime/modelos efectivos. No continuar
   sólo porque un objetivo técnico figure activo.
2. Terminar y revisar el acumulador B y el data plane E. No superponer writers:
   B `aggregation.py`; E `processor.py`/`historical_backtest.py`; G guard separado;
   D `market_data_service.py`/test nuevo; raíz CLI/docs/QA final.
3. Corregir el checkpoint strict conocido; preservar identities/byte behavior
   legacy. Integrar guard y cursor a checkpoint/resume. `seen_signal_ids` y
   `risk_ledger_signatures` aún requieren límite/durabilidad demostrados, no
   poda arbitraria. No declarar memoria acotada por un flag.
4. Añadir los tests específicos que aún no se crearon: acumulador, plane
   compartido, CLI descriptiva registrada. Completo/bloques/resume deben ser
   equivalentes, sin duplicación de exposición, ni cambios retroactivos por
   datos futuros. Reutilizar indicadores sin compartir estado mutable de
   estrategia ni alterar sus señales.
5. Repetir los mismos prefijos reales sobre la revisión, con fuente durable,
   quote sequence hash idéntico, coste/latencia/modelo iguales y desglose de
   decodificación/procesamiento/checkpoint. Comparar funnel/ledger/equity y
   etiquetas por semántica, no sólo elapsed/RSS. No ejecutar ahora estos pasos.
6. Después, revalidar semana/mes descriptivos reteniendo fuentes completas y
   probar un prefijo mayor del backtest antes de mes/año. No extrapolar de 8k
   ticks ni asumir suficiencia D1 por haber cumplido un smoke.
7. Integrar costes completos con fuentes reales/condicionales explícitas:
   `_legacy_cfd_config` aún fija comisiones desconocidas y financiación None;
   métricas del backtest siguen gross/unknown incluso con spec conocida. No
   llamar a esto contabilidad neta completa ni falsear `known=True` para avanzar.
8. El registro protege holdout, pero el runner histórico aún rechaza todo 2024+
   en `_partition`. Los tests de capability con runner stub **no prueban una
   campaña holdout real**. Falta seam de permiso tipado/verificado y test con
   fixture fechada 2024 antes de una futura campaña; no relajar el guard ahora.
9. Gate global desde fuentes finales: Ruff/formato completos, mypy estricto
   con imports normales, Pyright completo, arquitectura, suite aislada sin
   omisiones, cobertura líneas y ramas >=60%, codec real, wheel reproducible
   y smoke instalado desde otro cwd. No sumar pruebas focales ni usar el
   antiguo 653/653 de H0–H6 para cerrar esta ampliación.
10. Sólo tras calidad/procedencia/capacidad ampliar HistData al año de desarrollo.
    Resolver el contraste/licencia Dukascopy antes de tratarlo como disponible.
    Las fases multiaño/holdout/operativa/DEMO siguen pendientes, y publicación
    e instalación no se infieren de completar código.

Helpers existentes (no invocarlos durante la pausa):

- `/home/winterboss/MTF/runtime/market-evidence/run_structure_pilot.py`: copia
  durable, manifests pre/copy/post, helper/child/runtime y manifest original;
  cuenta decoded y selected. **Pendiente:** `_frozen` compara además MAIN con
  la copia y marcaría fallo por cambios independientes del checkout. Separar
  drift de MAIN de la verdadera fence de la copia ejecutada; comprobar también
  el inventario pre/post, no sólo hashes de la lista inicial.
- `/home/winterboss/MTF/runtime/market-evidence/run_backtest_pilot.py`: prefijos
  registrados 2048/8192/32768, runtime aislado y fuentes retenidas; `--source`
  reutiliza una copia verificada. Sólo se ejecutaron 2048 y 8192. Es helper QA,
  no API productiva ni sustituto del guard confirmatorio. No ejecutar 32768 sin
  verificar antes el estado/memoria y los cambios pendientes.

## 8. Qué no se debe perder ni reinterpretar

Conservar raws, manifests, registry y sus errores, código sin commit, snapshots,
licencias, runtime V1 rechazado/V2 staged y receipts originales. No limpiar nada
en esta pausa. QA regenerable no se convierte por ello en una nueva fuente
canónica; los artefactos útiles y sus referencias ya viven fuera del vault.

El diagnóstico útil hasta aquí es **evidencia insuficiente para rentabilidad**,
con problemas reales de cobertura y rendimiento identificados. No hay candidato
descartado por estos prefijos, aprobación económica, publicación nueva,
instalación operativa, correo enviado, OAuth, órdenes ni fills DEMO observados.

La reanudación debe continuar desde estas dependencias, no volver a crear una
campaña paralela ni modificar los umbrales para obtener un resultado favorable.

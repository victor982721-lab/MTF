# Piloto histórico — estado y límites de la evidencia

Verificado el 2026-09-18. Este registro distingue resultados descriptivos
intermedios, implementación, validación de una release y operación externa.
No hay todavía una estrategia seleccionada ni evidencia de ventaja neta.

**La pausa se levantó por instrucción humana expresa («Puedes continuar») el 2026-09-13.**
El [handoff](handoffs/2026-09-13-market-evidence-pause.md) conserva el punto anterior,
los bytes y las limitaciones; la integración continúa desde el árbol y el snapshot
comparados. Aún no es una release y no se deben ejecutar datos/órdenes fuera de los
gates descritos.

## Datos reales conservados

HistData EUR/USD, Generic ASCII Tick Bid/Ask, con 2016 completo, 2017 y 2018
completos, y 11 de 12 meses válidos de 2019. Se conserva el timestamp EST fijo
original y su transformación UTC (+5 horas), sin corregirlo para hacer coincidir
un calendario. El piloto descriptivo documentado en este archivo sigue siendo
sólo marzo de 2016: su contenedor mensual contiene 1,979,243 ticks y la ventana
del 7 al 14 de marzo UTC selecciona 499,804. Los timestamps de receipts se
expresan en UTC (sufijo `Z`); la fecha de verificación del documento usa
`America/Mexico_City`.

- Manifiesto: `/home/winterboss/.local/share/mtf-lab/market-data/manifests/histdata-eurusd-201603.json`.
- Raw: 10,160,571 bytes; SHA-256 `8357ef823ac27d9c53da9acff6f79b6e8fc058cea9fdf8f32be08bf06dbe3e1b`.
- Cobertura del contenedor: 2016-03-01 05:00:00.400 a 2016-04-01 04:59:59.277 UTC.
- La validación de formato, hash y orden terminó sin incidencias. Esto no
  acredita cobertura completa, liquidez ejecutable ni un calendario de cuenta.
- Abril adquirido por el formulario web oficial, con manifiesto
  `/home/winterboss/.local/share/mtf-lab/market-data/manifests/histdata-eurusd-201604.json`:
  raw de 9,369,977 bytes, SHA-256
  `c02c329d8bac5f49c094d5d41b413f5beffd12eed120591c54fece4346d119ac`,
  1,827,066 ticks y cobertura 2016-04-01 05:00:00.777 a
  2016-04-29 21:59:58.440 UTC. El archivo es modo 600 y no sustituye al raw
  de marzo.
- El manifiesto compuesto marzo–abril está en
  `/home/winterboss/.local/share/mtf-lab/market-data/manifests/histdata-eurusd-201603-201604.json`,
  SHA-256 `4a3b1af127990294853eaaca4ade985a83ef0e1c51ef7401ce07b99f5133cb8f`,
  `content_hash` `74b4d280c75dc55808e385b58974f221a6f7954f0bf93d048cab9d08fe064d7e`
  y 3,806,309 ticks. Marzo y abril son contiguos y pasaron validación
  streaming; no se ha ejecutado aún el backtest de ese compuesto.
- Se completó la adquisición gratuita del año 2016 (12 ZIPs, enero–diciembre)
  y cada manifiesto mensual pasó validación. El manifiesto compuesto anual está
  en `/home/winterboss/.local/share/mtf-lab/market-data/manifests/histdata-eurusd-2016.json`,
  SHA-256 `3b1edbe64f5f349cf99246b3da8cb4f1223ea4ce0c197baa4c57567dd916ab87`,
  `content_hash` `3beebfb5ed44b15fa9fb55699008fefeed329bf6659604ffb7101605c9406810`,
  `dataset_id` `histdata:EUR/USD:201601-201612:3beebfb5ed44b15fa9fb55699008fefe`,
  19,026,438 ticks y cobertura 2016-01-03 22:00:15.493 a
  2016-12-30 21:59:20.383 UTC. La composición fue exclusiva, ordenada y sin
  colisiones. El backtest anual `development-2016-full-v17` terminó en
  `COMPLETED_DEVELOPMENT_REVIEW_ONLY`, sin candidato seleccionado.
  El checkpoint terminal conserva `finished=true`, `status=COMPLETED`, cursor
  `2016-12-30T21:59:20.383000Z` y 19,026,438 cotizaciones; su SHA-256 es
  `f2b4e084aa1e3797a0f0da34bd0caa0c3840d470b96d91a9797f1dccb54348ce`.
  Receipt terminal:
  `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/runs/development-2016-full-v17/resume-receipt-bd14ebb22e894eec8eeb142994c56477.json`,
  SHA-256 `e987bb99de2d67fbafcf0cef219de1c6f24561e6258d526cca96e9c84e0bca01`.
  La corrida es `resumable=false`, `promotable=false`, con
  `UNKNOWN_COSTS`, `economic_conclusion=NOT_ASSESSED`, `holdout=CLOSED`, sin
  red, escritura de base de datos, selección, promoción ni trading.
  La auditoría de terminalidad, offsets, artefactos y registry está en
  `runtime/market-evidence/development-2016-v17-terminal-audit-20260917.json`,
  SHA-256 `920c18ced374bb4dee8ec4d53f1f84083de6946510dc156bee76b240cd2d6122`.
- 2017 está completo y validado: 12/12 meses, 14,125,996 cotizaciones. Su
  manifiesto anual es
  `/home/winterboss/.local/share/mtf-lab/market-data/manifests/histdata-eurusd-2017.json`,
  SHA-256 `d750e8abda20d4a368f5df7342ea93a8ad48f6c27c420dcd2dc3c8260b08a56b`.
- 2018 está completo y validado: 12/12 meses, 18,393,327 cotizaciones. Su
  manifiesto anual es
  `/home/winterboss/.local/share/mtf-lab/market-data/manifests/histdata-eurusd-2018.json`,
  SHA-256 `43d593a20cdbca5ca2e86b6f3ecda5bea83c47754f8faa86406cbca86c7350bd`.
- 2019 tiene 11/12 meses válidos (`201901–201909`, `201911–201912`), con
  26,877,692 cotizaciones. `201910` no se cuenta como válido.
- El raw de `201910` se conserva sin sobrescribir ni corregir en
  `/home/winterboss/.local/share/mtf-lab/market-data/raw/HISTDATA_COM_ASCII_EURUSD_T_201910.zip`
  (SHA-256
  `fd4d1765b95b592ebee9e2c3e03a2a8bf973796257bb20ea327a3dad63bde5f1`), pero
  falla por `source order violation at sequence 1929387`.
- No existe manifiesto anual de 2019 ni manifiesto compuesto `2016–2019`.
- El holdout 2024–2025 permanece cerrado.
- El inventario físico agregado actual de HistData/Dukascopy cuenta 49 archivos y
  435,790,724 bytes, con proyección dentro de 40 GiB y reserva libre superior al
  20%. Receipt vigente:
  `runtime/market-evidence/storage-budget-live-20260919T013007Z.json`, SHA-256
  `2afb1ffc036ba48c76cb143702c4022e79624d7c99158d4571e561b6d4aabf70`.
- El QA descriptivo estricto de 2017 y 2018 terminó sin incidencias, sin red ni
  estrategia: 14,125,996 y 18,393,327 cotizaciones, respectivamente. El receipt
  consolidado es
  `runtime/market-evidence/qa-descriptive-2017-2018-20260919T034200Z.json`, SHA-256
  `c39e49ac3fca03a782157c5e01d9970d68014929092137f3e2cca03873d16be5`; los
  reportes JSON/HTML y sus hashes están bajo
  `/home/winterboss/.local/state/mtf-lab/research/market-structure/`.
- La anomalía de `201910` quedó documentada sin alterar bytes en
  `runtime/market-evidence/histdata-201910-source-order-block-20260919T034708Z.json`,
  SHA-256 `5f9fdf370dc58138612409f5ba9f3c146abb60fc31b452f781a631d5f869a99f`:
  el ZIP tiene 2,275,024 filas y el bloque repetido contiene 1,236 filas idénticas.
  El QA anual y este receipt de anomalía se generaron contra `HEAD`
  `423cdc731d6789deb8bc9105d12ed0fc01101fa2`; el código/documentación actual
  está en `HEAD` `d951ed5ab8391098e307eb13cdd42f59350d8e11` y no se deben confundir
  sus procedencias.

El contrato de lectura acepta una composición explícita de particiones
mensuales contiguas 2016–2019 (`manifest_from_histdata_archives`), con identidad
por mes/ruta/hash, orden global y rechazo de gaps, colisiones y meses de
holdout. La composición anual de 2017 y 2018 quedó validada. La composición
2019 y el compuesto `2016–2019` permanecen cerrados hasta resolver `201910`;
no se desplazan timestamps, no se ordenan artificialmente los registros y no
se relabela el raw inválido como válido.
La FAQ no fija derechos de redistribución/retención: los futuros raw se
mantienen locales y privados hasta aclaración escrita, sin FTP/SFTP de pago.

## Primeras mediciones descriptivas

Un trabajador, sin SQLite, con estado HOME/XDG/TMP aislado y guardas de red y
escritura. Se comprobó que el raw, el manifiesto y las fuentes ejecutadas no
cambiaran durante cada corrida. El pico de memoria procede del contador del
sistema operativo, no de una corrida instrumentada con un profiler.

| Ventana evaluada | Ticks seleccionados | Tiempo de pared | CPU | RSS máximo |
|---|---:|---:|---:|---:|
| 7–14 marzo UTC | 499,804 | 173.881 s | 173.297 s | 178.762 MiB |
| Contenedor marzo | 1,979,243 | 362.122 s | 361.648 s | 179.258 MiB |

**La lectura decodifica el contenedor mensual completo antes de filtrar la
ventana.** Ambas mediciones incluyen ese trabajo de lectura, aunque la semana
analiza menos ticks. La relación entre ambos tiempos no demuestra una
aceleración, crecimiento anual lineal ni capacidad anual. Tampoco mide el
backtest de las seis estrategias.

Artefactos originales, sin sobrescritura:

- `/home/winterboss/.local/state/mtf-lab/research/market-structure/201603-week-a39c3705472b/`.
- `/home/winterboss/.local/state/mtf-lab/research/market-structure/201603-month-e4d9bb3d95d4/`.

Cada carpeta conserva `structure.json`, protocolo, manifiesto de fuentes y
receipt. En estas primeras corridas se conservó la identidad de las fuentes,
pero no la copia completa de sus bytes: **no constituyen aún una entrega de
replay íntegro desde una release retenida**. Se preservan como observaciones
intermedias y se revalidarán desde fuentes conservadas; no se reescribirá su
evidencia para atribuirles una garantía que no tuvieron.

## Hallazgos, no reglas de trading

- Spread medio por tick: aproximadamente 0.3683 pip en la semana y 0.3678 pip
  en el mes. Es un promedio ponderado por frecuencia de ticks, no una tarifa
  ejecutable ni el coste completo de una operación.
- Ocho intervalos superiores a 90 segundos en la semana y 54 en el mes. El
  contador incluye cierres de mercado; no todos son fallos del proveedor.
- El domingo 13 de marzo abre a las 22:00:34.657 UTC en esta fuente. El modelo
  aproximado de cierre FX a las 17:00 de Nueva York esperaba apertura a las
  21:00 UTC tras el cambio DST. Esa diferencia se conserva como incertidumbre
  de cobertura/horario; no se altera EST fijo ni se imputa una hora de ticks.
- Días con cotizaciones no equivalen a sesiones completas. H4/D1 aún requieren
  calentamiento y evaluación de continuidad antes de interpretar señales.
- Dukascopy no ha producido un dataset: la consulta de metadata terminó en
  timeout, lo que no prueba indisponibilidad permanente. La revisión de sus
  [términos oficiales](https://www.dukascopy.com/swiss/english/legal-pages/terms-of-use/)
  identificó después un gate independiente: consentimiento escrito previo
  para acceso automatizado y restricciones para construir bases de datos.
  No habrá más adquisición automatizada sin resolver ese alcance con el
  proveedor. Una exportación manual no se presume suficiente para resolver
  todas las condiciones de uso. Se conservan los receipts fallidos; cero
  ticks Dukascopy descargados, sin evasión, cambio de fuente ni compra.

## Preflight real del backtest, anterior a la optimización

Dos prefijos del mismo flujo del 7 de marzo se ejecutaron con los seis perfiles,
desde una **copia durable idéntica** del código, fuera del checkout en edición.
Los seis intentos por corrida se registraron antes de procesar el mercado.
Modelo explícito virtual USD 10,000 y calendario público retroproyectado;
costes completos desconocidos, sin selección ni habilitación DEMO.

| Prefijo seleccionado | Filas decodificadas | Pared | CPU | RSS máximo | Checkpoint final |
|---|---:|---:|---:|---:|---:|
| 2,048 ticks, hasta 00:47:42.460 UTC | 378,623 | 22.630 s | 22.507 s | 194,236,416 B | 34,026,934 B |
| 8,192 ticks, hasta 03:40:27.180 UTC | 384,767 | 51.302 s | 51.040 s | 336,494,592 B | 58,481,547 B |

La decodificación incluye el prefijo mensual anterior al 7 de marzo; no se
compara solamente el número de ticks seleccionados. Hubo cero operaciones
simuladas en estos prefijos cortos: las variantes lentas no están calentadas y
los Donchian todavía no producen ruptura. Esto **no descarta candidatos** ni
demuestra la ruta económica con operaciones reales de mercado.

Se verificaron cero intentos de red/escritura fuera del guard y estabilidad de
raw/manifiesto/copia ejecutada. Artefactos y fuentes retenidas:

- `/home/winterboss/.local/state/mtf-lab/research/market-backtest/201603-baseline-2048-e49ae3895855/`.
- `/home/winterboss/.local/state/mtf-lab/research/market-backtest/201603-baseline-8192-fd978d2fc173/`.
- Fuente común: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/201603-baseline-2048-6e0bca15b050/source/`;
  identidad `561f61c8263a9f0ec92dded23fa0d9830e13bbfddfa6bed517c0f62b0bbc8f4c`.

La carpeta que contiene esa fuente conserva también un primer fallo de
preparación JSON del helper, anterior a leer mercado. La corrección y los
intentos posteriores están separados, sin reemplazar su evidencia.

La inspección y la prueba controlada localizaron crecimiento de listas de ticks
por barra, checkpoints repetidos y cálculos idénticos por candidato. También
había conjuntos de IDs que crecen con las señales. Ya se incorporaron un
acumulador online, un `SharedDataPlane` y un guard de identidad; el campo
original `ticks_retained_in_memory=0` no describe la implementación previa y no
se usa como evidencia.

La comparación posterior usó las mismas entradas y el mismo runtime, con una
copia durable nueva del código. Para 2,048 ticks: 378,623 filas decodificadas,
19.880 s de pared, 19.826 s de CPU, RSS máximo 103,395,328 B y checkpoint
11,393,829 B. Para 8,192 ticks: 384,767 filas decodificadas, 38.977 s de
pared, 38.897 s de CPU, RSS máximo 147,861,504 B y checkpoint 19,984,455 B.
La baseline equivalente había medido 22.630/22.507 s, 194,236,416 B y
34,026,934 B; y 51.302/51.040 s, 336,494,592 B y 58,481,547 B.

Los conteos y campos económicos/funnel no-ID resultaron iguales en ambas
comparaciones; los `evaluation_id` cambian porque la identidad de código/plane
es distinta y eso queda registrado. La mejora observada es un resultado de
estos prefijos, no una capacidad anual ni una garantía de rendimiento. Falta
validar el conjunto global y prefijos más largos antes del mes/año de backtest.

La descripción de mercado ahora puede registrar `NO_TRADE_DESCRIPTIVE` antes
de consumir cotizaciones y cerrar `COMPLETED`/`FAILED` con receipt. El guard de
streaming enlaza cada quote a manifest/locator y los checkpoints de backtest
enlazan guard y plane. Estos contratos están focalmente probados. El
procesamiento conserva bloques de 2,048 cotizaciones y la política durable por
defecto toma un snapshot completo cada 32 bloques (65,536 cotizaciones), sin
cambiar la granularidad de cálculo. Una parada solicitada fuerza un snapshot
inmediato y no duplica el snapshot si coincide con la frontera periódica; la
cadencia queda ligada al `config_hash` y al checkpoint para impedir una
reanudación con política distinta. La prueba de equivalencia parcial→resume
conserva artefactos y estado byte a byte; el snapshot periódico sigue siendo
`CHECKPOINTED` y el final sigue siendo `COMPLETED` terminal. Después se integró
la proyección económica explícita: un `contract_spec` incompleto o modelado deja
`UNKNOWN_NOT_ZERO`; no se adopta comisión/financiación por defecto y el
multiplier de estrés sólo escala campos expresos. El guard agregado de raws
HistData/Dukascopy y el contrato aislado WF ya están implementados y probados,
sin habilitar adquisiciones o ejecución. La validación previa del diagnóstico
causal pasó 955/955 tests, cero omitidos/fallos, Ruff/formato/mypy/Pyright/
arquitectura y `pip check` en cero errores, con 79.717% de líneas y 62.686%
de ramas. La última referencia global previa a los cambios locales pasó 990/990
pruebas, con 79.871% de líneas y 62.989% de ramas. Receipt versionado:
`runtime/market-evidence/quality-gate-20260915-ctrader-paper-tick-v3.json`
(SHA-256 `5d572ea09264914ffc95fef5318aa4f252a165ec25e7c749e22be7080339d2b4`).
Este resultado valida el checkout y el runtime de QA de aquella referencia; no
valida los bytes posteriores del commit `58a2fdd`. No publica ni instala una
release, no abre el holdout y no acredita ventaja económica. El gate offline
completo del árbol ejecutable `HEAD=d951ed5ab8391098e307eb13cdd42f59350d8e11`
terminó `PASS`: 1,037/1,037 pruebas atendidas, sin red, con 79.910% de líneas y
63.291% de ramas. Receipt consolidado:
`runtime/market-evidence/quality-gate-current-tree-20260919T050125Z.json`, SHA-256
`f1d86b6fe1d05e312ca1a150456eade96a9a86c9c24c5fed5f92f1a2abdd2e62`.
La actualización documental posterior conserva el vínculo al árbol ejecutable
validado; no convierte este gate en release, validación económica ni habilitación
de WF/holdout.

El servicio de campaña conserva además un permiso de holdout no secreto,
ligado a candidatos, intentos, dataset, escenarios y la ventana fija
`[2024-01-01, 2026-01-01)`. Un runner genérico sin ese parámetro se rechaza y el
contrato actual `DatasetManifest` de HistData de desarrollo 2016 no puede
relabelarse como holdout; no se abrió el acceso confirmatorio ni se relajó
`_partition`.

## Ruta conectada cTrader DEMO — sólo lectura

OAuth ya quedó verificado y no se repitió: el receipt privado, fuera del
repositorio (SHA-256
`a2d6af011fa43b05187c2aace05f6b5c51e592dc9a5e506f872b4cf1f43ac72e`; no se
reproduce el identificador concreto), observa `SCOPE_VIEW`/`accounts`, la
cuenta DEMO Winter y ninguna cuenta REAL/scope de trading.

El query real verificó conexión, autenticación, discovery, catálogo (1,940
símbolos) y EUR/USD. El receipt
`runtime/market-evidence/connected-v18-20260914T195151Z.json` (SHA-256
`168ef960a9aa2ef0bf10263045bbd3652e5683ace036524bc4e17878c78faf49`)
conserva 9,999 barras M1 nativas en 20 páginas, pero `hasMore=true`: la captura
es `PARTIAL`, no un dataset cerrado, y no aporta bid/ask ni fills PAPER. La
corrección de contexto de periodo normaliza trendbars periodless sólo cuando
existe evidencia explícita a nivel de request/response; las discrepancias se
rechazan.

El watch DEMO quedó bloqueado por `available_at < event_time` y una cotización
`CROSSED`/bid=ask. Se preservó la evidencia, sin clamping, reordenamiento,
reintento, dato sintético ni orden. El gate de captura parcial es
`runtime/market-evidence/connected-v18-partial-gate-20260914T195256Z.json`.

El diagnóstico mínimo posterior reutilizó el mismo OAuth DEMO, sin OAuth nuevo,
órdenes, REAL ni SQLite: 12 `SpotEvent` reales, todos con timestamp original en
ms, `symbolId`, `bid` y `ask` crudos presentes, escala 100000, cinco dígitos,
pip 4 y sin actualización parcial. Los 12 tenían `bid == ask`, ninguno
`bid > ask`; la regla `bid >= ask -> CROSSED` los marcó no operables sin
modificar precios. El receipt y su cadena de evidencia son
`runtime/market-evidence/ctrader-diagnostic-20260915T004049Z/receipt.json`
(SHA-256 `bdac08161c9bfd726c43b30b5cf802092c79da805e7f6b3fc4e3afe934eadcf5` y
`fd77a9c2d53aad880e4057c972688ba03e7f293bddf4e9f826c34319deb1f51f`). En esa
ventana no ocurrió `available_at < event_time` (0/12); el caso previo de
`-1.576856 s` se conserva como antecedente. NTP local estuvo sincronizado y no
se alteró el reloj. La evidencia no muestra que normalización/composición haya
creado la igualdad, pero tampoco aporta una referencia independiente del reloj
del servidor; no se aplicó corrección ni se relajó el gate.

La ruta offline completa sí está comprobada con una fixture sintética:
captura → indicadores/señal → PAPER → SQLite → reporte y reanudación equivalente
(190 barras, una señal y tres trades PAPER cerrados), en
`runtime/market-evidence/paper-report-e2e-20260914T194812Z-v18/paper-report-e2e-receipt.json`.
No es evidencia de mercado real, frescura, costes, fills ni rentabilidad.

El código local en validación separa la base de análisis del origen de
cotización (sin canaria conectada posterior todavía).
Con `price_base="native"`, `ctrader watch --network` solicita un prefijo causal
cerrado/contiguo de M1/M5/M15 con un solo `cutoff`, lo pasa como
`WARMUP_ONLY` al mismo `RuntimeCoordinator` y suscribe spots y trendbars live;
`hasMore` queda sólo como procedencia de la consulta. Los trendbars periodless
requieren contexto de periodo explícito. El sink `FOREX_CFD_LOCAL_PAPER` nunca
convierte una vela en bid/ask: sólo un SpotEvent `VALID` con `bid < ask` puede
llenar. Los perfiles `mid`/`bid`/`ask` no inyectan historia nativa y calientan
con spots; EOF, backpressure o `needs_reconciliation` bloquean la continuidad
y desconectan PAPER sin reintentar ni enviar órdenes.

### Captura histórica DEMO bounded V22 — 2026-09-15

La ventana DEMO histórica acotada ya existente se conserva en
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/window-receipt.json`;
el raw EUR/USD M1 `native` es
`/home/winterboss/.local/state/mtf-lab/research/ctrader-demo/20260915-m1-bounded-v22/history-native-bounded.jsonl`,
SHA-256 `80022a3890ee6f3a867819e4c4b19b72cf5c2dfb71b18714d37464aa330f0dad`.
El receipt marca 1,327 barras en 3 páginas, ventana
`2026-09-09T22:52:00Z`–`2026-09-10T20:59:00Z`, sin gaps ni incidencias; no hay
bid/ask, `quote_events=0` ni `paper_fills=0`. La captura fue de sólo lectura,
sin órdenes ni apertura de SQLite.

La fuente mantiene `source_has_more=true` (3,000 barras/6 páginas), mientras la
selección bounded satisface `bounded_complete=true` y
`bounded_has_more=false`, con 1,327 barras seleccionadas y sin gaps reportados en la cobertura temporal. Es cobertura cerrada sólo para esa ventana, no para la consulta fuente
completa ni para historia adicional. El nuevo gate sólo acepta
`CONTINUOUS` cuando páginas y metadatos llevan el marcador bounded y las barras
M1 son adyacentes; ausencia de marcador o gap deja `UNKNOWN`/fail-closed. El
`END` legacy de V22 fue admitido por el fix compatible sólo después de volver a
verificar esas condiciones. El replay local sobre el raw existente quedó
comprobado con reporte y reanudación byte-equivalentes, `coverage_satisfied=true`,
`CONTINUOUS`, 142 señales y `paper_capture_complete=true`; los 426 intents PAPER
quedaron `UNKNOWN` por ausencia de bid/ask y `FILLED=0`. El receipt compacto es
`runtime/market-evidence/ctrader-real-history-paper-e2e-20260915.json`; el receipt
de captura no acredita por sí solo fills, ventaja neta ni rentabilidad. La prueba focal es
`../tests/test_ctrader_historical_paper_gate.py`.

## Canario de campaña sobre el año 2016

Con el manifiesto anual validado se ejecutó un prefijo acotado de 32,768
cotizaciones (hasta 2016-01-04 09:08:26.377 UTC) desde el runtime aislado y una
copia del código. Los seis intentos se registraron antes de leer la fuente y
terminaron `COMPLETED_DEVELOPMENT_REVIEW_ONLY`; hubo 23 señales y 8 cierres
virtuales (`dc_m5_v1` 18/7, `dc_m15_v1` 5/1), todos con `UNKNOWN_COSTS`, sin
selección/promoción y con holdout `CLOSED`.

- Receipt: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/runs/development-2016-prefix-32768/receipt.json`,
  SHA-256 `7bf668b0106308bcfb64ecafb72643f23432ed59e7b786c339b8ac9d66596a4c`.
- Registry: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/registry.jsonl`,
  SHA-256 `54507e64f83d4ec5ae5a60dcbe1a5e0464f1d4456202716a92dd1d898a5db7af`,
  seis intentos registrados y seis estados terminales `COMPLETED`.

Es un canario de integración y streaming, no el backtest anual ni una
selección de candidato; la ventana restante y la economía completa requieren
otro gate y más evidencia.

Se amplió el canario a 131,072 cotizaciones del mismo manifiesto anual (hasta
2016-01-05 12:29:09.090 UTC). Los seis intentos adicionales se preregistraron y
completaron; `tp_fast_v1` produjo 10 señales/2 cierres, `dc_m5_v1` 68/7,
`dc_m15_v1` 22/1 y `dc_h1_v1` 3/3; los otros perfiles no señalaron. Todas las
entradas quedaron bloqueadas por `UNKNOWN_COSTS`, `entry_allowed=0`, sin
selección/promoción y con holdout `CLOSED`. Sigue siendo un prefijo de
integración, no el resultado anual ni evidencia de ventaja neta.

- Receipt: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/runs/development-2016-prefix-131072/receipt.json`,
  SHA-256 `c1b257f9304d246b0b910cf7e4935709069f13f4f14bb5805021fbcebc19015c`.
- El registry conserva ahora 12 intentos (dos prefijos de seis candidatos) y
  SHA-256 `54446db96581de3290cd89f5f707c9936d28cda1f8d5e35d6d3f9f52ec683faa`.

Se ejecutó un tercer prefijo, de 262,144 cotizaciones, hasta
2016-01-07 01:31:35.493 UTC. Los seis intentos adicionales terminaron
`COMPLETED_DEVELOPMENT_REVIEW_ONLY`: `tp_fast_v1` 42 señales/2 cierres,
`dc_m5_v1` 115/7, `dc_m15_v1` 36/1 y `dc_h1_v1` 7/7; los otros perfiles no
señalaron. `entry_allowed=0` en todos los perfiles porque los costes siguen
`UNKNOWN_COSTS`; no hubo selección/promoción y el holdout permaneció `CLOSED`.

- Receipt: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/runs/development-2016-prefix-262144/receipt.json`,
  SHA-256 `75df9346ad196c9466f63f095b6293d704c18d5a1347dc9b187e1b7bc8886da0`.
- El registry conserva ahora 18 intentos (tres prefijos de seis candidatos) y
  SHA-256 `95e491004d5a29ae731b16ab25f63dac4c03ed609c8797f5506b1ca1cc472543`.

Se intentó un cuarto prefijo de 524,288 cotizaciones con el ejecutable V13 pero
una identidad declarada V12. Se detuvo de forma segura en 201,663 cotizaciones;
los seis intentos quedaron `FAILED` por `KeyboardInterrupt` y se conservaron
sin mezclarlos con resultados válidos (receipt SHA-256
`915dbdfa1e316c422e37e29af87bfc064b525bb22e0c9869cdda8c4125cf5639`). El
reintento con identidad V13 completó las 524,288 cotizaciones hasta
2016-01-11 14:34:38.003 UTC: `tp_fast_v1` 83 señales/2 cierres,
`dc_m5_v1` 211/7, `dc_m15_v1` 69/1 y `dc_h1_v1` 17/9; los perfiles lentos no
señalaron. `entry_allowed=0`, `UNKNOWN_COSTS`, sin selección/promoción y
holdout `CLOSED`.

- Receipt válido V13: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/runs/development-2016-prefix-524288-v13/receipt.json`,
  SHA-256 `cdc61baa9f5482867b97392f0362d1aba3ca3901ebc617c0e213278691b87838`.
- El registry al cierre de este reintento conservaba 30 intentos: 24 `COMPLETED` y 6 `FAILED`; SHA-256
  `5c5a616f267c5f309809a689a477fd086f09104e95a5153b211f62a56f7a2ea3`.
- Auditoría de integridad de los cuatro canarios válidos y del fallo preservado:
  `/home/winterboss/MTF/runtime/market-evidence/campaign-canary-audit-20260914-0829.json`,
  SHA-256 `9cd2fbcb470b8a7282a25655502e67bad761ef9d02262f408a34b924bc3f8f22`.
- Auditoría temporal y de offsets: `/home/winterboss/MTF/runtime/market-evidence/campaign-canary-audit-20260914-0835.json`,
  SHA-256 `0494d88b0568a42ce6f33b8c69a266c990b5ad5aa3aece9e6ab4ce4af38634b0`.
  Los prefijos terminales no pueden reanudarse entre sí sin un checkpoint
  parcial explícito; el desarrollo anual requiere primero ese contrato y una
  medición durable de pared/CPU/RSS.

Se ejecutó después un canario de capacidad de 1,048,576 cotizaciones del mismo
manifiesto anual, con los seis candidatos, runtime V16 único y la cadencia
durable por defecto (bloque de procesamiento 2,048; snapshot cada 65,536).
Terminó `COMPLETED_DEVELOPMENT_REVIEW_ONLY` hasta 2016-01-15 16:40:23.643 UTC:
45:42.00 de pared, 2,729.63 s de CPU de usuario, 7.12 s de sistema y RSS
máximo observado de 263,488 KiB (269,811,712 B). Se escribieron 16 snapshots
periódicos y uno terminal; el checkpoint final mide 33,738,181 B. Los seis
perfiles conservaron `UNKNOWN_COSTS`, `costs_applied=false`,
`entry_allowed=0`, `promotable=false`, sin selección/promoción y holdout
`CLOSED`; es una medición de capacidad/streaming, no el backtest anual ni una
conclusión de ventaja.

- Corrida: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/development-2016-prefix-1048576-v16/`.
- Receipt: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/2016-full/development-2016-prefix-1048576-v16/receipt.json`, SHA-256 `0a868119f4e985b7edc0f60a53d01cce8af461c303168ca7fd6ea915078ff6d2`.
- Auditoría durable de capacidad (incluye tiempo/RSS, artefactos y registry): `/home/winterboss/MTF/runtime/market-evidence/campaign-capacity-1048576-v16.json`, SHA-256 `c3f21ce74252600eb95bae55b9442f627ad2ad07aca5098578453597940f25f0`.
- El registry actual quedó en 36 intentos (30 `COMPLETED`, 6 `FAILED`), SHA-256 `059772a8ea723377b27954c5952a61eaee192d429064c0431f4ebb1e83a6cbc1`.

Se verificó el contrato de checkpoint parcial/resume con el manifiesto real:
una corrida se detuvo en 65,536 cotizaciones (`CHECKPOINTED`, `finished=false`)
y se reanudó hasta 70,000 sin registrar intentos duplicados. Métricas, estado,
cursor y los tres artefactos JSONL (`ledger`, `equity`, `funnel`) fueron
byte-identical a una corrida continua de 70,000; holdout `CLOSED`, sin selección
ni promoción y `UNKNOWN_COSTS`.

- Auditoría de equivalencia: `/home/winterboss/MTF/runtime/market-evidence/campaign-resume-equivalence-20260914-0946.json`,
  SHA-256 `5740bfe36266cd1033e6c4c298276a3649d80985fe1632046990bfa3c5179d39`.
- Gate de bloque y reintento: `/home/winterboss/MTF/runtime/market-evidence/histdata-development-gate-20260914-0815.json`,
  SHA-256 `b6c9605e01c2d0bf28671202ee9fac33552c1b7191492e80e9a23c796627e6d6`.

## Runtime y aceptación pendiente

El primer candidato privado fue rechazado por incompatibilidad con `-I` y con
el aislamiento del runner. La revisión V2 carga en un proceso nuevo Python
3.12.14 y SQLite 3.53.1 desde su destino privado, sin depender del caché de
Codex. Sus smokes aislados y codec no acreditan por sí solos la suite completa
del código final ni una instalación operativa.

Los payloads de las reviews V2/V27 históricas ya no están en el árbol de
usuario. Sus manifests y logs acotados se preservan, sin copiar los runtimes
completos, en
`/home/winterboss/MTF/runtime/market-evidence/runtime-review-legacy-evidence-20260917/index.json`
(SHA-256 `a47c3cec49f9af9afbdf39c21bde600c32c97b77317e3f6ce20f5794f05e237b`).
No se inició resistencia de 72 horas, shadow, OAuth ni órdenes DEMO por esta
limpieza.

El runtime actual fue construido por el preparador gestionado el 2026-09-17 y
promovido de forma controlada tras validar Python 3.12.14/SQLite 3.53.1/
Protobuf 7.36.1, `pip check`, imports aislados y el launcher (`--help`). El
manifiesto activo conserva el run `20260917T192349Z-0e52abd2`, en
`state=ACTIVE_RUNTIME`/`promotion_state=ACTIVE`, y su SHA-256 es
`2766dd4e1be0c8b0f417d7fd40a3a912bf628ab0839e6158f864ab12eb1a88ba`.
El receipt vigente de promoción es
`runtime/verification/runtime-lifecycle-final6-promote-20260917.json`; su hash
vigente y el vínculo al HEAD se conservan en la verificación consolidada. Los
receipts `runtime-promotion-lifecycle-20260917.json` y `*-final-verification*`
anteriores se conservan como antecedentes, no como estado vivo.
La verificación consolidada del filesystem, espacio liberado y pruebas finales
está en `runtime/market-evidence/runtime-lifecycle-final-verification-20260917-v7.json`;
la focal del lifecycle es
`runtime/market-evidence/runtime-lifecycle-focused-tests-final-v5-20260917.json`.
Dos builds independientes con `SOURCE_DATE_EPOCH=0` y mtimes distintos fueron
byte-identical; su correspondencia fuente→wheel→instalación quedó validada. La
edición documental histórica y su comprobación estática se conservaron en
`runtime/market-evidence/docs-static-20260915-v32.json`; la verificación posterior
a esta auditoría está en
`runtime/market-evidence/docs-static-20260916-post-independent-audit.json`
(receipt local; la suma vigente se conserva en el propio archivo). El receipt V19
parcial y su gate fail-closed se conservan como antecedentes.
El receipt vigente del presupuesto agregado del inventario canónico está en
`runtime/market-evidence/storage-budget-live-20260919T013007Z.json`, SHA-256
`2afb1ffc036ba48c76cb143702c4022e79624d7c99158d4571e561b6d4aabf70`. El
`storage-budget-live-20260914T2355Z.json` se conserva como antecedente histórico
del inventario de 2016, no como estado actual.
La compatibilidad V17→V20 (módulos históricos sin cambios) está en
`runtime/market-evidence/runtime-v17-v20-compatibility-audit.json`, SHA-256
`f8d32e937173ff445f106c6fd6762c5785df5473e3c37450bbf399f192db24a1`.

## Prefijo real posterior al cierre

Se midió un prefijo HistData de 32,768 ticks seleccionados (409,343 decodificados)
desde una copia durable del código actual. Resultado preflight, no selección ni
capacidad anual: 111.998 s de pared, 111.642 s de CPU y RSS máximo 173,805,568 B;
ventana seleccionada 2016-03-07 00:00:00.490 a 12:00:35.843 UTC. Los seis
perfiles permanecieron en `DEV`; `dc_m5_v1` tuvo 13 señales/9 operaciones de
riesgo y `dc_m15_v1` 4/4, todas con economía `UNKNOWN_COSTS`; los demás no
señalaron en ese prefijo. Raw/manifiesto/código retenido y guard se mantuvieron
estables, sin red ni escrituras fuera del estado de corrida.
Artefacto: `/home/winterboss/.local/state/mtf-lab/research/market-backtest/201603-revised-32768-574eb158ec3b/`;
source hash `9c0cc6401f96e43d8864435f17fb9172b1b61beba9d161717ccbc030e2681130`.

Se amplió la medición a 131,072 ticks seleccionados (507,647 decodificados),
desde otra copia durable del mismo código: 445.618 s de pared, 443.916 s de
CPU y RSS máximo 221,552,640 B. La ventana terminó el 2016-03-08
13:48:17.090 UTC; los seis perfiles siguieron en `DEV`. Hubo señales/operaciones
virtuales en `tp_fast_v1` (24/7), `dc_m5_v1` (43/21), `dc_m15_v1` (16/11) y
`dc_h1_v1` (1/1); todos los cierres conservan `UNKNOWN_COSTS`, sin selección ni
capacidad anual. El guard de red/escritura y las entradas canónicas permanecieron
estables. Artefacto `/home/winterboss/.local/state/mtf-lab/research/market-backtest/201603-revised-131072-f959641b64db/`;
resumen SHA-256 `46b0c8695e68d0d95911bb7d63bdf146e34e9591b67bccc05635d1eccf2eafc3`,
source hash `440210d823742e848efdae1a231be1c75248d13aedd1a290874832925b529f0a`.

## Ruta crítica actual

1. Mantener costes contractuales, financiación, fills y calendario histórico
   como desconocidos hasta contar con una fuente vinculada a la cuenta; los
   specs explícitos conocidos sólo habilitan neto condicional.
2. La auditoría de terminalidad, offsets, artefactos, registry y hashes de
   `development-2016-full-v17` quedó verificada en el receipt indicado arriba;
   no repetirla ni iniciar otra campaña pesada en paralelo. El gate global
   offline y la reconstrucción/promoción controlada del runtime ya quedaron
   verificados en los receipts finales; 2017 y 2018 ya están ampliados,
   validados y descritos, y 2019 permanece incompleto por la partición inválida
   `201910`. Gate actual sobre el árbol ejecutable `HEAD`
   `d951ed5ab8391098e307eb13cdd42f59350d8e11`: `PASS`, enlazado por
   `runtime/market-evidence/quality-gate-current-tree-20260919T050125Z.json`;
   sigue sin ser una release. WF 2020–2023
   necesita antes un contrato/fuente separado; el contrato y la fixture offline
   `WARMUP_ONLY → WF` con resume están implementados/validados, pero no hay
   datos WF ni consumidor productivo habilitado. No usar 2024–2025.
3. Preparar una futura campaña holdout sólo con un contrato de dataset 2024–2025,
   permiso confirmatorio tipado y acceso humano único; la resistencia 72h,
   ejecución DEMO y forward siguen cerrados (OAuth sólo lectura ya verificado;
   no se repite ni se amplían scopes).

El inventario actual ya cuenta con un guard de tamaño **agregado** para todo el
corpus, con lock común, proyección y reserva de 20%; su smoke de sólo lectura
queda en:
`runtime/market-evidence/next-phase-gate-audit-20260914T2205Z.json`, SHA-256
`b3130d1076f3227588df087aad11967a4184a624611f911401644a0cf3fe1bfa`.
La implementación y su medición canónica del inventario están en
`runtime/market-evidence/storage-budget-live-20260919T013007Z.json`, SHA-256
`2afb1ffc036ba48c76cb143702c4022e79624d7c99158d4571e561b6d4aabf70`; el
receipt de 20260914 queda sólo como antecedente histórico.
Los diseños no habilitantes para cerrar ambos contratos están en
[`storage_budget_contract.md`](storage_budget_contract.md) y
[`walk_forward_contract.md`](walk_forward_contract.md).

El [plan autorizado](market_evidence_plan.md), la [procedencia de costes](research_cost_provenance.md)
y la [revisión temporal](historical_time_validation.md) conservan los gates
económicos, estadísticos y operativos restantes.

# Calidad global de MTF Lab

## Segunda iteración offline — 2026-09-20

Código validado `ca3ebaa6806106465a3a4662df4ed71c541b913d` en
`codex/mtf-offline-hardening-20260920`, separado de `main` y del runtime instalado.
Los 13 hallazgos confirmados quedaron corregidos, con revisión independiente y
regresiones sintéticas:

- **Riesgo y recuperación:** `PARTIAL` bloquea nuevas entradas no conciliadas;
  `moneyDigits=0` conserva su escala. Recovery restaura el contador de barra y
  las protecciones, valida identidad, plan de riesgo y ledger acumulado de fills,
  y rechaza actualizaciones contradictorias sin reenviar órdenes.
- **Causalidad y checkpoints:** la disponibilidad efectiva incluye los lags de
  contexto/preparación y se consume en orden, sin barridos completos por trigger.
  Tres fixtures nominales conservan JSON público idéntico al baseline. El
  checkpoint continuo standalone recupera su acumulador; un checkpoint legado
  incompleto falla cerrado, mientras strict/shared conserva compatibilidad.
- **Archivos y protocolo:** preservación confinada con preflight, snapshot de
  fuente, padres anclados por descriptor y publicación exclusiva. Se cubren
  carreras, colisiones y múltiples licencias ZIP/TAR. Auth exige la respuesta
  esperada antes de acreditar sesión y la redacción incluye Protobuf anidado.
- **Economía y reportes:** la retención no elimina incertidumbre global. El
  puente de costes consume todas las páginas del ledger y valida los bytes
  leídos, hash y conteo al EOF antes de publicar. Los resúmenes se acotan sin
  confundir un sufijo con el conjunto completo. Los descriptores paginados de
  equity/funnel permanecen explícitamente `NOT_ASSESSED`, no se inventan filas.
- **Supervisión y UI:** el monotónico se compara sólo dentro del mismo boot,
  conservando riesgo y la guardia UTC. El deadline se comprueba tras preparar y
  capturar. Las consultas de UI fijan sesión/filtros; las páginas se pintan y
  conservan hasta una actualización explícita, sin aceptar respuestas obsoletas.

La suite completa terminó el **2026-09-20T07:23:51Z**, exit 0 y cgroup vacío:
**1,217/1,217 pruebas**, 68 nuevas, cero fallos, omisiones o resultados esperados
fallidos. Ruff, formato, mypy, Pyright, empaquetado y arquitectura aprobaron.
Cobertura: **80.268% de líneas / 63.881% de ramas**; ejecución de suite y wrapper:
1,566.796 s. La identidad de fuentes permaneció idéntica antes/después.

HOME/XDG/TMP estuvieron aislados y no hubo intentos de red externa. La nueva
auditoría bloquea escrituras Python fuera de los destinos temporales autorizados;
los smokes conservan su bloqueo estricto. Los tests de cercas usan repositorios
sintéticos y ya no modifican el checkout. **No es un sandbox del sistema
operativo:** I/O nativo de C/SQLite, Node o shell no está instrumentado y los
diagnósticos de hijos son por proceso, no un contador global agregado.

Evidencia local: directorio
`/home/winterboss/MTF/runtime/market-evidence/implementation-iteration2-20260920T054423Z`.
Su `quality-gate.json` tiene SHA-256
`b616412e1f8416039efb9ca6a9e06e81ae2459361509e3933bc78a417e17481c`;
`quality-terminal.json` verifica la terminalidad y `acceptance-map.json` enlaza
los hallazgos con sus regresiones. El wheel offline conserva 130 archivos
byte-equivalentes, SHA-256
`1f752ea25fc0c6d8672f2db86c873acd1177c6351a5cf3c6cb1e3b24fe93486c`.
Se probó en un venv efímero desde otro cwd, sin índice ni nuevas dependencias;
no se promocionó el runtime. No hubo bróker, credenciales reales, corpus,
SQLite productiva ni órdenes. La canaria original continúa sin modificaciones.

## Mejoras offline aisladas — 2026-09-20

Código validado `f3b187fdc76af70fe280bf9ecbd11313a02eebd6`, publicado en la rama
privada `codex/mtf-offline-hardening-20260920`, sin merge ni promoción operativa.
La versión y programación de la canaria DEMO permanecen separadas e intactas.

- Una confirmación de cierre que contradice la posición residual queda
  `UNKNOWN`: conserva cantidades y fills observados, sin fabricar un cierre
  parcial ni reenviar automáticamente.
- La cancelación conserva el registro acumulado de fills, incluso si la
  respuesta los omite. Repeticiones, conflictos y exceso de cantidad tienen
  regresiones de recuperación y rechazo sin reenvío ciego.
- Los identificadores de cuenta rechazan booleanos, decimales, aliases
  conflictivos y valores fuera de int64. OAuth exige strings no vacíos y
  preserva los bytes opacos de los parámetros válidos.
- El contrato puro de cotizaciones CFD está separado con 42 definiciones
  AST-equivalentes. El codec de checkpoints comparte los campos mutables,
  conserva el formato/orden v1 y su equivalencia con la versión anterior.
- El controlador y los codecs usan tipos más precisos; los 17 argumentos del
  recolector son explícitos. Se verifica que el riesgo posterior use el
  observador del binding, no la observación inicial.

El gate completo pasó **1,149/1,149 pruebas**, incluidas 24 regresiones nuevas,
sin fallos ni omisiones. Ruff, formato, mypy, Pyright y arquitectura aprobaron.
Cobertura: **80.063% de líneas y 63.488% de ramas**. La suite usó Python 3.12.14,
HOME/XDG/TMP aislados y bloqueo de red externa; la cerca de fuentes quedó intacta.
El bloqueo total de escritura corresponde a los smokes de importación/ayuda:
el contador de escritura de la suite no es una auditoría de todo el filesystem.

Receipt local:
`/home/winterboss/MTF/runtime/market-evidence/offline-hardening-20260920/quality-gate.json`,
SHA-256 `39089e13c0d1ba084eb9b2e4f2885412d6db35beea401ae4b1a4d47010fa3b16`.
El wheel se construyó sin índice ni nuevas dependencias y cargó desde un venv
efímero independiente con Python 3.14.4; sus 130 archivos empaquetados coinciden
con las fuentes. Su SHA-256 es
`4540a4533599d9a5dafe187ba2dd87f9a1daf16b7de20b993620a889cd0934b0` y los bytes
se conservan en el subdirectorio `wheels/` de la misma evidencia local.
Ese smoke no sustituye ni promociona el runtime instalado, y ninguna prueba
de esta fase contactó al bróker ni envió órdenes.

## Antecedente de alcance y metodología

Alcance autorizado el 2026-09-12: terminar la limpieza Ruff/mypy heredada y
mejorar el código sin auth, OAuth, cuentas ni conexiones al bróker. La validación
es local; los fixtures siguen siendo sintéticos y no acreditan rentabilidad,
permisos externos ni fills reales.

## Alcance sin excepciones de legado

- **Ruff y formato:** fuentes Python de `mtf_lab/`, `tests/` y `tools/`.
  La ampliación H1 de 2026-09-13 excluye sólo `data/protobuf_generated/` del
  estilo: son bytes de protoc, identificados por manifiesto y comprobados con
  `tools/generate_ctrader_protobuf.py --check`, no una excepción de código manual.
- **Mypy:** `--strict --explicit-package-bases mtf_lab tools`, con imports normales.
- **Pyright:** todo `mtf_lab/` y `tools/`, Python 3.11/Linux.
- **Arquitectura:** auditoría AST de dependencias, ciclos, efectos de importación
  y complejidad; `core` no depende de red, SQLite, CLI ni SDK.
- **Pruebas:** suite unittest descubierta de nuevo, con HOME/XDG/TMP/estado
  temporales y red externa bloqueada. Node es requerido para los tests de UI.
- **Cobertura:** líneas y ramas se informan por separado; Coverage.py también
  calcula una métrica combinada que no equivale al porcentaje de ramas.

`tools/quality_scope.py` define los directorios y los descubre en cada corrida.
Un archivo/directorio requerido ausente falla la puerta; no se filtra para
reducir el alcance. Se conservaron fachadas públicas y representaciones de enums;
no se cambiaron reglas financieras para satisfacer un analizador.

```bash
.venv-dev/bin/python tools/quality_gate.py \
  --runtime-python .venv/bin/python \
  --dev-python .venv-dev/bin/python \
  --json runtime/verification/quality.json
```

Las comprobaciones estáticas también son ejecutables individualmente:

```bash
.venv-dev/bin/ruff check mtf_lab tests tools
.venv-dev/bin/ruff format --check mtf_lab tests tools
.venv-dev/bin/python -m mypy --strict --explicit-package-bases mtf_lab tools
.venv-dev/bin/python -m pyright --project pyrightconfig.json \
  --pythonpath runtime/implementation/runtime-venv/bin/python
```

El `--pythonpath` debe seleccionar el runtime candidato realmente validado;
la configuración no fuerza el antiguo `.venv`. Para la evolución H0–H6 se usa
el mismo intérprete candidato en `quality_gate.py --runtime-python`. El ejemplo
anterior con `.venv` documenta la entrega histórica, no su aptitud externa actual.

## Cambios de comportamiento protegidos

- `DataQuality.valid()` continúa siendo fábrica de clase y `quality.valid` un
  booleano de instancia, con un descriptor tipado y pruebas de ambos contratos.
- Identidades, temporalidades normalizadas, aritmética Decimal, disponibilidad,
  reconstrucción y gates de riesgo se conservan mediante narrowing/contratos
  explícitos; no se usa `Any` como supresión general de errores.
- Se modularizan admisión de eventos/velas, restauración y evaluaciones sin
  alterar el orden causal ni eliminar evidencia de datos incompletos.
- Los checkpoints conservan también `last_event_id` al restaurar. Las señales
  `LIVE` se rechazan tanto como dataclass con serializer como mappings con enum
  o texto con espacios; se corrigió ese fallo heredado del baseline sin activar
  la ruta externa.
- No se cambia la autenticación real: sólo se validan rutas locales/controladas.

## Distribución aislada

`install-mtf-lab-wheel.sh` delega al helper stdlib `tools/wheel_installer.py`.
El build usa `build==1.6.1`, `setuptools==84.0.0` y por defecto
`SOURCE_DATE_EPOCH=0`. Trabaja en una copia temporal de fuentes; no genera
`build/` ni `*.egg-info` en el checkout activo. Pip usa `--no-index --no-deps`.
Los wheels se conservan por SHA-256 en `dist/<sha256>/`, y el smoke comprueba el
origen instalado con estado HOME/XDG separado. No instala una release personal
ni publica en PyPI como efecto de esta limpieza.
El almacén rechaza symlinks y archivos especiales (incluidos FIFO) sin seguirlos
ni bloquearse; las colisiones conservan los bytes existentes. El staging rechaza
directorios symlink para evitar omitir fuentes silenciosamente.

## Línea base

Se partió de `ea288085690ccfc617e6bcd5ba44f20925633f8f`: 356 hallazgos Ruff,
8 C901 y 202 diagnósticos mypy al ampliar el alcance real a paquete y tooling.
La auditoría AST de ese SHA tenía 123 funciones por encima de 10, máximo 38,
0 ciclos y 0 violaciones estrictas. Su métrica AST cuenta constructos distintos
que Ruff C901 y no debe intercambiarse numéricamente con ella.

## Resultado verificado

La puerta global pasó con **395/395 pruebas**, cero fallos y cero omitidas:
**53 pruebas más** que el baseline. Ruff/formato, mypy estricto (72 fuentes) y
Pyright completos terminaron en cero errores; desaparecieron los 8 C901.
La revisión cubre 117 archivos Python con Ruff, además del formato de ejemplos.

Coverage.py midió **16,767/21,160 líneas (79.239 %)** y
**4,003/6,568 ramas (60.947 %)**. El umbral actual exige 60 % en cada métrica,
no sólo en la combinación. La cobertura es del proceso de tests instrumentado;
no se presenta como cobertura total de cada subprocess de CLI/instalación.

La auditoría AST mantiene 0 ciclos/violaciones: sus funciones sobre 10 bajan
123 → 117 y el máximo sigue en 38. Eso no contradice C901 limpio: son métricas
con reglas distintas. Queda margen para aumentar cobertura de ramas, no deuda
Ruff/mypy oculta.

El [receipt compacto](../reports/quality/global-static-20260912.json) conserva
ámbito, comandos, versiones, métricas e identidad de los inputs ejecutables.
Los fuentes no cambiaron durante la suite; después sólo se agregó este cierre
documental y el receipt. El estado de `data/` mantuvo tamaño, mtime e inode en
la verificación integrada, sin abrir sus bases.

La autenticación y la observación del servidor siguen reservadas para Víctor.

## Estado posterior — 2026-09-16

El árbol actual añade la frontera local de warmup cTrader, salud del lector y
PAPER fail-closed, además de una extracción mecánica de `result_metrics` en
componentes puros. La validación focal (cTrader/warmup/reporting) y los checks
Ruff/formato/mypy/Pyright están en verde; el último gate global completo
(`990/990`) es el receipt anterior a estos bytes. El gate global nuevo y el
runtime que lo contenga se emitirán después de que termine la reanudación V17;
ningún resultado focal se presenta como una release o como evidencia económica.

## Ampliación verificada: runner continuo y observabilidad — 2026-09-12

La base cTrader de **422 pruebas** se consolidó en `9d066c6`; el runner continuo
de lectura, su CLI y el panel observacional quedaron en `f6bcb1e`. El gate
global desde este último SHA pasó **457/457 pruebas**, cero fallos, cero
omitidas y cero intentos de red externa. No se redujo el alcance: Ruff/formato
cubre 127 fuentes, mypy/Pyright todo el paquete y tooling, y la auditoría
arquitectónica sigue sin violaciones estrictas.

Cobertura de esta ampliación: **18,481/22,964 líneas (80.478 %)** y
**4,506/7,222 ramas (62.393 %)**. HOME/XDG/TMP/estado estuvieron aislados; los
fuentes no cambiaron durante la corrida. Los metadatos de los dos archivos
presentes en `data/` se conservaron sin abrirlos para verificarlos.

El [receipt compacto](../reports/quality/ctrader-watch-observability-20260912.json)
vincula el SHA y los inputs con límites/parada, checkpoint atómico, rechazo de
reanudación ambigua, validación del endpoint antes de credenciales y la
proyección observacional. El QA visual usó una fixture aislada y un servidor
loopback de sólo lectura; comprobó las tarjetas, el aviso de error y la
recuperación sin perder el último dato bueno. El servidor temporal se cerró.

La revisión independiente no dejó bloqueantes. El cierre posterior es sólo
documentación y receipt; conserva los mismos inputs no documentales y deja
commits locales, sin push ni instalación de release. La conexión real DEMO
permanece como compromiso externo independiente.

## Evolución CFD y fiabilidad DEMO — 2026-09-13

La ampliación local desde `6d50a1a` quedó validada en `c1246cc`: **653/653 pruebas**,
cero omisiones y cero intentos de red/escrituras fuera del guard. Los gates de
Ruff, formato, mypy estricto, Pyright, arquitectura, codec y empaquetado aprobaron.
Cobertura: **25,888/32,608 líneas (79.392%)** y **6,614/10,600 ramas (62.396%)**;
ambos umbrales siguen en 60%. No se reemplaza el resultado histórico anterior.

El [comprobante](../reports/quality/mtf-demo-reliability-20260913.json) identifica
SHA/árbol/inputs, wheel reproducible, QA de UI, aislamiento y preservación de datos.
La [medición por rutas](../reports/quality/mtf-demo-reliability-performance-20260913.json)
separa velocidad, memoria y perfil CPU diagnóstico. Sólo tres archivos upstream
con whitespace original tienen una regla exacta `-whitespace`; no hay filtros de
limpieza/EOL y sus bytes y blobs se conservaron. La excepción de estilo del codec
generado permanece acotada y comprobada con protoc.

El cierre posterior sólo modifica documentación y comprobantes. No se publicó,
instaló la release operativa ni activó DEMO. Las 72 horas locales, el gate SQLite
y la aceptación externa permanecen separados en la matriz H0–H6.

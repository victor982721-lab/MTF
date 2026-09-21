# Estado operativo y preservación

Verificación del 21 de septiembre de 2026. La publicación de código no es una
release de trading, una aceptación contractual ni una conclusión de rentabilidad.
El [receipt versionado del 19 de septiembre](../reports/quality/mtf-operational-status-20260919.json)
se conserva como antecedente; la evidencia vigente de esta promoción se enlaza
abajo y permanece fuera de Git.

## Estado vigente — 2026-09-21

**Release validada e instalada; canaria `CURRENT_REVIEW`.** El código
`0a6adfb5b9bf1915edb43399495ae10e08ae9707` pasó **1,315/1,315 pruebas**, cero
fallos/omisiones, 80.463% de líneas y 64.290% de ramas. La instalación usa
`run_id=20260921T231515Z-61e48a36`, 132 archivos/RECORD byte-equivalentes en
ambos entornos y tres launchers verificados. El rollback conserva
`37b30cd908588baa57d2d003ea0e488dee3d3991` con 132 archivos. Esta identidad de
código es independiente del cierre documental posterior, que conserva el
mismo árbol ejecutable validado.

El launcher ejecuta `/home/winterboss/MTF/tools/demo_canary.py` desde
el checkout con Python instalado; los smokes acreditan la biblioteca/runtime
empaquetados, no imports del controlador fuera del checkout.
La canaria queda **`CURRENT_REVIEW`/`NO_EJECUTADA`**. La ejecución
`execution-20260921T233107Z` terminó a las 23:37:36.570 UTC, tras 372.825 s,
con exit 2 y `OBSERVED_INPUTS_BLOCKED`; su razón fue
`producer aún no tiene ATR válido para el timeframe trigger`.

Capturó 101 eventos. El watcher terminó por `IDLE_TIMEOUT` con límite de 30 s
antes de disponer de ATR14; al detenerse reportó continuidad `CONTINUOUS`,
frescura `VALID`, reconciliación `NOT_APPLICABLE`, `block_details={}` y sólo
calentamientos/señales técnicas deshabilitadas. El único rechazo BBO registrado
fue histórico y preventana (`BBO fuera de la ventana aprobada`); no es la causa
final. Los datos fueron aceptados sin bloqueos internos de continuidad o
normalización; no se atribuye la pausa exclusivamente al feed o al bróker.

El receipt final conserva `orders_attempted=false`, `cycles_completed=0`,
ledger ausente, aprobación no reservada y journal de 0 bytes. Los PID/PGID
quedaron terminales; no se afirma cgroup vacío. No se inventan precios ni ATR,
no se amplía el límite de frescura ni se baja el periodo. El objetivo de dos
ciclos no se cumplió y la validación económica sigue independiente. No se programó otra
fecha ni se crearon automatizaciones de esta canaria.

Víctor autorizó dos excepciones humanas sólo para esta canaria DEMO: aceptar un
`bid == ask` genuinamente observado y usar el equity inicial observado como
ancla de pérdida del trial. El ancla diaria sigue desconocida. Fuera de ellas
la regla predeterminada `bid >= ask -> CROSSED` permanece estricta y `bid > ask`
continúa rechazado; no se amplían a PAPER, estrategia, watch, REAL/LIVE, 72 h,
otra fecha ni automatización.

Una comprobación de sólo lectura documentó 77 eventos en 240 s, continuidad
`CONTINUOUS` y cero rechazos BBO o de sus piernas. Verificó las correcciones del
cierre por reloj y la igualdad autorizada con actualizaciones asíncronas, sin
órdenes ni evidencia de ATR14.

Evidencia privada vigente, sin credenciales ni corpus en Git:

- [Gate completo](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/quality/20260921T231507Z/quality-gate.json)
  y [auditoría terminal](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/quality-terminal-audit-v7.json).
- [Instalación](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/installed-canary-v7.json),
  [rollback](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/rollback-verification-v7.json),
  [promoción](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/runtime-promotion-v7.json)
  y [publicación](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/code-publication-v7.json).
- [Comprobación de lectura de cuatro minutos](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/live-quality-confirmation-v7.json).
- [Resultado terminal final](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/execution-20260921T233107Z/result.json),
  [controlador](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/execution-20260921T233107Z/controller.stdout.json)
  y [estado del host](/home/winterboss/.local/state/mtf-lab/demo-canary/20260921-now-155034Z/trial-anchor-implementation/zero-spread-implementation/execution-20260921T233107Z/host-terminal-state.json).

No se acredita ejecución DEMO, fills, rentabilidad, validación económica,
72 h/30 sesiones, apertura de WF/holdout, operación continua ni REAL.

## Próxima ejecución DEMO

**Antecedente de la planificación del 20 de septiembre, no una cita vigente.**

### Runtime promovido — 2026-09-20

Código validado `968bf0e7439b878e4b2d403d9b00bba9c7186223`: **1,219/1,219 pruebas**,
cero fallos/omisiones, 80.272% de líneas y 63.875% de ramas. El gate
terminó el 20 de septiembre a las 16:50:16 UTC con exit 0, PID 0, cgroup vacío
y fuente idéntica antes/después. No hubo intentos de red externa ni escrituras
Python bloqueadas. La auditoría no cubre todo I/O nativo ni agrega todos los
diagnósticos de procesos hijos; no se afirma auditoría global del filesystem.

El código quedó publicado por fast-forward y el runtime canónico fue promovido
con `run_id=20260920T162153Z-f4c62495`: **130 archivos** del paquete y sus
entradas RECORD son byte-equivalentes al SHA validado en ambos entornos
instalados. Imports, `pip check`, los tres launchers de usuario y el controlador
con Python instalado pasaron smokes desde un cwd ajeno, con HOME/XDG/TMP
aislados, sin `PYTHONPATH` del checkout ni credenciales. El paquete conserva
`0.1.0`; su procedencia se distingue por SHA y `run_id`. Python 3.12.14, SQLite
3.53.1, Protobuf 7.36.1, los 17 artefactos no-proyecto y el binario base son
idénticos al runtime anterior. Las 15 descargas fijadas desde PyPI fueron
autorizadas expresamente y no subieron código, datos ni secretos.

El rollback inmediato conserva código `b162e0f`, 128 archivos verificados,
imports y `pip check` tras su reubicación. Restaurar un runtime no restaura por
sí solo el código del controlador ni los bindings: deben conciliarse antes
de cualquier uso. La [promoción del runtime](runtime_lifecycle.md) exige
un intérprete externo al activo y al candidato, sin retirar el guard de uso.

Evidencia canónica de esta entrega:

- Gate `/home/winterboss/MTF/runtime/market-evidence/canary-version-promotion-20260920T155156Z/quality-gate.json`,
  SHA-256 `913eebefffdc72127503ba4458cbb0cb18e88d21155707ebe8391280c8faafc7`;
  terminalidad en `/home/winterboss/MTF/runtime/market-evidence/canary-version-promotion-20260920T155156Z/quality-terminal.json`.
- Instalación `/home/winterboss/MTF/runtime/market-evidence/canary-version-promotion-20260920T155156Z/installed-canary.json`,
  SHA-256 `c5bc43e88554b53322ebf89db4feb62cd6986da2c613e378ec2c09561fa78262`;
  manifest activo SHA-256 `c6638c1e9e2286d89b8054d7ced30ca4c95dbf5c81a045c5f07b3245d7004031`.
- Rollback `/home/winterboss/MTF/runtime/market-evidence/canary-version-promotion-20260920T155156Z/rollback-verification.json` y promoción
  `/home/winterboss/MTF/runtime/market-evidence/canary-version-promotion-20260920T155156Z/runtime-promotion-v2.json`.
- Publicación final `/home/winterboss/MTF/runtime/market-evidence/canary-version-promotion-20260920T155156Z/final-publication.json` y
  coherencia del plan/heartbeat `/home/winterboss/MTF/runtime/market-evidence/canary-version-promotion-20260920T155156Z/automation-updated.json`.

La configuración y aprobación privadas mantienen sus bytes originales. No se
repitió OAuth, no se tocó corpus/SQLite productiva y **no se enviaron órdenes**
durante esta preparación. La publicación técnica no abre estrategia, 72 horas,
forward, REAL ni fondeo.

La planificación original tuvo un heartbeat de una sola oportunidad,
`mtf-canaria-demo-autorizada-del-21-de-septiembre`, con preparación desde las
08:55 y órdenes previstas 09:00–09:20, America/Mexico_City. Esa oportunidad
terminó sin lanzamiento y el heartbeat fue eliminado. No se programó otro día.
El alias `/home/winterboss/.local/bin/mtf-lab-demo-canary` permanece instalado
y la configuración persistente sigue deshabilitada. Los límites de las nuevas
preparaciones autorizadas del mismo día constan en sus aprobaciones privadas,
sin extensión de un trial reservado ni reintento de órdenes ambiguas.

### Antecedente de instalación — 2026-09-19

El código `b162e0f1a52e272268bf3bd9ae1631e5cae688a0` pasó el gate completo:
1,125/1,125 pruebas, cero fallos/omisiones, 79.915% de líneas y 63.291% de ramas,
sin intentos de red ni escrituras fuera del aislamiento. La fuente quedó intacta.
Está publicado por fast-forward y el runtime canónico fue promovido con 128
archivos byte-equivalentes, `run_id=20260919T225409Z-4d1675aa` y rollback conservado.
La ayuda aislada, los imports instalados y el preflight de autenticación DEMO
pasaron; este último no reúne todavía BBO/ATR de mercado abierto ni habilita órdenes.


## Primer resultado histórico evaluable

El piloto `tp_fast_v1` usó 499,804 cotizaciones por escenario de la semana
2016-03-07→14, con costes de modelo explícitos, no cargos históricos observados.

| Escenario | Operaciones cerradas | Ganadoras/perdedoras netas | Neto modelado, USD |
|---|---:|---:|---:|
| Base | 6 | 2/4 | -0.64 |
| Adverso | 14 | 5/9 | -1.73 |
| Extremo | 14 | 4/10 | -2.20 |

Cada escenario conserva además un intent `UNKNOWN` por expiración de ventana de
entrada, sin precio ni PnL. Por ello el agregado no está aceptado aunque los
cierres individuales sean evaluables. No hubo recorte de trades retenidos, no se
suman escenarios y no se acredita ventaja. El V17 anual anterior tuvo cero fills
RiskExit evaluables; 2017–2019 sólo tienen QA descriptivo, no resultados de estrategia.

## Conservación

- Los 30 commits pendientes hasta `66506ad`, la documentación y el código
  previo `48ef12a687f74f7050147e3ea71d704b3ce8b358` se publicaron por
  fast-forward en el remoto privado `victor982721-lab/MTF`, sin force push ni
  GitHub Actions. El cierre documental posterior conserva el mismo árbol
  ejecutable y su verificación de publicación se registra fuera de Git.
- Código, contratos, pruebas y resúmenes redactados se versionan. `runtime/`,
  corpus, capturas, SQLite y credenciales permanecen fuera de Git.
- Se verificó un bundle Git local y una copia independiente de 102 archivos
  fuente/manifiestos históricos, con igualdad SHA-256 de cada miembro y fuentes
  originales sin cambios. Ambos están en `~/.local/share/mtf-lab/backups/`.
  Esta copia local protege de cambios accidentales, no de perder el disco.
- La evidencia regenerable pesada no se añade al repositorio ni se confunde con
  una copia remota del corpus. No se transfirieron datos privados a otro proveedor.

## Los seis puntos

| Punto | Resultado verificado | Condición pendiente |
|---|---|---|
| 1. Cierre de 2019 | 11 meses válidos; el ZIP de octubre original y la nueva descarga oficial son byte-idénticos y fallan orden temporal. | Fuente corregida/versionada de HistData o respuesta oficial que permita resolver la procedencia sin inventar datos. |
| 2. Desarrollo multianual | No se creó un año incompleto ni se ejecutó como si octubre fuera válido. | Manifiesto 2019 contiguo y válido antes de componer 2016–2019. Costos `UNKNOWN_COSTS`. |
| 3. Runtime | Código `0a6adfb5b9bf1915edb43399495ae10e08ae9707` validado, publicado e instalado; gate 1,315/1,315, 132 archivos byte-equivalentes, `run_id=20260921T231515Z-61e48a36` y rollback `37b30cd908588baa57d2d003ea0e488dee3d3991` comprobado. | El cierre documental conserva el mismo árbol ejecutable; cualquier cambio de código requiere su propio gate. |
| 4. App y contrato | App `Active`; VIEW original intacto y autorización separada `TRADING` observada sólo en la cuenta DEMO aprobada. | Condiciones de REAL/México y cargos realizados siguen separados; la especificación observada no prueba cargos ni costes históricos. |
| 5. Canaria técnica | Software instalado; las dos excepciones humanas están acotadas a esta canaria: `bid == ask` genuinamente observado y ancla de equity inicial del trial. La ejecución `20260921T233107Z` terminó `NO_EJECUTADA`/`OBSERVED_INPUTS_BLOCKED` por falta de ATR válido, con cero ciclos y órdenes. | El watcher terminó por `IDLE_TIMEOUT` de 30 s antes de ATR14; siguen siendo obligatorios continuidad/frescura, causalidad, ATR, calendario, cuenta plana, margen, riesgo, dos ciclos y conciliación observada. |
| 6. Shadow/forward | No iniciado. Las excepciones están acotadas a esta canaria y no habilitan shadow/forward. | Autorización operativa separada; 72 horas/30 sesiones no se sustituyen por fixtures ni se acelera el reloj. |

## Correcciones de diagnóstico

El lanzador `bin/mtf-lab-ctrader-query` utiliza el runtime privado canónico y una
referencia explícita al archivo privado de credenciales. Valida owner/modos,
token `accounts` vigente, selección durable DEMO y coincidencia con discovery;
el CLI vuelve a verificar la cuenta contra el servidor. No acepta `--activate`,
cambios de cuenta ni scopes, no inicia OAuth y no imprime secretos.
La ayuda no accede a los stores privados. Los tests aíslan HOME/XDG y usan
credenciales de fixture, nunca el token del usuario.
Los reportes y capturas requieren destinos nuevos y distintos en directorios
privados; se rechazan alias léxicos, symlinks y archivos existentes. La publicación
del reporte tampoco reemplaza un archivo creado concurrentemente.

Los accesos de usuario están instalados en `/home/winterboss/.local/bin/mtf-lab`
y `/home/winterboss/.local/bin/mtf-lab-ctrader-query`, con modo `0700`. El primero
usa el runtime canónico y el segundo incorpora sólo la referencia privada, nunca
el valor de las credenciales. Ambos pasaron `--help` desde otro directorio, con
HOME/XDG aislados y variables Python hostiles. No se modificó el perfil del shell.

Uso instalado, con un directorio privado nuevo para cada consulta:

```bash
/home/winterboss/.local/bin/mtf-lab --help
run_dir="$(mktemp -d "$HOME/.local/state/mtf-lab/research/ctrader-demo/query-XXXXXXXX")"
/home/winterboss/.local/bin/mtf-lab-ctrader-query --report "$run_dir/report.json"
```

La consulta instalada terminó con exit 0: `AUTHENTICATED`, `CONNECTED`,
`SCOPE_VIEW`, 1,940 símbolos y EUR/USD resuelto. Obtuvo 10,000 barras M1 en
20 páginas, pero `has_more=true` y `complete=false`: es historia parcial, no
prueba de frescura live. No repitió OAuth ni envió órdenes. Su receipt privado
queda enlazado desde el resumen versionado.

Alternativa desde el checkout, cuando la referencia privada ya está configurada:

```bash
MTF_LAB_CTRADER_CREDENTIALS_FILE=/ruta/privada/ctrader-app-ID.credentials.json \
  /home/winterboss/MTF/bin/mtf-lab-ctrader-query --report /ruta/privada/query.json
```

El lanzador admite sólo consulta. La referencia instalada y los secretos se
mantienen fuera de Git.

El primer gate de la canaria rechazó errores de tipos y una fixture que omitía
el nuevo valor predeterminado; no se promovió ese candidato. La corrección pasó
la suite íntegra en 1,567.154 segundos, sin reducir pruebas ni umbrales.
La evidencia del cierre del 19 de septiembre se conserva en
`runtime/market-evidence/canary-preparation-20260919T215251Z/quality-gate-v2.json`
(SHA-256 `fbcb473977577e5e76b73d8f4dc8c203184d7aba121b10350480644e072922eb`).
Los receipts de instalación, publicación, preflight y programación se enlazan
desde el resumen versionado; la ejecución contra el bróker sigue pendiente.

En la validación de la base anterior, el primer quality gate se interrumpió por
decisión del coordinador, no por una denegación del usuario. El segundo agotó
900 segundos; la suite previa ya había
requerido unos 1,430 segundos. La corrida con 1,800 segundos terminó sin fallos y
sin reducir pruebas o umbrales. Un timeout no demostró un defecto de código.

El gate final de `48ef12a` terminó el 19 de septiembre a las 18:59:50 UTC,
con fuente inalterada y 1,057 pruebas atendidas. La suite tomó 1,550.194 segundos
bajo el límite de 1,800. El runtime se promovió después de esa aceptación,
conservó rollback y comparó 126 archivos empaquetados byte a byte contra el código
validado. `runtime inspect` observó `ACTIVE_CANONICAL` y `process_scan=COMPLETE`.

`APP_CREDENTIALS_REQUIRED` describió procesos sin variables inyectadas. Las
credenciales locales sí permitieron autenticación DEMO. Del mismo modo,
`Submitted` era una observación histórica, sustituida ahora por `Active`.

La canaria del sábado recibió la última marca del viernes. No se puede prometer
un feed fresco con el mercado cerrado ni declarar una avería usando sólo esa
ventana. Se preservaron los rechazos y los timestamps; no se relajaron los guards.

La lectura histórica nueva cubre exactamente `2026-09-18T19:00:00Z` a
`2026-09-18T20:00:00Z`: 60 barras M1 nativas, `COMPLETE`/`CONTINUOUS` y cero gaps.
La consulta fuente aún tiene `has_more=true`; la selección bounded completa no
convierte todo el histórico en completo. No hay eventos bid/ask ni fills en esa
captura y no acredita frescura live.

Se corrigió la interpretación de zonas de festivos: cada `ProtoOAHoliday` usa
su propio `scheduleTimeZone`, independientemente del calendario semanal del
símbolo, de acuerdo con el
[contrato de mensajes cTrader](https://help.ctrader.com/open-api/model-messages/).
Las pruebas cubren cambio de fecha UTC/Nueva York, festivos parciales y recurrentes.
Las zonas ausentes/inválidas y las ventanas explícitas `0/0` siguen siendo
`UNKNOWN`. No hay base documental para convertir `0/0` en un día completo por
conveniencia; esa condición se agregó a la consulta no enviada al bróker.

## Especificaciones y costos efectivos

El catálogo observado declara volumen en centi-unidades: para EUR/USD,
`lotSize=10000000` representa 100,000 EUR y el mínimo/incremento de `100000`
representa 1,000 EUR, equivalentes a 0.01 del lote declarado. Son restricciones
de símbolo, no autorización ni recomendación de tamaño de operación.

La metadata de comisión, mínimos y swap tiene estado `SPEC_OBSERVED`, no
`CHARGED_OBSERVED`. Los campos de comisión legados no sustituyen las tasas
precisas del protocolo: el catálogo actual declara USD 3 por lote y lado,
con mínimo de comisión cero. El resumen anterior omitía esos campos precisos;
su ausencia en ese resumen no demostraba ausencia en el servidor.
No se observaron fills, conversiones ni cargos realizados;
`UNKNOWN_COSTS` permanece vigente y estas especificaciones actuales no se aplican
retroactivamente a 2016–2019.

## Próximos desbloqueos externos

- [Consulta técnica a HistData, no enviada](histdata_201910_issue.md): solicitar
  una versión corregida o explicación oficial. No ordenar, deduplicar, desplazar
  timestamps ni sustituir Tick Bid/Ask por M1 OHLC.
- [Consulta a Pepperstone, no enviada](pepperstone_ctrader_openapi_draft.md):
  confirmar las condiciones efectivas. La [oferta regional](https://pepperstone.com/es-la/formas-de-operar/cuentas-de-trading/)
  y los [costos publicados](https://pepperstone.com/en/trading/costs-and-fees)
  son referencias generales, no tarifas observadas de la cuenta.
- Conservar `accounts` de sólo lectura según el
  [contrato OAuth de cTrader](https://help.ctrader.com/open-api/account-authentication/).
  El token separado `TRADING` sólo cubre la canaria DEMO aprobada; no convertir
  su concesión en permiso para operación continua o REAL.

El histórico es el filtro principal de la estrategia. El holdout sigue cerrado,
no hay ventaja neta acreditada y el bloqueo de una fuente no autoriza cambiar
silenciosamente el experimento.

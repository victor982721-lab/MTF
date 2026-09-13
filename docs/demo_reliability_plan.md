# Evolución local hacia una DEMO fiable

## Alcance aprobado

Implementar H0–H6 localmente para EUR/USD, M1/M5/M15, Linux propio y pocas
estrategias. La investigación y las pruebas no son rentabilidad ni observación
del bróker. REAL/LIVE queda excluido. No se autorizaron OAuth nuevo, scopes,
órdenes DEMO, instalación operativa, publicación, cambios del host/red ni datos
de pago. El servicio se entrega deshabilitado. Se conservan los consentimientos
previos sin ampliarlos.

La base de implementación es `6d50a1abf57d3fb5dc31ae0876ec45cfd0b8d7b1`, observada
limpia el 2026-09-12, e incluye la entrega previa del observador y panel. Las
pruebas de esa base no validan los cambios de esta evolución. Compromiso:
`MTF-REL-001`; la conexión externa sigue separada en `MTF-DEM-001`.

## Matriz de entrega

| ID | Requisito | Pruebas y evidencia canónica | Estado local |
|---|---|---|---|
| H0 | Preservar base, decisiones y trabajo previo | Base Git indicada arriba; matriz y comprobante final de inputs | Base identificada |
| H1-E | Slippage una vez, fills cuantizados, economía versionada | [Economía v2](../tests/test_cfd_economics_v2.py), [persistencia v1/v2](../tests/test_cfd_economics_persistence.py) | Validación final pendiente |
| H1-C | Identidad antes del estado, disponibilidad por pierna, replay causal | [Causalidad](../tests/test_cfd_causality_v2.py), [fronteras heredadas](../tests/test_reliability_boundaries.py) | Validación final pendiente |
| H1-D | Codec fijado, presencia proto2 y dependencias | [Codec](../tests/test_generated_codec.py), [procedencia y hashes](../manifests/ctrader-protobuf-91.json), [SQLite](../manifests/sqlite-wal-review-20260913.json) | Codec integrado; SQLite conserva gate |
| H2 | Backtest CFD separado del binario: detector, bid/ask, ledger y equity | [Backtest y escenarios](../tests/test_cfd_backtest.py) | Validación final pendiente |
| H3-R | Trials completos, holdout 20%, walk-forward 40% + 4x10%, purga | [Registro](../tests/test_research.py), [validación semántica](../tests/test_research_validation.py), [métricas](../tests/test_research_statistics.py) | Validación final pendiente |
| H3-S | Baseline intacta, control M1 y Donchian20 M5 causal | [Estrategias](../tests/test_strategy_extensions.py), [snapshots públicos](../tests/test_indicator_snapshots.py) | Validación final pendiente |
| H4 | Permisos exactos, propiedad durable, cierre residual y exposición total | [Contratos de ejecución](../tests/test_ctrader_demo_contracts.py), [riesgo observado](../tests/test_ctrader_account_risk.py), [integración](../tests/test_execution_risk_integration.py) | Validación final pendiente |
| H5 | Supervisor, readiness, alertas locales y OAuth/journal seguros | [Supervisor](../tests/test_supervision.py), [fronteras](../tests/test_supervision_boundaries.py), [OAuth](../tests/test_ctrader_oauth_interop.py), [readiness](../tests/test_readiness.py) | Validación final pendiente |
| H6 | Medidas reales por ruta, caché segura y empaquetado | [Benchmark](../tests/test_benchmark_reliability.py), [consultas](../tests/test_executor_status_cache.py), [wheel](../tests/test_packaging_delivery.py), [soak](../tests/test_soak_reliability.py) | Validación final pendiente |

La implementación local está ensamblada. El cierre de validación integrada
queda sujeto al receipt final; los resultados focales no se suman para simular
una suite completa. Ninguna fila equivale a aprobación operativa externa.

Los costes económicos y límites de riesgo ausentes bloquean evaluación o
entradas; no se inventan ceros. La reducción de exposición sólo toca posiciones
atribuibles a MTF y reconciliadas; UNKNOWN no autoriza reenvío ni cierre ciego.
La UI sigue siendo de lectura. Un proceso vivo no prueba readiness y un equipo
apagado no puede emitir sus propias alertas locales.

## Dependencias y aceptación

H1 económico precede a H2/H3. Codec y contratos de ejecución preceden a H4/H5.
Los frentes tienen ownership disjunto; la raíz integra CLI, documentación y
gates. Las pruebas focales no sustituyen la suite integrada sobre entradas
congeladas. HOME/XDG/TMP/estado aislados y red externa bloqueada en pruebas.

La combinación candidata de codec es schemas Spotware release 91, commit
`017413087c1c23c1866bbf07ff24d56574047253`, protoc 36.1 y protobuf 7.36.1.
Se valida fuera del venv operativo; no se fuerza metadata incompatible ni se
recupera automáticamente el SDK antiguo. Los originales y licencias se
conservan con procedencia.
El [inventario local de componentes](../manifests/components-local-20260913.json)
identifica el runtime candidato y las herramientas de desarrollo, con versiones,
licencias declaradas y hashes de metadata instalada. Estos hashes no sustituyen
los hashes de wheels originales ni constituyen una auditoría remota de advisories.

La revisión del runtime enlazado identifica SQLite 3.46.1 del paquete Ubuntu
`libsqlite3-0 3.46.1-9ubuntu0.2`. Su patchset oficial inspeccionado no modifica
`src/wal.c` ni contiene referencias WAL-reset. La
[recepción local](../manifests/sqlite-wal-review-20260913.json) conserva la
identidad de biblioteca y patchset. No se actualizaron paquetes del host.
La [referencia upstream](https://sqlite.org/wal.html#the_wal_reset_bug)
documenta la reparación en 3.51.3 y backports 3.44.6/3.50.7; esto no demuestra
corrupción en bases del usuario. Sin reparación acreditada no se declara apta
la operación externa continua con WAL.

## Interfaces locales nuevas

`research run --fixture --manifest <archivo-nuevo.json>` ejecuta investigación
CFD sintética; `research compare <manifiestos...>` compara sin sumar horizontes
alternativos y `research validate <manifiesto>` comprueba su contrato. La
entrada real requiere una captura local y especificación explícitas; no se
adquieren datos. La estrategia binaria y sus comandos anteriores permanecen
separados.

`--execution-model full_fill|ioc_partial|rejected` declara escenarios de
ejecución sintética, no fills observados. `ioc_partial --fill-fraction 0.5`
simula la ejecución de esa fracción y cancela el residual; cantidades solicitada,
ejecutada, cancelada y rechazada se conservan separadas. Los costes se calculan
sobre la cantidad efectiva; la comisión fija no se prorratea indebidamente.
Los escenarios y parámetros quedan registrados antes de producir resultados.

`ctrader supervise --mode observe --fixture --state-dir <directorio-privado>
--db <base-aislada> --duration 30` ejercita el supervisor sin bróker. El modo
shadow tampoco envía órdenes. El modo demo necesita activación explícita y
evidencia externa vigente; un fixture nunca satisface ese gate. `--continuous`
retira solamente los límites de tiempo/eventos, no los controles de seguridad.
No se instala ni habilita el ejemplo systemd como parte de esta entrega.

`ui --db <base-local> --supervisor-state <estado-privado.json>` añade el panel
de capacidad efectiva. `/api/health` sólo comprueba HTTP; `/api/readiness`
verifica esquema, modo, PID + boot ID + inicio del proceso y vigencia del
snapshot. Un JSON viejo, un PID reutilizado o un proceso terminado no son
readiness. No se divulgan tokens ni valores arbitrarios del journal.

La economía `cfd-economics-v2` calcula bruto/pips desde fills cuantizados;
slippage queda informativo, y el neto resta únicamente los costes adicionales
conocidos. Los snapshots sin versión económica conservan semántica v1 explícita;
los resultados antiguos no se recalculan como si fueran v2.

## Riesgo, mantenimiento y evidencia

El gestor reconstruye intenciones, órdenes y posiciones desde journal y
respuestas correlacionadas. Las entradas exigen límites de pérdida/drawdown,
margen, cantidad/exposición y duración máxima declarados, grid de volumen
observado y protección aceptada por el servidor. El precio de un stop no es
garantía de ejecución. Posiciones extranjeras o no atribuibles no se cierran.
Un cierre parcial o incierto no habilita otro envío ciego.

La observación de cuenta usa el mismo cliente autenticado, `moneyDigits`,
posiciones, PnL y deals diarios completos. Fees/conversión ausentes quedan
desconocidos. El drawdown operativo es sobre equity observada **desde el armado
explícito**, no un máximo histórico reconstruido: su high-water se preserva
fuera del checkout. Margen usado cero observado no se representa con un número
infinito inventado. Reautenticación y reconexión no amplían scopes; un token
vencido/no verificable bloquea y no provoca OAuth nuevo automáticamente.

La UI sigue siendo sólo lectura. Las proyecciones repetidas usan un snapshot
de posiciones completo, de la generación vigente y dentro de su edad máxima;
aperturas/cierres y ciclos de gestión vuelven a reconciliar. Calendarios usan
los intervalos del catálogo desde domingo 00:00 en su zona IANA, sin asumir
mercado abierto cuando falta evidencia. Alertas locales: journal y,
explícitamente con `--dbus-alerts`, `notify-send`; nunca canales remotos.

OAuth usa transacciones one-use con bloqueo y marcador durable sin secretos.
Un resultado incierto impide reutilizar el token anterior; preparar una nueva
autorización no limpia ese bloqueo hasta que el nuevo token verificado se
confirma. Tokens, intentos, perfiles, scopes y nombres de archivo se vinculan;
se rechazan redirects, parámetros extra, aliases y hardlinks inseguros.

`tools/benchmark_reliability.py` compara rutas reales con timing sin
`tracemalloc` y un pase de memoria separado, sobre entradas idénticas y con
equivalencia funcional. No extrapola latencia ni rentabilidad del bróker.
`tools/soak_reliability.py --dry-run` muestra la preparación sin iniciar una
unidad; `--smoke` es siempre insuficiente para las 72 horas. El runner real
exige tiempo de pared/monotónico, cercas de código/entradas, progreso útil y
replay/restauración del modelo; downtime y controles de prueba no acreditan
esa ventana.

## Gates que no se simulan

- 72 horas reales de prueba local de resistencia: no acreditadas por un smoke.
- Aprobación/credenciales/OAuth y catálogo cTrader: fuentes externas separadas.
- Observación, shadow, comisionamiento y 30 sesiones DEMO: no ejecutados ni
  autorizados por la implementación local.
- Publicación, instalación y activación del servicio: no realizadas.

El cierre local distingue comportamiento implementado y probado de operación
externa pendiente. No se exige encontrar una estrategia rentable; rechazarla o
declarar evidencia insuficiente es un resultado válido.

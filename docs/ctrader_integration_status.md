# Estado de integración cTrader / Forex-CFD

La consolidación histórica del 2026-09-11 publicó la implementación
offline anterior; no activó cuentas, no realizó OAuth y no envió operaciones. El [informe
de ingeniería](../reports/engineering/latest/engineering_consolidation.md), sus
[resultados](../reports/engineering/latest/engineering_results.json) y el
[manifest de tooling](../reports/engineering/latest/engineering_tooling.json)
son la evidencia reproducible del gate final. La [matriz A/B/C de fronteras](activation_boundaries.md) separa hechos locales de evidencia que sólo puede
producir un servidor autenticado.

## Estado actual

### Evolución H0–H6 local — 2026-09-13

La [matriz de implementación y aceptación](demo_reliability_plan.md) describe
los cambios posteriores: economía CFD v2, investigación causal, codec generado,
gestión de riesgo y supervisor deshabilitado. Los receipts históricos de abajo
no validan estos bytes nuevos. No se publicó ni instaló una release operativa
como parte de esta evolución y no se volvió a consultar el portal.

El codec candidato usa Protobuf 7.36.1 y schemas Spotware 91, sin SDK antiguo.
La presencia del SDK anterior en `.venv` no prueba utilidad: Protobuf 3.20.1
queda rechazado. La reparación SQLite WAL-reset, la resistencia real de 72
horas y las 30 sesiones DEMO son gates separados de la implementación local.

### Preparación externa DEMO — 2026-09-12

Se verificó en el navegador una sesión cTrader ID autenticada y la cuenta
Pepperstone DEMO vinculada. Esta observación de la interfaz no equivale a una
sesión Open API de MTF. La aplicación **MTF Lab** se registró y el portal confirma
**`Submitted`** (en revisión), todavía no **`Approved`**. Se verificó que el contacto
autorizado quedó guardado; no se duplican aquí sus datos personales. Víctor autorizó los términos de Open API y la comunicación del contacto
a Spotware y a los brokers para soporte; no debe repetirse ese consentimiento
mientras no cambie su alcance.

El perfil inicial es `config/ctrader_query.toml`, exclusivamente **DEMO** y
scope **`accounts`**. El callback configurado es
`http://127.0.0.1:8767/oauth/callback`. El doctor local observa el SDK, pero informa
`APP_CREDENTIALS_REQUIRED`; el portal ya ofrece credenciales de aplicación, pero
no se han exportado a almacenamiento local ni realizado OAuth para MTF. Cualquier cuenta REAL/LIVE queda fuera de esta tarea;
no se habilitan órdenes ni se mueven fondos. La aceptación de Open API no se
presenta como validación contractual general de Pepperstone.

#### Preparación local verificada

La integración de consulta de lectura e importación del histórico nativo quedó
validada con **422/422 pruebas**, sin fallos ni omisiones, Ruff/formato, mypy y
Pyright en verde. La suite se ejecutó con HOME/XDG/TMP/estado aislados y cero
intentos de red externa; los imports/smokes no activaron el SDK por defecto.
El [receipt compacto](../reports/quality/ctrader-demo-preparation-20260912.json)
conserva la identidad de los inputs y las métricas del gate final. En el cierre inicial, los cambios
quedaron locales y todavía sin commit, publicación ni instalación de release;
ese receipt describe la preparación anterior a su consolidación en Git.

El intercambio y refresh mantienen el token en memoria hasta verificar el eco
del servidor, el permiso efectivo, los endpoints y la generación de conexión.
La consulta revalida esa evidencia fresca: esta fase rechaza permisos de trading
no pedidos e inventarios con cuentas REAL/LIVE. El discovery público permanece
sin tokens; el acceso al eco sensible es explícito e interno a la verificación.

El histórico se exporta con metadatos mínimos de cuenta/entorno/especificación,
archivos privados y publicación sin reemplazo. La reproducción exige
`price_base=native` y `order=market_time_corrected`, conserva la recepción original
y no fabrica bid/ask ni fills. Las capturas parciales, vacías o incompatibles no
se presentan como análisis completo. La ruta y los comandos están en el README.
Esa preparación de 422 pruebas no incluía aún el runner continuo. **La conexión
real de MTF por Open API sigue pendiente de aprobación y OAuth**.

### Runner y observabilidad offline — 2026-09-12

La base anterior se consolidó en el commit local `9d066c6`, sin publicarla ni
instalar una release. El alcance posterior añade `ctrader watch`, composición
de lectura del proveedor existente con runtime MTF, límites, checkpoint
atómico, reanudación exacta y parada cooperativa. La UI separa procedencia,
frescura y estado de feed, y hace visibles los fallos de refresco. El
[manual de uso](ctrader_watch.md) documenta fixtures y límites de recuperación.

Este alcance no usa OAuth, cuentas ni conexión real al bróker. La ruta externa
se conserva para activación posterior; el compromiso de conexión DEMO no se
cierra con una validación sintética.

El commit local de implementación `f6bcb1e` pasó el gate global con **457/457
pruebas**, cero fallos/omisiones/intentos de red externa, Ruff/formato, mypy,
Pyright y auditoría arquitectónica en verde. Los fuentes permanecieron
idénticos durante la corrida. El [receipt de esta ampliación](../reports/quality/ctrader-watch-observability-20260912.json)
incluye identidad, cobertura, revisión y QA visual: pausa/continuidad/warmup
correctos, error visible, datos previos conservados y recuperación automática.
El cierre posterior sólo agrega documentación/evidencia; no publica ni instala
una release.


### IMPLEMENTADO Y COMPROBADO OFFLINE

- La fachada `mtf_lab.data.ctrader` separa configuración, Protobuf/codec,
  transporte, sesión, cuentas, mercado y fixtures. Importar el dominio o usar
  `--help` no carga un transporte externo.
- El lector por generación, framing, correlación, heartbeat, backpressure,
  reconexión y causas de cierre/EOF se prueban con transporte controlado.
- `CaptureEnvelope` v1 conserva `event_time`, `received_at`, `available_at`,
  secuencia global y generación. `CTraderPaperSession` mantiene detector y CFD
  durante ingesta, avance, checkpoint, restauración y cierre, con replay causal
  e idempotente.
- La calidad bid/ask, frescura, timestamps, cruces, discontinuidades y
  cantidades Decimal llegan al fill. Trendbars usan base `native`; no se
  reinterpretan como operaciones negociadas.
- SQLite schema v4 separa `cfd_trades` del producto binario y conserva datos v3
  como evidencia legacy. La recuperación y los límites de retención son
  explícitos; no se descartan estados activos silenciosamente.
- La composición local cubre RuntimeCoordinator, señal, gates de riesgo/safety,
  intent durable, construcción de `ProtoOANewOrderReq`, eventos de ejecución,
  reconciliación, cierre y persistencia mediante Protobuf instalado y gateway
  falso/local. `ctrader fixture` y `cfd-paper` son fixtures sintéticos.
- La instalación, los imports, los defaults TOML empaquetados, los lanzadores,
  el fixture CFD, la UI de sólo lectura y el gate JavaScript se validan sin
  credenciales ni red de bróker.

Las pruebas y fixtures no demuestran rentabilidad, ventaja estadística,
condiciones de ejecución ni tarifas. Las cotizaciones sintéticas no son
redistribución ni observación de mercado real.

### IMPLEMENTADO, PENDIENTE DE VALIDACIÓN EXTERNA

El código y los contratos para autenticación de aplicación, descubrimiento de
cuentas, selección DEMO explícita, autorización de cuenta, catálogo/símbolo,
market data y transporte DEMO están implementados y probados localmente. La
prueba local no sustituye una respuesta de cTrader: quedan sin observar los
permisos y entorno de la cuenta, símbolos habilitados, escalas, límites de
volumen, horarios, spreads, comisiones, conversiones, fills, posiciones,
historia y respuestas de aceptación/rechazo. Un timeout ambiguo permanece
`UNKNOWN` y exige reconciliación; no autoriza reintentar una orden.

La ejecución externa permanece deshabilitada y **REAL/LIVE se rechaza
fail-closed**. Ningún fixture prueba que una orden haya llegado a un servidor.

### ETAPAS EXTERNAS PENDIENTES

Los únicos bloqueos externos de esta fase son:

1. Obtener la aprobación de Spotware para la aplicación ya registrada.
2. Disponer fuera del repositorio de `CTRADER_CLIENT_ID` y
   `CTRADER_CLIENT_SECRET`.
3. Completar OAuth y consentir el alcance requerido.
4. Descubrir las cuentas realmente autorizadas y seleccionar explícitamente una
   cuenta **DEMO**.
5. Verificar catálogo, condiciones del bróker y la ruta de lectura autorizada
   contra el servidor **DEMO**; cualquier ejecución exige otro alcance expreso.
6. Confirmar las condiciones contractuales de Pepperstone aplicables a México,
   almacenamiento/redistribución de datos y costes; no están verificadas.

Estos puntos son hechos, decisiones o autorizaciones externas, no tareas de
programación pendientes. No se solicitan secretos ni se inicia autenticación
para reproducir el gate offline.

## Referencias de protocolo

- [Open API cTrader](https://help.ctrader.com/open-api/), [endpoints](https://help.ctrader.com/open-api/proxies-endpoints/), [conexión](https://help.ctrader.com/open-api/connection/), [autenticación](https://help.ctrader.com/open-api/account-authentication/) y [datos de símbolos](https://help.ctrader.com/open-api/symbol-data/).
- [Contrato Protobuf de presencia de campos](https://protobuf.dev/programming-guides/field_presence/).
- [Mensajes cTrader](https://help.ctrader.com/open-api/messages/) y [modelos](https://help.ctrader.com/open-api/model-messages/).
- [Contrato `Decimal` de Python](https://docs.python.org/3/library/decimal.html).

El [borrador de consulta Pepperstone](pepperstone_ctrader_openapi_draft.md) es
sólo un borrador local y no acredita validación contractual ni fue enviado por
esta entrega.

## Cierre de implementación local H0–H6 — 2026-09-13

Código validado: `c1246cc173725e9dc22ae813e3f2f9b6f96bf5e6`, **653/653 pruebas**,
codec generado operativo en el runtime candidato, wheel reproducible y smoke
instalado aislado. El [comprobante local](../reports/quality/mtf-demo-reliability-20260913.json)
y la [matriz H0–H6](demo_reliability_plan.md) detallan alcance y evidencia.
Esto cierra `MTF-REL-001`, no la operación desatendida: `MTF-VAL-001` conserva
72 horas reales, resolución/verificación SQLite y aceptación de piloto; las
30 sesiones DEMO no se realizaron. No hubo nueva lectura del portal, OAuth,
ampliación de scopes, conexión API/órdenes, push ni instalación operativa.
`MTF-DEM-001` mantiene su estado externo por separado; no se reinterpreta la
observación histórica de la aplicación como aprobación vigente.

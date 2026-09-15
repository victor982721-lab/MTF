# MTF — campaña de evidencia histórica y DEMO

## Autorización y estado

Plan aprobado expresamente para implementación por Víctor el 2026-09-13.
Base de código: `bea3303a6e497d7a70cfd607e73037f4182e211e`, árbol limpio al inicio.
El objetivo es evidencia de ventaja neta, no declarar rentabilidad por aprobar QA.
**Estado: integración y primeros pilotos descriptivos reales; no hay resultado económico aprobado.**
El [estado del piloto](market_pilot_status.md) conserva mediciones y límites vigentes.

El histórico es el filtro principal. La operación DEMO y los costes observados
son gates distintos. No se inician OAuth nuevo, ampliaciones de scopes, órdenes,
servicios operativos o cambios de energía/red/privacidad por inferencia. Se
conservan consentimientos válidos existentes; las fases externas requieren su
cuenta, política, ventana y límites inequívocos. REAL y fondos reales excluidos.
No hay compras, cloud, suscripciones nuevas ni GitHub Actions.

## Decisiones y campaña congelable

- EUR/USD inicial, dos familias y seis candidatos; no ampliar instrumentos ni
  espacio de búsqueda para rescatar un resultado desfavorable.
- Perfiles intradía y de varios días; sin exposición en cierres semanales o
  festivos prolongados conocidos. Estabilidad y menor riesgo antes que retorno.
- Riesgo previsto <=0.25% de equity por operación, pérdida diaria 1% y drawdown
  5%, con parada/revisión humana. No son garantías de pérdida máxima.
- Equity de investigación USD 10,000 virtual, no capital personal del usuario.
- Mantener baseline y resultados legacy; cambios de perfiles/ejecución tienen
  identidades/versiones nuevas, sin reinterpretación histórica.

| ID | Familia / temporalidades | Perfil |
|---|---|---|
| tp_fast_v1 | Trend pullback M15/M5/M1 | intradía |
| tp_intraday_slow_v1 | Trend pullback H4/H1/M15 | intradía |
| tp_multiday_v1 | Trend pullback D1/H4/H1 | varios días |
| dc_m5_v1 | Donchian 20 barras previas M5 | intradía |
| dc_m15_v1 | Donchian 20 barras previas M15 | intradía |
| dc_h1_v1 | Donchian 20 barras previas H1 | varios días |

EMA 20/50, RSI 14, ATR 14 y umbrales originales de entrada; TTL de preparación
en barras (tres barras de preparación). Mid para análisis, Bid/Ask para ejecución.
Control M1 y horizontes 60/180/300 segundos son diagnósticos, no cartera sumable.
No scores ajustables, búsqueda genética, familias nuevas ni autoajuste inicial.

| Fechas UTC, extremos [inicio, fin) | Uso |
|---|---|
| 2015-01-01 a 2016-01-01 | calentamiento, sin retorno de evaluación |
| 2016-01-01 a 2020-01-01 | desarrollo |
| 2020-01-01 a 2021-01-01 | walk-forward 1, prefijo anterior |
| 2021-01-01 a 2022-01-01 | walk-forward 2 |
| 2022-01-01 a 2023-01-01 | walk-forward 3 |
| 2023-01-01 a 2024-01-01 | walk-forward 4 |
| 2024-01-01 a 2026-01-01 | holdout final reservado |

No evaluar 2024–2025 hasta congelar protocolo, código/runtime, datos, costes y
calendario y registrar el acceso confirmatorio. Si se usa para modificar reglas,
queda contaminado y hace falta una reserva no usada para esa selección.

## Datos y escala

Fuente primaria HistData Generic ASCII Tick Bid/Ask, EST fijo UTC-5 sin DST.
Dukascopy sirve como contraste independiente de periodos solapados, no como fills
del bróker. Acceso web gratuito; no FTP/SFTP pagado, cuentas nuevas ni evasión de
controles del proveedor. Se documentan condiciones antes de la descarga.

Destino aprobado: `/home/winterboss/.local/share/mtf-lab/market-data`.
Raw comprimido e inalterado, SHA-256, proveedor/fecha/URI/formato/precisión y
localizador de fila. Versiones nuevas ante colisiones, sin sobrescribir originales.
No corpus, capturas, credenciales ni ticks masivos en Git.

Escalar por gates: semana 2016-03-07 a 2016-03-14 UTC -> marzo completo -> abril
adquirido/validado -> compuesto marzo–abril -> año de desarrollo 2016 -> años de
desarrollo 2017–2019 -> campaña completa. Si el contenedor mínimo es mensual,
conservar los ZIPs adquiridos y distinguir bytes adquiridos de las ventanas
realmente validadas. Antes del año completo, medir un canario de capacidad de
1,048,576 cotizaciones; el procesamiento usa bloques de 2,048 y el snapshot
durable por defecto cada 32 bloques (65,536 cotizaciones), política que forma
parte de la identidad del checkpoint.
Límite inicial 40 GiB y reserva >=20% del disco: parar expansión, no borrar fuentes
ni contratar almacenamiento para sortear el límite.

La comprobación conserva el límite de 40 GiB por archivo/archive y ahora añade
un guard agregado de todos los raws, con lock común, proyección y reserva de
filesystem. Su implementación no habilita por sí sola una adquisición: cada
proveedor debe conservar su receipt y validación de contenido. El modelo
`DatasetManifest` vigente sólo admite meses HistData de desarrollo 2016–2019;
las ventanas WF 2020–2023 usan ya un contrato separado, pero no hay datos WF
adquiridos ni corridas WF iniciadas y no se pueden relabelar como holdout
2024–2025. Los contratos y límites implementados están en
[presupuesto agregado](storage_budget_contract.md) y
[walk-forward](walk_forward_contract.md).

Ticks con mismo timestamp se conservan; identidad por fuente/hash/offset, nunca
sólo timestamp. UTC, received_at no observado permanece desconocido; disponibilidad
histórica se etiqueta como modelada, no como recepción observada. No se fabrican
volumen, profundidad, velas de huecos ni secuencia intrabar desde OHLC.

El camino de investigación es incremental, bloques iniciales 2048, un trabajador
y RSS objetivo <=1.5 GiB. Indicadores sólo al cerrar barras, estado mutable de
estrategias separado y reutilización de cálculos válidos. Resultados grandes se
persisten con cursores/artefactos; no se materializa todo el histórico en listas.
El contrato de checkpoint parcial distingue `CHECKPOINTED` de `COMPLETED` y
permite reanudar una misma corrida sólo tras validar identidad, offsets y
artefactos; un prefijo terminal no se reutiliza como si fuera parcial.

## Economía, riesgo y contratos

Contratos nuevos mínimos: DatasetManifest, ResearchProtocol, RiskExitPolicy,
GlobalTrialRegistry y ResearchEvidenceReport. Se mantienen interfaces legacy.

RiskExitPolicy compartida backtest/DEMO: SL de 1.5 ATR del disparador, TP de 3 ATR; intradía máximo
5 barras del disparador y corte documentado menos 30 minutos; multidía 72 horas y cierre
semanal/festivo conocido menos 60 minutos. Una posición/intent, floor de volumen al grid,
sin martingala ni promediado. Si especificación/coste/calendario no permiten
acreditar elegibilidad, no operar; investigación parcial no se declara aprobada.
R inicial inmutable. Equity de inicio del día persistida incluye flotante y
cashflows verificables; reinicio no restaura presupuesto gastado. High-water
persiste. Cierres sólo exposición propia conocida; UNKNOWN no autoriza reenvío.

Latencia modelada base de 5 s y slippage adverso de 0.1 pip/fill; escenario adverso de 10 s/0.2 pip y
costes/spread 1.5×; extremo 2× sólo diagnóstico. Son supuestos, no mediciones.
Stops de servidor se evalúan por tick elegible, sin latencia de cliente ficticia;
gaps no se ejecutan automáticamente al precio solicitado. Costes desconocidos
bloquean conclusión favorable; no se convierten a cero. Gross desde fills ya
incorpora spread/slippage: no descontarlos otra vez.

## Evaluación y aceptación

Registrar antes de resultados todos los intentos y accesos, incluidos fallos,
reintentos, descartados y QA sintético identificado. DSR auxiliar, alcance/registro
explícitos; nunca permiso de operación ni sustituto de muestras independientes.

Cada candidato contra no operar: media neta bajo estrés >=0.05 R/episodio, LCB unilateral
>0 en base y adverso, DD <=5%, quitar cinco mejores no convierte neto en negativo;
ninguna aprobación con costes/marcas/cierres/R incompletos.

Por candidato en holdout: >=120 episodios liquidados evaluables, >=100 sesiones,
>=20 bloques completos de dos semanas; >=20 episodios en cada ventana anual WF.
No exigir todas ventanas positivas. Bootstrap de 100,000 remuestreos, semilla 20260913, bloques de dos semanas,
sensibilidad de una/cuatro semanas, alineación entre candidatos; α = 0.05/12 por contraste. Intervalos
nominales y condicionales, no probabilidad garantizada de rentabilidad. Dependencia
inestable => evidencia insuficiente.

Ranking de quienes pasan: DD adverso, concentración/sensibilidad, margen estadístico,
ID. Un candidato operativo; los demás sólo referencia/shadow. No sumar alternativas.

## Entregas y gates

| Fase | Entrega | Gate para avanzar |
|---|---|---|
| A | protocolo, adaptador y semana/mes reales | calidad/procedencia/capacidad verificadas |
| B | streaming, política/economía e informe de año | ledger y causalidad reconstruibles |
| C | desarrollo y WF multiaño, contraste fuente | candidatos y motivos; sin ampliar búsqueda |
| D | campaña holdout única | condiciones/runtime/costes congelados y registro |
| E | artefacto/runtime/72 horas/observe/shadow | candidato histórico, QA y scopes aplicables |
| F | canary DEMO acotada | autorización cuenta/símbolo/volumen/ventana/límites |
| G | forward 30/100/200 sesiones | muestras, riesgo, costes y decisión humana |

Canary: BUY/cierre y SELL/cierre; riesgo previsto <=0.05% por ciclo, menor volumen
compatible, una posición, <=6 mensajes de mutación incluidas cancelaciones
condicionales. La autorización cubre el protocolo completo, no cada mensaje.
No provocar fallos peligrosos; casos no observados se conservan como tales.

Forward: mínimo operativo de 30 sesiones, revisión a 100, evaluación a 200 con >=200 episodios
y bloques requeridos. Muestra faltante => observar sin forzar trades y preregistrar
el contraste adicional; no repetir hasta obtener un resultado favorable. Se conservan pérdidas e
incidencias de sesiones incompletas. Monitor unilateral de deterioro de retornos
diarios, referencia fija y umbral calibrado antes del forward; pausa y revisión,
no autoajuste. Ni 72 horas ni 30/100/200 sesiones se simulan con reloj acelerado.

## Tecnología y validación

Mantener Linux/Python. Runtime privado candidato Python 3.12.14/SQLite 3.53.1,
independiente del caché de Codex, sólo después de comprobar procedencia/licencias,
rutas, biblioteca efectivamente cargada, imports/codec y suite. `.venv` operativo y host no
se sustituyen por inferencia. Runtime inseguro de rollback no permite operar.

PostgreSQL o aceleración en Rust sólo si las mediciones demuestran necesidad después de
streaming/lotes/reutilización; no son prerrequisitos ni motivos suficientes para un rediseño. Paquetes
oficiales y cambios necesarios dentro de la autorización, sin interrumpir trabajo ajeno,
sin energía/red/privacidad implícitas, con ventana para reinicios y reversión.

QA: entrada/instrumento/escala/timestamps/duplicados/gaps; no lookahead; SL/TP/gaps/
financiación/parciales; Decimal/costes una vez/R fijo; rollover/reinicio/cashflows;
replay completo/bloques/resume; rendimiento equivalente; Ruff/mypy/Pyright globales y
coverage sin reducir alcance, codec real, wheel reproducible y smoke aislado;
QA visual completo del HTML/JSON con gráficos autocontenidos sin CDN. UI de corridas
cercadas consume snapshots publicados, no abre SQLite vigilada como sonda.

Primer informe: spreads/ATR/sesiones/calidad/regímenes/embudo, señales/episodios/
exposición, equity realizada/MTM/DD/recuperación, MFE/MAE y frontera de costes. Los
resultados pueden ser favorables, descartados o insuficientes. Ninguno valida REAL.

# Validación histórica de horario y contraste EUR/USD

**Fecha de revisión:** 2026-09-13. **Estado:** investigación documental y
validación de un artefacto local; no es una aprobación de datos, broker ni
rentabilidad.

## Alcance y evidencia local

Se leyeron, sin modificarlos, `structure.json` y `receipt.json` del piloto

`/home/winterboss/.local/state/mtf-lab/research/market-structure/201603-week-a39c3705472b/`.

El artefacto identifica `REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS`, no fills del
broker objetivo. Registra 499,804 cotizaciones, cobertura
`2016-03-07T00:00:00.490Z`–`2016-03-13T23:59:57.907Z`, SHA-256 bruto
`8357ef823ac27d9c53da9acff6f79b6e8fc058cea9fdf8f32be08bf06dbe3e1b`,
`availability=HISTORICAL_EVENT_TIME_MODELED_NOT_RECEIPT` y
`provider_comparison=NOT_ASSESSED_SECOND_PROVIDER_NOT_ACQUIRED`. El recibo es
`GENERATED_DESCRIPTIVE_ONLY`, con listas de intentos de red y escritura vacías.

## Hallazgos

1. **La premisa “HistData sólo ofrece M1 OHLC mid sin spread” no es correcta.**
   La especificación oficial describe un archivo Generic ASCII de *ticks* con
   `timestamp,Bid Quote,Ask Quote,Volume`, milisegundos y zona `EST` fija sin
   ajuste de horario de verano. Su M1 documentado sí es OHLC de **Bid** (no
   mid). Por tanto, el adaptador debe distinguir `M1 bid-only` de `tick
   bid/ask`; no debe fabricar un spread a partir de M1.

2. **El fin de semana observado es compatible con EST fijo, no prueba el
   horario real del mercado.** El último tick antes del cierre es
   `2016-03-11T21:59:56.467Z` y el primero del domingo es
   `2016-03-13T22:00:34.657Z`: intervalo `172838.190 s` (`48 h 00 m
   38.190 s`). Convertidos con el convenio HistData fijo UTC−05:00 son
   viernes 16:59:56.467 y domingo 17:00:34.657. Así, el primer tick del domingo
   cae 34.657 s después de las 17:00 **en la representación de la fuente**.

3. **Hay una diferencia con el calendario modelado y debe permanecer como
   incertidumbre.** El reporte usa `America/New_York`, viernes/domingo 17:00,
   con base documental aproximada de OANDA. El 13 de marzo de 2016 ese huso ya
   estaba en EDT, por lo que el domingo 17:00 modelado corresponde a 21:00Z;
   queda un tramo sin cotización de `1 h 00 m 34.657 s` hasta el primer tick.
   Se clasifica como `UNKNOWN` (convención temporal de la fuente y/o hueco de
   cobertura), no como cierre programado observado. No se deben desplazar los
   timestamps brutos ni convertir el EST fijo de HistData a DST para cerrar la
   discrepancia.

4. **Dukascopy es un contraste documental plausible, pero no es el broker
   objetivo.** Su exportador oficial declara CSV desde tick a mensual y acceso
   gratuito por la herramienta Historical Data Export; la misma página describe
   por separado el Historical Data Manager de JForex con inicio de sesión demo o
   live. La documentación de History ticks usa `Instrument.EURUSD`, objetos
   `ITick` con `getBid`/`getAsk` y rangos inclusivos. Esto acredita formato/API y
   una fuente independiente de precios, no cobertura exacta de la semana
   solicitada,
   permisos, liquidez, fills ni condiciones del broker objetivo. En esta
   revisión no se creó cuenta, no hubo OAuth, pago, descarga de la fuente de
   contraste ni carga remota.

5. **Comprimir ticks a barras no preserva la secuencia intrabar para ejecución.**
   La documentación oficial de JForex separa ticks históricos reales de velas
   con ticks interpolados desde OHLC; “process all ticks” reproduce cada tick
   registrado. OHLC no conserva el orden de high/low, cruces repetidos, spread ni
   el primer tick que dispara un stop. Usar barras para señales puede ser
   válido; stops, límites y fills simulados deben reproducirse sobre ticks
   bid/ask, con reglas de gap y procedencia separadas. Un fill del tester de
   Dukascopy tampoco es un fill del broker objetivo.

6. **Propuesta de contraste sin retocar la investigación.** Después del gate de
   adquisición correspondiente, obtener de Dukascopy sólo la ventana UTC
   `[2016-03-07T00:00:00Z, 2016-03-14T00:00:00Z)` y conservarla como fuente,
   hash y manifest independientes. Comparar, sin cambiar umbrales, costes ni
   PnL: extremos y conteos, normalización temporal, primera/última cotización de
   viernes/domingo, distribución de huecos, bid/ask y spread. Reportar cada
   proveedor por separado; el resultado sólo puede hablar de cobertura,
   procedencia y sensibilidad entre fuentes, nunca de fills o rentabilidad.

## Fuentes oficiales consultadas

- [HistData — especificación detallada de archivos](https://www.histdata.com/f-a-q/data-files-detailed-specification/)
- [OANDA — Hours of Operation (PDF)](https://www.oanda.com/assets/documents/402/Hours_of_Operation_bT4prdz.pdf)
- [Dukascopy — Forex Historical Data Export](https://www.dukascopy.com/swiss/english/marketwatch/historical/)
- [Dukascopy/JForex — History ticks](https://www.dukascopy.com/wiki/en/development/strategy-api/historical-data/history-ticks/)
- [Dukascopy/JForex — Strategy Tester](https://www.dukascopy.com/wiki/en/manuals/jforex4-desktop/strategy-tester/)

## No verificado

No se verificó documentalmente que Dukascopy conserve exactamente EUR/USD para
`2016-03-07`–`2016-03-14`, ni su zona horaria/precisión efectiva para esa
exportación concreta, tamaño comprimido, límites vigentes o términos de
redistribución. Las condiciones de acceso, uso personal y términos deben
verificarse **antes** de cualquier adquisición; un manifest, hash o QA de una
descarga no sustituye esa verificación. En esta revisión no se descargó
Dukascopy y el último recibo es
`/home/winterboss/.local/share/mtf-lab/market-data/dukascopy/receipts/dukascopy-eurusd-20160307-20160314-attempt-1.json`
con estado `BLOCKED_ACCESS_TERMS` (el recibo previo de timeout se conserva y no
se sobrescribe); el calendario OANDA utilizado por el piloto es sólo
`MODELED_WEEKLY_FX_NOT_ACCOUNT_VERIFIED`.

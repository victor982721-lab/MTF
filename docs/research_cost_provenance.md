# Procedencia de costes y calendario de investigación

## Separación de evidencia

Verificación documental pública: 2026-09-13. No se consultó una cuenta, ni se
observaron fills o cargos DEMO. Estos datos no acreditan la entidad, contrato,
saldo, permisos ni condiciones de la cuenta de Víctor. Tampoco son un historial
de las condiciones ofrecidas en 2016.

| Capa | Estado actual | Uso permitido |
|---|---|---|
| HistData Bid/Ask | sujeto al manifiesto y validación de cada partición (actualmente enero–diciembre 2016) | precios de esa fuente, no liquidez ejecutable del bróker |
| Latencia/slippage | supuestos del protocolo de Víctor | base 5 s / 0.1 pip; adverso 10 s / 0.2 pip por fill |
| Condiciones públicas actuales | documentadas, no vinculadas a la cuenta | sensibilidad condicional; nunca `OBSERVED_DEMO` |
| Contrato y calendario histórico completos | no acreditados | bloqueo de una conclusión económica favorable |
| Fills y costes DEMO | no observados | no inferirlos de precios o simulaciones |

## Fuentes públicas

Pepperstone publica para cTrader una comisión de USD 6 por ida y vuelta,
proporcional para operaciones menores de un lote, y conversión cuando la cuenta
no está denominada en USD. Su página también distingue Standard de Razor,
sitúa el rollover a las 17:00 de Nueva York y remite a la plataforma para las
tasas de swap variables. La versión consultada identifica a Pepperstone Markets
Limited (Bahamas). Esto no confirma la entidad ni tarifa aplicable a una cuenta
concreta. [Costes y financiación](https://pepperstone.com/en/trading/costs-and-fees).

Un documento de Pepperstone Limited de diciembre de 2021 ejemplifica USD 3 por
lado para 100,000 EUR/USD. Sirve para aclarar la unidad del ejemplo publicado,
no para adoptar automáticamente un contrato británico antiguo ni completar los
costes desconocidos de otra cuenta.
[Ejemplo contractual fechado](https://files.pepperstone.com/Pepperstone-Limited-Cost-and-Charges-December-2021-Website-FINAL.pdf).

El horario general público utiliza servidor GMT+3 durante el horario de verano
estadounidense y GMT+2 durante su horario estándar. Para Forex indica de lunes
a jueves 00:01–23:59 y viernes hasta 23:55; advierte cambios por festivos.
La página vigente no proporciona un calendario histórico exhaustivo de 2016.
[Horario general y festivos actuales](https://pepperstone.com/en/about-us/trading-hours).

## Tratamiento reproducible

El calendario numérico opcional usa viernes 17:00–domingo 17:00 Nueva York como
**modelo aproximado** de cierre semanal. Una descripción histórica de OANDA
usa esas horas aproximadas; su horario actual tiene minutos y pausas diferentes.
No se presenta el modelo como calendario de OANDA, HistData o de la DEMO.
[Descripción histórica aproximada](https://www.oanda.com/assets/documents/402/Hours_of_Operation_bT4prdz.pdf) ·
[Horario actual distinto](https://www.oanda.com/us-en/trading/hours-of-operation/).
El modelo sólo permite continuidad numérica entre barras observadas: no crea
barras de sábado, no acredita feriados y no habilita ejecución externa.

- Aplicar un calendario público actual a ticks históricos es **retroproyección
  modelada**, no evidencia de que el bróker estuvo abierto en esas fechas.
- El corte intradía modelado, si se utiliza, se calcula en `America/New_York`;
  su margen de 30 minutos no cambia el timestamp EST fijo de HistData.
- Los festivos desconocidos, mínimos, grid, margen, conversión, financiación y
  comisiones no se transforman en cero ni en `verified=true`.
- Un diagnóstico parcial puede mostrar gross, exposición, señales y coste de
  equilibrio. La expectativa **neta completa** permanece desconocida cuando
  falta un componente aplicable; no hay selección favorable ni promoción.
- El ledger descontará cada cargo una sola vez. Spread y slippage ya incorporados
  a los fills se muestran como atribución, no se descuentan nuevamente del PnL.
- Antes del holdout se necesita una especificación vinculada a la cuenta objetivo,
  sus fuentes, vigencia, calendario y escenarios congelados. La lectura del
  bróker y cualquier operación externa conservan sus gates independientes.

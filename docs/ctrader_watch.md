# Observación continua cTrader y panel local

## Alcance

`ctrader watch` une el proveedor cTrader existente, su cola de mensajes y
normalizador con `RuntimeCoordinator`, M1/M5/M15, indicadores, señales y
persistencia. Es **sólo lectura**: no construye un ejecutor ni envía órdenes.
La prueba offline utiliza ese mismo cliente/proveedor con un transporte
controlado; no sustituye al runner por uno de demostración.

La aplicación externa sigue pendiente de Spotware y OAuth, según el
[estado de integración](ctrader_integration_status.md). Esta mejora local no
acredita conexión, permisos ni datos observados en el bróker.

## Uso offline desde el checkout

No requiere secretos ni una cuenta. El perfil sigue siendo DEMO/accounts,
pero `--fixture` identifica la sesión como `SYNTHETIC`, el origen como
`SYNTHETIC_FIXTURE` y el entorno observado como `OFFLINE`.

```bash
./mtf-lab ctrader watch --fixture \
  --db runtime/ctrader-watch/fixture.sqlite3 \
  --duration 30 --max-messages 120 --checkpoint-every 25 \
  --report runtime/ctrader-watch/first.json

./mtf-lab ui --host 127.0.0.1 --port 8765 \
  --db runtime/ctrader-watch/fixture.sqlite3
```

El JSON incluye `session_id`, `analysis_id`, hashes semánticos, contadores,
procedencia, motivo de parada, `clean_stop` y estado del checkpoint. La base
de salida debe indicarse explícitamente y los archivos nuevos son privados
(`0600`); no se elige una base productiva por defecto.

Para reanudar, usa el `session_id` del primer resultado y la misma base y
configuración:

```bash
./mtf-lab ctrader watch --fixture \
  --db runtime/ctrader-watch/fixture.sqlite3 \
  --session ID_DEL_RESULTADO --resume \
  --duration 30 --max-messages 120 \
  --report runtime/ctrader-watch/resumed.json
```

La fixture conserva un cursor verificable y avanza al siguiente tramo, sin
volver a ingerir los primeros mensajes. Es una secuencia sintética finita de
hasta 5,000 mensajes, no historia del mercado. Un estado incompatible, otro
análisis o captura posterior al checkpoint se rechaza; no se mezcla con una
sesión nueva. La repetición desde cero con la misma entrada/configuración
conserva los hashes semánticos aunque cambien los identificadores de sesión.

## Límites y recuperación

- `--duration` limita tiempo real monotónico; `--idle-timeout` limita inactividad.
- `--max-events` y `--max-messages` son alias: cuentan **mensajes de protocolo
  por invocación**, no eventos normalizados ni velas. Un mensaje se procesa
  como unidad, aunque produzca varios registros.
- `SIGINT`/`SIGTERM` solicitan parada cooperativa; no se procesa el mensaje
  devuelto por un poll si ya llegó STOP. El runner guarda checkpoint y cierra
  el proveedor. Una parada acotada deja captura `PAUSED`, no feed activo.
- El checkpoint incluye el estado de runtime y del runner en una transacción.
  La reanudación exige identidad de datos, configuración, versión y análisis
  exactos; no usa un checkpoint alterno por conveniencia.
- Cada proceso nuevo descarta la cotización en memoria previa. Una nueva
  generación o discontinuidad exige cotización fresca y conciliación explícita;
  conectarse de nuevo no demuestra continuidad. Sin esa evidencia el análisis
  permanece bloqueado. La CLI no inventa un backfill de mercado.
- El CLI observa spots `mid`, `bid` o `ask`; no interpola trendbars nativas.
  El [flujo histórico nativo](../README.md) sigue separado y no fabrica bid/ask
  ni fills.

`--network` es una ruta distinta, explícita, para una futura sesión DEMO ya
autorizada. Reutiliza la preparación y verificación fresca de `ctrader query`
antes de autorizar la cuenta, resolver el símbolo y suscribir spots. Rechaza
REAL/LIVE, inventarios mixtos, permisos de trading no solicitados y ejecución
habilitada. No se utilizó esa ruta externa para validar esta entrega.

## Qué muestra el panel

- Procedencia respaldada: `FIXTURE`, `HISTORICAL_REPLAY`, `DEMO_OBSERVED` o
  `UNKNOWN`, con causa. El nombre del proveedor y el entorno de configuración
  no prueban observación real; la evidencia sintética prevalece.
- Entorno observado separado del entorno de cuenta configurado, ejecución
  deshabilitada, instrumento, referencia/hash y rango disponibles.
- Estado de sesión/captura, feed activo, conexión, continuidad, último dato,
  frescura, cobertura, huecos y contadores de errores.
- La edad se actualiza aunque no lleguen datos nuevos. La frescura de una
  fixture o de un histórico completado es `NOT_APPLICABLE`, no un feed vivo.
- Un fallo de refresco es visible; conserva el último estado bueno y permite
  reintentar. No presenta errores crudos que puedan contener secretos.

La UI consulta registros persistidos: no recalcula indicadores ni convierte
simulaciones en órdenes o rentabilidad. `RUNNING` en una sesión resumible no
prueba un proceso vivo; se muestra por separado el estado de captura y el feed.

## Verificación

Los tests de runner, CLI y observabilidad se ejecutan con HOME/XDG/TMP/estado
aislados y red externa bloqueada. Incluyen límites, STOP, recuperación exacta,
discontinuidad, rollback del checkpoint, reproducibilidad, cierre del
proveedor y errores de refresco. El gate global vigente usa
[`tools/quality_gate.py`](../tools/quality_gate.py); sus reglas están en
[calidad global](refactor_quality.md).

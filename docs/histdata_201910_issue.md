# HistData EUR/USD 2019-10 — consulta técnica no enviada

Asunto: EUR/USD Generic ASCII Tick 2019-10 — regresión temporal y bloque duplicado en el ZIP

Hola HistData,

Solicito confirmar y, si corresponde, corregir el archivo oficial siguiente:

- Formato: Generic ASCII Tick Bid/Ask
- Instrumento/mes: EUR/USD, 2019-10
- Archivo: HISTDATA_COM_ASCII_EURUSD_T_201910.zip
- SHA-256 observado: fd4d1765b95b592ebee9e2c3e03a2a8bf973796257bb20ea327a3dad63bde5f1
- Tamaño observado: 13133983 bytes
- Miembros: DAT_ASCII_EURUSD_T_201910.csv y DAT_ASCII_EURUSD_T_201910.txt

Hallazgo reproducible en el CSV, sin ordenar, eliminar ni modificar filas:
- La fila 1,929,387 termina en 20191027 195959933.
- La fila 1,929,388 retrocede a 20191027 190000504 (−3599.429 s).
- Las filas 1,928,152–1,929,387 se repiten byte a byte en 1,929,388–1,930,623 (1236 filas; bloque repetido de 48,204 bytes).
- La siguiente fila vuelve a 20191027 200001078.

El companion status report del mismo ZIP enumera 55 gaps mayores a 60 s y reporta intervalo máximo de 233404 ms e intervalo promedio de 5405 ms, pero no señala esta regresión ni el bloque duplicado.

¿Podrían confirmar, por favor?:
1. Si este ZIP es defectuoso y si existe una versión corregida/versionada del mes 201910.
2. Si la regresión/bloque repetido proviene de un error de generación o de una duplicación de origen.
3. Si existe una explicación oficial relacionada con la base temporal; la especificación pública indica EST fijo sin ajustes DST, por lo que no aplicaré ninguna corrección local de horario.
4. La ruta oficial para obtener la versión corregida y su SHA-256/tamaño, o la disposición oficial si no se corregirá.

No solicito ni aplicaré sorting, deduplicación, desplazamiento de timestamps o relabeling local. El ZIP original se conserva íntegro para auditoría.

Referencias públicas: https://www.histdata.com/f-a-q/ ; https://www.histdata.com/f-a-q/data-files-detailed-specification/ ; https://www.histdata.com/support/

Gracias.

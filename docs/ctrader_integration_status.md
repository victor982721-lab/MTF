# Estado de integración cTrader/Forex-CFD

Fecha de revisión: 2026-09-10. Checkout local: `8e3de714` como base; esta iteración aún no se ha publicado.

## Comprobado localmente

- Python: 3.14.4.
- `ctrader_open_api`, `google.protobuf`, Twisted y pyOpenSSL: no instalados en este host; no se instalaron automáticamente.
- Fixtures cTrader Protobuf, normalización de símbolos, escalas de trendbars, cotizaciones bid/ask, correlación, timeout/cancelación, heartbeat, cola acotada y reconexión: probados offline.
- Paper CFD: probados LONG/SHORT, ask/bid, latencias, spread, comisión, conversión/financiación desconocida y horizontes independientes.
- Ejecutor demo: probado únicamente con `DemoTransport`, sin red ni credenciales; REAL/LIVE se rechaza y `activate()` es obligatorio.

## Contrato oficial usado

- Protobuf TCP/TLS: `demo.ctraderapi.com:5035` para DEMO y `live.ctraderapi.com:5035` para LIVE, con entornos aislados.
- Límite documentado: 50 solicitudes no históricas/s y 5 históricas/s por conexión.
- OAuth: `accounts` es lectura; `trading` habilita operaciones. El código de autorización tiene vigencia corta y los tokens se guardan fuera del proyecto.
- Trendbars usan precios relativos y requieren reconstruir OHLC con la escala del instrumento; su volumen queda como dato de protocolo, no volumen económico.

Fuentes: [Open API](https://help.ctrader.com/open-api/), [endpoints](https://help.ctrader.com/open-api/proxies-endpoints/), [conexión](https://help.ctrader.com/open-api/connection/), [autenticación](https://help.ctrader.com/open-api/account-authentication/), [datos de símbolos](https://help.ctrader.com/open-api/symbol-data/), [OpenApiPy oficial](https://github.com/spotware/OpenApiPy).

## Pendiente de activación humana/externa

- Registrar y obtener aprobación de la aplicación cTrader.
- Configurar `CTRADER_CLIENT_ID` y `CTRADER_CLIENT_SECRET` fuera del repositorio.
- Ejecutar OAuth local y seleccionar explícitamente un `ctidTraderAccountId` observado como DEMO.
- Confirmar Pepperstone, entidad aplicable a México, tarifas, permisos de almacenamiento histórico y automatización DEMO.
- Instalar/probar el extra oficial del SDK en un entorno aislado y verificar TCP/TLS contra una cuenta DEMO autorizada.

Nada de lo anterior se infiere de los fixtures. No existe una verificación externa de Pepperstone ni se enviaron operaciones.

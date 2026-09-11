# Estado de integración cTrader/Forex-CFD

Fecha de revisión: 2026-09-10. Checkout local sobre `f88f070`; esta iteración queda lista para publicar tras la validación final.

## Comprobado localmente

- Python: 3.14.4 en `.venv`.
- Extra oficial instalado y comprobado en `.venv`: `ctrader-open-api 0.9.2`, `protobuf 3.20.1`, Twisted 24.3.0, pyOpenSSL 24.1.0, cryptography 42.0.8 y service-identity 24.2.0; `pip check` OK. El lock reproducible está en requirements-ctrader.lock.
- Codec Protobuf real, clases generadas, envelope/heartbeat, framing incremental, correlación, timeout/cancelación, heartbeat, cola acotada y reconexión: probados sin credenciales.
- El diagnóstico distingue `MISSING`, `UNIMPORTABLE`, codec operativo y perfil pendiente; `ctrader doctor --network` es una sonda DNS/TCP/TLS explícita y no autentica cuentas.
- Sonda externa acotada 2026-09-10: `demo.ctraderapi.com:5035` DNS/TCP/TLS OK (TLSv1.3); no OAuth ni cuenta autorizada.
- Paper CFD: probados LONG/SHORT, ask/bid, latencias, spread, comisión, conversión/financiación desconocida y horizontes independientes.
- Ejecutor demo: `DemoTransport` local y adaptación `CTraderDemoTransport` con mensajes Protobuf oficiales, gateway inyectado y ServerAccountObservation explícita; sin red/OAuth en pruebas, REAL/LIVE se rechaza y `activate()` es obligatorio.
- Pipeline cTrader → RuntimeCoordinator → CFD PAPER → SQLite v3/UI: probado con fixture sintético con señales auténticas y stores temporales.

## Contrato oficial usado

- Protobuf TCP/TLS: `demo.ctraderapi.com:5035` para DEMO y `live.ctraderapi.com:5035` para LIVE, con entornos aislados.
- Límite documentado: 50 solicitudes no históricas/s y 5 históricas/s por conexión.
- OAuth: `accounts` es lectura; `trading` habilita operaciones. El código de autorización tiene vigencia corta y los tokens se guardan fuera del proyecto.
- Trendbars usan precios relativos y requieren reconstruir OHLC con la escala del instrumento; su volumen queda como dato de protocolo, no volumen económico.

Fuentes: [Open API](https://help.ctrader.com/open-api/), [endpoints](https://help.ctrader.com/open-api/proxies-endpoints/), [conexión](https://help.ctrader.com/open-api/connection/), [autenticación](https://help.ctrader.com/open-api/account-authentication/), [datos de símbolos](https://help.ctrader.com/open-api/symbol-data/), [OpenApiPy oficial](https://github.com/spotware/OpenApiPy).

## Pendiente de activación humana/externa

- Registrar/aprobar la aplicación cTrader y configurar `CTRADER_CLIENT_ID`/`CTRADER_CLIENT_SECRET` fuera del repositorio.
- Ejecutar OAuth local con callback protegido y seleccionar explícitamente un `ctidTraderAccountId` observado como DEMO.
- Confirmar Pepperstone, entidad aplicable a México, tarifas, permisos de almacenamiento histórico y automatización DEMO.
- Crear el gateway SDK/Twisted síncrono autorizado y verificar TCP/TLS contra una cuenta DEMO; ninguna instalación, doctor o fixture envía operaciones.

Nada de lo anterior se infiere de los fixtures. No existe una verificación externa de Pepperstone ni se enviaron operaciones.

# Estado operativo y preservación

Verificación del 19 de septiembre de 2026. La publicación de código no es una
release de trading, una aceptación contractual ni una conclusión de rentabilidad.
El [receipt compacto](../reports/quality/mtf-operational-status-20260919.json)
conserva identidades, resultados y referencias a la evidencia privada.

## Conservación

- Los 30 commits pendientes hasta `66506ad`, la documentación y el código
  validado `48ef12a687f74f7050147e3ea71d704b3ce8b358` se publicaron por
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
| 3. Runtime | Código `48ef12a` validado y promovido: 1,057/1,057 pruebas, cero fallos/omisiones, sin red ni escrituras fuera del aislamiento. Cobertura: 79.906% líneas / 63.307% ramas. Launchers instalados y consulta DEMO comprobada. | Las nuevas modificaciones ejecutables requieren validación propia; documentación sola conserva el gate por equivalencia comprobada. |
| 4. App y contrato | Portal MTF Lab `Active` y callback local correcto; consulta `accounts` autenticada. | Entidad/condiciones aplicables a México, retención de datos y costos efectivos de la cuenta, no publicidad. |
| 5. Canaria de lectura | Cuenta DEMO y catálogo observados. Ventana histórica M1 de 60/60 barras completa, sin gaps. Los precios live obsoletos se rechazaron. | Frescura, continuidad y calentamiento observados en mercado abierto, separados de historia bounded. |
| 6. Shadow/forward | No iniciado, sin órdenes ni scopes nuevos. | Gates anteriores y autorización operativa separada; 72 horas/30 sesiones no se sustituyen por fixtures ni se acelera el reloj. |

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
precisas del protocolo. No se observaron fills, conversiones ni cargos realizados;
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
  No convertir la aprobación administrativa en permiso para `trading` o REAL.

El histórico es el filtro principal de la estrategia. El holdout sigue cerrado,
no hay ventaja neta acreditada y el bloqueo de una fuente no autoriza cambiar
silenciosamente el experimento.

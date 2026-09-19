# Estado operativo y preservación

Verificación del 19 de septiembre de 2026. La publicación de código no es una
release de trading, una aceptación contractual ni una conclusión de rentabilidad.
El [receipt compacto](../reports/quality/mtf-operational-status-20260919.json)
conserva identidades, resultados y referencias a la evidencia privada.

## Conservación

- Los 30 commits pendientes hasta `66506ad` se publicaron por fast-forward en
  el remoto privado `victor982721-lab/MTF`, sin force push ni GitHub Actions.
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
| 3. Runtime | Runtime promovido desde el paquete validado, lifecycle completo y 1,037/1,037 pruebas sin red. Cobertura: 79.912% líneas / 63.297% ramas. | Las nuevas modificaciones ejecutables requieren validación propia; documentación sola conserva el gate por equivalencia comprobada. |
| 4. App y contrato | Portal MTF Lab `Active` y callback local correcto; consulta `accounts` autenticada. | Entidad/condiciones aplicables a México, retención de datos y costos efectivos de la cuenta, no publicidad. |
| 5. Canaria de lectura | Cuenta DEMO y catálogo observados. Los precios obsoletos se rechazaron. | Frescura, continuidad y calentamiento observados en mercado abierto, separados de historia bounded. |
| 6. Shadow/forward | No iniciado, sin órdenes ni scopes nuevos. | Gates anteriores y autorización operativa separada; 72 horas/30 sesiones no se sustituyen por fixtures ni se acelera el reloj. |

## Correcciones de diagnóstico

El primer quality gate se interrumpió por decisión del coordinador, no por una
denegación del usuario. El segundo agotó 900 segundos; la suite previa ya había
requerido unos 1,430 segundos. La corrida con 1,800 segundos terminó sin fallos y
sin reducir pruebas o umbrales. Un timeout no demostró un defecto de código.

`APP_CREDENTIALS_REQUIRED` describió procesos sin variables inyectadas. Las
credenciales locales sí permitieron autenticación DEMO. Del mismo modo,
`Submitted` era una observación histórica, sustituida ahora por `Active`.

La canaria del sábado recibió la última marca del viernes. No se puede prometer
un feed fresco con el mercado cerrado ni declarar una avería usando sólo esa
ventana. Se preservaron los rechazos y los timestamps; no se relajaron los guards.

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

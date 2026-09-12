# Refactorización y calidad local

Revisión: 2026-09-12. Este documento describe únicamente la mejora verificable
del checkout local; no activa cTrader, no inicia OAuth y no envía operaciones.

## Alcance

Se separaron responsabilidades en configuración, dominio, adaptadores de datos,
servicios de consulta/simulación y persistencia incremental. Las extracciones
son helpers deterministas y conservan las fachadas públicas y los contratos de
causalidad. El estado sintético sigue marcado como `SYNTHETIC`/`PAPER`; no es
cotización, fill ni evidencia de rentabilidad.

La auditoría de arquitectura continúa siendo una segunda comprobación: `core`
no puede depender de sockets, OAuth, SQLite, CLI ni del SDK opcional, y no se
permite introducir ciclos ni efectos de importación.

## Puertas reproducibles

`tools/quality_gate.py` es la entrada única para la validación de esta revisión:

```bash
.venv-dev/bin/python tools/quality_gate.py \
  --runtime-python .venv/bin/python \
  --dev-python .venv-dev/bin/python \
  --json runtime/verification/quality.json
```

La salida separa cada comando y su cola diagnóstica. Las herramientas son
locales y no requieren red:

- Ruff y `ruff format` sobre el alcance refactorizado y los tests de entrega.
- mypy estricto sobre los módulos tipados establecidos.
- Pyright según `pyrightconfig.json` (Python 3.11/Linux) sobre los módulos
  refactorizados y el contrato tipado; excluye `build`, `dist`, caches y
  reportes generados.
- auditoría AST estricta de dependencias, ciclos y efectos de importación.
- suite `unittest` con Coverage.py de ramas y umbral mínimo de 60 %.

El inventario repository-wide de Ruff y la complejidad heredada pueden contener
deuda fuera de este alcance; se reportan como advisory y no se presentan como
limpios por el hecho de que pase la puerta mantenida.

## Resultado observado

En el árbol de esta revisión, el gate terminó con **342 pruebas**, cero fallos,
3 omitidas y **74 % de cobertura de ramas**. Ruff check/formato, mypy estricto,
Pyright (su alcance declarado) y la auditoría arquitectónica terminaron en cero
errores; la complejidad Ruff de los módulos refactorizados quedó en cero
diagnósticos C901. La auditoría AST registró 0 ciclos, 0 violaciones estrictas y
un máximo de complejidad 38 (frente a 76 en el baseline histórico).

El chequeo advisory de todo el repositorio aún registra deuda heredada fuera del
gate mantenido (356 hallazgos Ruff en 43 archivos, incluidos 8 C901 en
`runtime/processor.py`, `runtime/state.py` y `ops/ctrader_activation.py`). No se
oculta ni se atribuye esa deuda a la validación del refactor; es trabajo futuro
separado si se desea ampliar el alcance.

## Wheel e instalación

`install-mtf-lab-wheel.sh` construye un wheel con `setuptools==84.0.0` mediante
el frontend `build==1.6.1` (por defecto `SOURCE_DATE_EPOCH=0` para bytes
repetibles), o instala un wheel entregado por el usuario. Después verifica el
launcher `mtf-lab --help` e imprime el SHA-256. El artefacto no se publica a
PyPI y el runtime no escribe junto al código instalado.

## Pendientes fuera del código

La autenticación, el login, el consentimiento OAuth, la selección DEMO y la
verificación del servidor/bróker siguen siendo intervención explícita de Víctor.
Una pasada local de tests, tipos o fixtures no acredita permisos, catálogo,
fills, costes ni aceptación contractual.

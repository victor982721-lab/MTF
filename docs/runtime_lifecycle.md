# Lifecycle de runtimes privados

## Contrato

`/home/winterboss/.local/share/mtf-lab/runtime` es el único destino canónico y
siempre está protegido por el gestor. Una revisión nueva se crea internamente
como `runtime-review-<UTC>-<nonce>/runtime`; la preparación sólo puede escribir
en esa raíz y conserva como máximo una revisión `REVIEW_READY` (la más nueva).
La promoción es explícita y, sólo si se ejecuta, conserva un rollback inmediato.
No se conservan historiales completos regenerables.

El lock `~/.local/share/mtf-lab/.runtime-manager.lock` serializa preparación,
promoción, recuperación y GC. Cada review tiene `RUNTIME_LIFECYCLE.json`:
`BUILDING`, `REVIEW_READY` o `FAILED`. Un proceso vivo, un marcador de build,
un journal de promoción, un symlink fuera de la review, un owner inesperado o
un manifiesto ambiguo dejan el candidato en `needs_review`; nunca se borra por
nombre. La segunda ejecución de GC no cambia el resultado.

El GC integrado se ejecuta al comenzar y al terminar una preparación. También
está disponible de forma complementaria:

```bash
mtf-lab runtime inspect
mtf-lab runtime gc --dry-run
mtf-lab runtime gc --keep-reviews 0   # sólo después de revisar el plan
mtf-lab runtime gc --keep-reviews 0 --purge-review-evidence  # migración explícita de legacy
mtf-lab runtime promote --path <review>/runtime
```

Los `runtime-review-*` históricos que no tienen este marcador se aceptan para
eliminación únicamente cuando su manifiesto `STAGED_RUNTIME.json` demuestra
`project=mtf-lab`, `STAGED_RUNTIME`, `NOT_PROMOTED`, `current_pointer=null`,
destino exacto, árbol propio sin symlinks externos y ausencia de procesos. Los
directorios desconocidos, `market-data`, `runtime-preparation-*`, `soak` y
`~/.local/state/mtf-lab/research` se preservan.

## Causa de la acumulación histórica

El preparador anterior aceptaba cualquier `--destination` terminado en
`runtime`, publicaba con `os.replace()` y sólo retiraba su scratch. Los flujos
de revisión elegían un padre `runtime-review-*` distinto por intento; no había
lock, marcador de estado del padre, promoción, recuperación ni GC. Por eso cada
review retenía otra copia completa de Python, dos venvs, wheels y herramientas.

La preparación actual genera destinos internos, marca el estado, elimina el
review fallido y reconcilia reviews anteriores bajo el lock. `SIGKILL` deja un
marcador `BUILDING`, que sólo se recoge después de su periodo de gracia y con
el PID sin vida; un proceso concurrente o un árbol inseguro se conserva para
revisión humana.

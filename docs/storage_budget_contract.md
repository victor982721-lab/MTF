# Contrato de presupuesto agregado del corpus

**Estado:** implementado y validado offline; no habilita por sí solo nuevas
adquisiciones ni borra/mueve raws existentes.

## Hallazgo actual

El código conserva el límite de 40 GiB por partición por compatibilidad y ahora
añade un inventario físico agregado común para HistData y Dukascopy. Comprueba
la suma proyectada, los `.part` y la reserva física antes/durante/después de
escribir; no se hizo ninguna adquisición nueva durante esta integración.

## Contrato requerido

Un helper único debe inventariar los roots canónicos de raw de HistData y
Dukascopy:

- sólo archivos físicos bajo `raw/`, incluidos `.part`;
- sin seguir symlinks;
- deduplicación de hardlinks por `(st_dev, st_ino)`;
- manifests, receipts y cachés fuera del presupuesto;
- lock común de adquisiciones antes del preflight para evitar TOCTOU.

Con tamaño esperado `additional_bytes` y reemplazo explícito
`replacing_bytes`, la condición lógica es:

```text
projected = current_raw - replacing_bytes + additional_bytes
projected <= 40 GiB
```

La reserva física usa aritmética entera:

```text
available_bytes - additional_bytes >= ceil(filesystem_bytes * 0.20)
```

Con `Content-Length` desconocido, el streaming debe verificar el límite antes
de cada bloque, conservar el `.part` y su estado si falla, y revalidar el
inventario justo antes del rename/manifiesto final. Un raw huérfano cuenta y no
se elimina automáticamente.

## Pruebas obligatorias

- dos archivos individualmente bajo 40 GiB cuya suma excede el límite;
- manifests repetidos y hardlinks sin doble conteo, symlinks rechazados;
- descarga proyectada que falla antes de escribir manifest;
- resume de `.part` sin contar dos veces el prefijo;
- bordes de reserva 20% con `statvfs` simulado;
- respuesta sin `Content-Length` que falla durante streaming conservando el
  parcial;
- lock y presupuesto compartidos entre HistData y Dukascopy.

`MAX_DATASET_BYTES` por partición debe conservarse por compatibilidad, pero no
sustituye este presupuesto agregado. El helper no pausa ni modifica una
corrida histórica lectora.

La prueba viva de sólo lectura del inventario canónico registró 12 raws,
99,592,565 bytes, proyección dentro de 40 GiB y reserva libre de 39.22%:
`/home/winterboss/MTF/runtime/market-evidence/storage-budget-live-20260914T2355Z.json`
(SHA-256 `0576b2673358d3b0d0207759c493c300e5b369d740d55fa52f78bab63489a09b`).
La regresión focal `tests/test_storage_budget.py` contiene 8 casos; el gate
global histórico final19 ejecutó 948/948 pruebas. El contrato agregado queda habilitado
para las futuras adquisiciones sólo después de conservar sus receipts y gates
de proveedor.

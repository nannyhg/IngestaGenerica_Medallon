# Configuración de despliegue

Esta carpeta contiene los archivos de control del despliegue usados por [scripts/Deploy_DataOps.ipynb](../scripts/Deploy_DataOps.ipynb).

## Archivos incluidos

1. [config/deployment_config.yaml](deployment_config.yaml)
2. [config/deployment_order.json](deployment_order.json)

## 1) deployment_config.yaml

Define parámetros funcionales del despliegue.

### Campos

1. workspace
- Nombre lógico del workspace origen usado para mapeo de IDs.
- Valor recomendado en este proyecto: DataOps.

2. connections.sql_connection
- Nombre lógico de la conexión SQL que será mapeada al workspace destino.
- Valor usado por defecto: cnBDDataOps.

3. dataops_lakehouse_semantic_models
- Lista de semantic models a reconectar/refrescar durante el flujo.

4. folders
- Define las carpetas del workspace destino y qué items se moverán a cada una.
- Cada item debe coincidir exactamente con el nombre del artefacto en formato Nombre.Tipo.

### Cuándo editarlo

1. Si cambias el nombre de la conexión SQL.
2. Si agregas o quitas semantic models.
3. Si cambias la organización por carpetas.
4. Si agregas nuevos artefactos y quieres que se ubiquen automáticamente en una carpeta.

## 2) deployment_order.json

Define el inventario de artefactos y sus IDs de origen para reemplazo dinámico durante el import.

Cada entrada tiene:

1. name
- Nombre del item en formato Nombre.Tipo.

2. dataops_id
- ID del workspace de referencia (origen) que será reemplazado por el ID del workspace destino durante el despliegue.

### Reglas importantes

1. Si agregas un nuevo artefacto en src, también debes agregarlo aquí.
2. Si renombraste un artefacto, actualiza aquí el campo name para que coincida exactamente.
3. Mantén el archivo en formato JSON válido.
4. Usa codificación UTF-8 preferiblemente para evitar problemas de acentos.

## Cómo configurar correctamente (paso a paso)

1. Verifica que el workspace destino exista y se llame DataOps.
2. Revisa [config/deployment_config.yaml](deployment_config.yaml):
- connections.sql_connection debe corresponder a la conexión que usarán los pipelines (cnBDDataOps).
- folders debe incluir todos los artefactos que quieres mover por carpeta.
3. Revisa [config/deployment_order.json](deployment_order.json):
- Debe incluir todos los artefactos presentes en src.
- Cada name debe coincidir exactamente con la carpeta del artefacto (ejemplo: Data Ingestion API.DataPipeline).
4. Si agregaste nuevos notebooks/pipelines/lakehouses:
- agrégalos en deployment_order.json,
- agrégalos en folders dentro de deployment_config.yaml (si quieres moverlos automáticamente).

## Buenas prácticas

1. Antes de ejecutar despliegue, valida JSON/YAML para evitar fallos de parseo.
2. No elimines entradas históricas de deployment_order.json sin validar impactos en mapeo.
3. Versiona cambios de esta carpeta junto con cambios de src en el mismo commit.
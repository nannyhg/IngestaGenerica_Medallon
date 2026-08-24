# How to deploy DataOps

Esta guía describe el despliegue inicial de DataOps en Microsoft Fabric con origen GitHub.

## 1. Prerrequisitos

1. Debe existir un workspace de destino en Fabric.
2. El workspace de destino debe llamarse exactamente DataOps.
3. El origen del despliegue es GitHub (repositorio Fabric.DataOps).
4. Debes tener permisos para crear y administrar conexiones en el workspace de destino.


## 2.1 Configurar carpeta config antes de ejecutar

La carpeta config controla el comportamiento del despliegue.

Archivos:

1. [config/deployment_config.yaml](../config/deployment_config.yaml)
2. [config/deployment_order.json](../config/deployment_order.json)

Qué hace cada uno:

1. deployment_config.yaml
- Define workspace lógico, nombre de conexión SQL, semantic models y estructura de carpetas destino.

2. deployment_order.json
- Define los artefactos y sus IDs de referencia para el reemplazo dinámico de IDs en el workspace destino.

Cómo configurarlos:

1. Verifica que connections.sql_connection sea cnBDDataOps (o el nombre real que usarás).
2. Verifica que folders incluya todos los artefactos a mover.
3. Si agregas un nuevo artefacto en src, agrégalo también en deployment_order.json.
4. Si renombraste un artefacto, actualiza su name exactamente igual en ambos archivos.

Guía detallada de esta carpeta:

1. [config/README.md](../config/README.md)

## 3. Crear conexión requerida antes del despliegue

Antes de ejecutar el despliegue, crea manualmente la conexión en Fabric:

- Connection name: cnBDDataOps
- Connection type: Base de datos SQL en Fabric
- Authentication: método de autenticación de tu organización

Importante:

1. En esta fase no es obligatorio seleccionar base de datos manualmente para continuar con el flujo guiado.
2. La base de datos se crea en pasos posteriores del despliegue.
3. Esta conexión debe existir previamente porque actualmente la creación automática con OAuth 2.0 puede no estar soportada por el flujo CLI usado en el notebook.

## 4. Importar notebook de despliegue

1. En el workspace DataOps, importa el notebook scripts/Deploy_DataOps.ipynb.
2. Abre el notebook importado.
3. Revisa y ajusta la celda de parámetros iniciales:
	- sql_connection_name
	- github_token
	- repo_owner
	- repo_name
	- branch
	- folder_prefix

## 5. Ejecutar despliegue

1. Ejecuta las celdas en orden, desde arriba hacia abajo.
2. Verifica que el notebook descargue correctamente src, config y data desde GitHub.
3. Verifica que los elementos del workspace se creen/importen sin errores.
4. Verifica al final que:
	- exista BDDataOps.SQLDatabase,
	- exista la conexión cnBDDataOps autenticada,
	- los pipelines no reporten errores de permisos de conexión.

## 6. Validaciones recomendadas

1. Si aparece error 404 descargando desde GitHub, valida owner, repo, branch y permisos del token PAT.
2. Si aparece error de permisos en pipelines, valida que la conexión cnBDDataOps exista y esté autenticada.
3. Si aparece error de contenido de notebook, confirma que el import de notebooks se esté realizando con formato .py cuando el origen venga de estructura Git de Fabric.

## 7. Resultado esperado

Al finalizar, el workspace DataOps debe quedar con:

1. Lakehouses, notebooks y pipelines desplegados.
2. Base de datos SQL de configuración creada.
3. Conexión cnBDDataOps disponible y autenticada para ejecución de pipelines.

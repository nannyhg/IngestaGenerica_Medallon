# IngestaGenerica-Medallon

Proyecto de despliegue genérico de DataOps en Microsoft Fabric (arquitectura Medallion).

## Objetivo

Este repositorio permite desplegar de forma consistente los artefactos de DataOps en un workspace de Fabric, incluyendo:

1. Lakehouses
2. Notebooks
3. Pipelines
4. SQL Database de configuración

## Estructura principal

1. src/: artefactos Fabric a desplegar
2. config/: orden y configuración de despliegue
3. data/: datos auxiliares para inicialización
4. scripts/: notebook principal de despliegue
5. how-to/: guías operativas

## Requisitos previos

1. Debe existir un workspace destino en Fabric llamado exactamente DataOps.
2. Debes tener permisos para crear/actualizar artefactos y conexiones en ese workspace.
3. El origen de despliegue es GitHub.
4. Si el repositorio es privado, necesitas un GitHub PAT con permisos de lectura.

## Flujo de despliegue recomendado

1. Crear o validar el workspace DataOps.
2. Crear manualmente la conexión requerida:
	1. Connection name: cnBDDataOps
	2. Connection type: Base de datos SQL en Fabric
	3. Authentication: método de autenticación de tu organización
3. Importar y abrir scripts/Deploy_DataOps.ipynb en el workspace destino.
4. Configurar parámetros iniciales del notebook:
	1. sql_connection_name
	2. github_token (solo si aplica)
	3. repo_owner
	4. repo_name
	5. branch
	6. folder_prefix
5. Ejecutar el notebook en orden, de arriba hacia abajo.

## Notas importantes del despliegue

1. El notebook descarga artefactos desde GitHub y luego despliega en Fabric.
2. La SQL Database BDDataOps.SQLDatabase debe existir antes de resolver la conexión final usada por pipelines.
3. Los notebooks del repositorio se importan en formato .py cuando provienen de estructura Git de Fabric.
4. Los pipelines requieren que el usuario tenga acceso a la conexión que referencian.

## Errores comunes y solución

1. Error 404 al descargar desde GitHub
	1. Validar repo_owner, repo_name, branch y folder_prefix.
	2. Validar PAT (si privado): permisos y vigencia.

2. UnicodeDecodeError al leer config/deployment_order.json o yaml
	1. Ocurre por archivos en encoding cp1252/latin-1.
	2. Usar lectura robusta con fallback de encoding.

3. InvalidPath al importar artefactos
	1. Ocurre cuando se asume ruta plana src/{name}.
	2. Solución: buscar ruta real del item en estructura anidada.

4. InvalidNotebookContent
	1. Ocurre al importar notebooks como .ipynb cuando el contenido fuente es .py.
	2. Solución: importar notebooks con formato .py.

5. Error de permisos de conexión en pipelines
	1. Asegurar que cnBDDataOps exista y esté autenticada.
	2. Asegurar que se use el connection ID correcto en el contenido del pipeline.

## Documentación detallada

Para pasos operativos completos, consultar:

1. [how-to/How_to_deploy.md](how-to/How_to_deploy.md)

## Estado esperado al finalizar

1. Artefactos de Bronze, Silver, Gold y Config desplegados.
2. BDDataOps.SQLDatabase creada.
3. Conexión cnBDDataOps disponible y utilizable por pipelines.

## Autor

Yesid Meneses
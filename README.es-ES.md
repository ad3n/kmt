

# Kejawen Migration Tool (KMT)

Gestiona la migración de clústeres de PostgreSQL fácilmente

## Requisitos

- PostgreSQL 9.5 o superior

- Go 1.25 o superior

- `pg_dump` (opcional) para soportar migración inversa

## Características

- Soporte para múltiples conexiones y esquemas

- Migración inversa desde una base de datos existente

- Limpieza automática de migraciones sucias

- Y muchas más

## Instalación

### Usando Go Install

- Ejecuta `go install github.com/ad3n/kmt/v2@latest` para instalar `kmt` en `$GOBIN`

- Verifica usando `kmt help`

### Usando Go Build

- Clona el repositorio `git clone github.com/ad3n/kmt`

- Ejecuta `go build -o kmt`

- Verifica usando `./kmt help`

## Actualización

- Ejecuta `kmt upgrade` para actualizar a la última versión

## Comandos disponibles

- `kmt create <schema> <name>` para crear un nuevo archivo de migración

- `kmt up <connection> <schema>` para desplegar migración(es) desde la base de datos y esquema

- `kmt down <connection> <schema>` para revertir migración(es) desde la base de datos y esquema

- `kmt drop <connection> <schema>` para eliminar migración(es) desde la base de datos y esquema

- `kmt generate <connection> [<schema> [--table=<tables> --view=<views> --function=<functions> --mview=<mviews> --include-data]` para generar migración inversa desde tu base de datos y esquema `source` con las opciones `table`, `view`, `function` y `mview` (vista materializada) separadas por comas

- `kmt rollback <connection> <schema> <step>` para revertir la versión de la migración en la base de datos y esquema

- `kmt run <connection> <schema> <step>` para ejecutar una versión de migración en la base de datos y esquema

- `kmt sync <connection> <cluster> <schema>` para sincronizar la migración en el clúster para el esquema

- `kmt set <connection> <schema> <version>` para establecer la migración en una versión específica sin ejecutar los archivos de migración

- `kmt migrate <connection> <schema> <version>` para establecer la migración en una versión específica

- `kmt clean <connection> <schema>` para limpiar la migración en la base de datos y esquema

- `kmt version <connection>|<cluster> [<schema>]` para mostrar la versión de la migración en el clúster/base de datos y esquema

- `kmt compare <connection1> <connection2> [<schema>]` para comparar migraciones entre bases de datos

- `kmt inspect <table> <schema> <connection1> [<connection2> ...]` para inspeccionar una tabla en un esquema específico

- `kmt make <schema> <connection> <destination>` para que el `schema` en `destination` tenga la misma versión que la `source`

- `kmt test` para probar la configuración

- `kmt upgrade` para actualizar la CLI

- `kmt about` para mostrar la versión

Ejecuta `kmt help` para ver la lista completa de comandos

## Uso

- Crea una nueva carpeta de proyecto

- Copia el archivo Kmtfile.yml a continuación

```yaml
migration:
    pg_dump: /usr/bin/pg_dump
    folder: migrations
    source: default
    clusters:
        local: [local]
    connections:
        default:
            host: default
            port: 5432
            name: database
            user: user
            password: s3cret
        local:
            host: localhost
            port: 5432
            name: database
            user: user
            password: s3cret
            options:
                sslmode: disable
            schemas:
                public:
                    excludes:
                        - exclude_tables
                    with_data:
                        - data_included_tables
                user:
                    excludes:
                        - exclude_tables
                    with_data:
                        - data_included_tables
```

- Crea una nueva migración o genérala desde `source`

## Por hacer

- [x] Migrar tablas
- [x] Migrar enums (UDT)
- [x] Migrar funciones
- [x] Migrar vistas
- [x] Migrar vistas materializadas
- [x] Mostrar versión de migración
- [x] Mostrar Estado/Comparar
- [x] Comando de actualización
- [x] Refactorizar código
- [x] Comparación a nivel de tabla
- [x] Volcado SQL para comparación de tablas

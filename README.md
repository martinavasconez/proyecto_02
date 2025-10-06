# Data-Mining / Proyecto 02 — NYC Taxi Trips

## Descripción  
En este proyecto se implementó un **data warehouse** en Snowflake siguiendo la arquitectura **medallion** (Bronze → Silver → Gold) para integrar y modelar datos históricos de viajes de taxi (amarillo y verde) de la ciudad de Nueva York.  

La orquestación se realizó con **Mage.ai**, y el modelado con **dbt**.  
Se diseñó un pipeline idempotente que permite realizar **backfill mensual** desde 2015 hasta 2025 y generar capas limpias y analíticas.

---

## Arquitectura  
[Ver diagrama de arquitectura](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1&title=Diagrama%20sin%20t%C3%ADtulo.drawio&dark=auto#R%3Cmxfile%3E%3Cdiagram%20name%3D%22P%C3%A1gina-1%22%20id%3D%22NKxbOaUBdPIvlTyA3FKM%22%3E7VxZc9s4Ev41qoofpOIt8dHyMZldzySba3f2xQWRkISYJBgSko9fv2gClHiAFHVQ403sVEUSCDaBPj50NxocmFfh028Jipd%2FUB8HA0Pznwbm9cAwdM3W%2BQe0PIuWse6IhkVCfNlp2%2FCZvOD8Ttm6Ij5OSx0ZpQEjcbnRo1GEPVZqQ0lCH8vd5jQoPzVGC1xr%2BOyhoN76b%2BKzpWidGONt%2B3tMFsv8ybrjiishyjvLmaRL5NPHQpN5MzCvEkqZ%2BBY%2BXeEAmJfzRdx323B1M7AER6zLDX%2F5%2F%2Fg4%2Fc%2FS%2BuNb8M%2FbdPZ19n46Gxq2ILNGwUrOWI6WPecsSOgq8jFQ0Qfm9HFJGP4cIw%2BuPnKh87YlCwN5eU6C4IoGNMnuNefzueF5vD1lCX3AhSu%2BM3Nsh1%2BRA8AJw0%2BNU9M3DOOahmmIWfLMuzyVlUXqmCt%2FPhYEZsm2ZUFYpitlhaSSLDaUt3zkXyQr92Grs5utC87XuHHyUp%2FRLO%2Bu7csUq8wUu86UsTse2XW2WGO9L7aMT6xtAZrhYIq8h0V2W0G9bm9vJ%2B6NQiFt%2BKdSSCf7gztoxArt4q%2BroraoQ11SUjR6TqggG91VKKwxmfQlmYlCMk7AnzudBdR7%2BLGiDAOMI4aGOBKkHGjlPSxzvP2Rd0oZSlipm%2BnYxW7OQn5mT4mPJj5uJo7DPqhfJt6SrGnKb%2FuIkh8rzPg3mpAFifjyAc3vDE23BzfGYKINXNPQDPsCTBslC74UQAe%2BtPnA1i93V%2FLhKWbpKB83FyUMvTwZ3hgX2riGcvgrXS5KLO9XsTKuwmy3eaGALCL%2B3ePqjRNpGnKNlg%2FtySSsOlptILBoEbmV7GEQX1OcfJh9B2fB0DIAEbcC9%2B8RF91zStIRiZ%2BjGe%2BQSc8ZuPAQGyaW4MUq4nLKpMf%2Fi%2FCCeoSCtH0coshH0IuDh6G90AhlU%2BLeAOgDyJhEiwSnmfCfM3I05uoiL64xlxzxkS%2Bv8kVggVNGBlfmYGpGopO%2FSpC3bdKEx7Um6DsWHXCA%2BD05HYZDPhoUXIwEpwISPfxSE27HPbeghhIo9zaTqlVkvyURQ2VGpzcbScZxyou%2BbitWFlO56tv7WxL%2FWTCmPVYbUzuHe3QIAw2Fl2Se1Ucy9d28SR8w82B4oG10xbhNc08mD4ByFSz4MPzfLYxhukiQT%2FD2WkQjXHeSxpeXPJCpOUmys4%2FSZeagwYNANoSHS3cApB9pShihoOszyhgNCx0upREwGqtMosGcjIo5wSNRGouJzskTjGMaUwJUbtacWCqJ8GgrhhvCpwUEpiP0mFojQpkAPD6i9D4mMQbOHWWQpqbWp9wALbdugNZkZI%2FrOjXZ3wI7qpRRUymOyTi6Z%2BiJ3LMEYukGB6HNGahKNiS%2BD7dPOd6TF2maWi6fbFL2dGBfA60V46oiRNykZkWdzJu2muFWFEM%2FvQglGbMCqZM6pCp99d6kab4BxM8DEIapWKHPDhBWTaWe%2BW308Q0huiCE4ZYQwnDMvxkhOiT13hDi1SOEJKONxrpb%2BJuUtc3UrVcAIPV8Z4YcPCDEb8jRlqMdjY2yOC2nHoGcFTw65Ghzy5gH%2BOkSNlkgxxb58uu1x%2BPxlHhlEW%2FzulodLXwbT3xLlZWdGDPT2W4TYL%2B2XXOgBAr8tRX8zdsSHCBG1uWHqpgun%2FAR9HErYH1S9h4Nxxjl8JxTSekq8bC80Shs4FRoVT1RFS2GkgVmNVqZJmwmf4Ry1NPE008f%2FvzvzcBwUAioHM3SuJCNTGMUlXQnT6LONkn7oSekfQm5mwRFaS7SaRbnb64FIJyhj5KHd8li9k4TuZ%2F840J8whXDtsWP4peLCzGqSg737vKvm0%2BF1KkYb3vS9BXiVu9AlS8yUvUmiqhWlZ41e0Mp9w2lTodSllZFFvMwiLLsHYR6xqd82%2BBX22DcFaBY1RyGrfAZFfqlO30ZsFVPc37%2B%2Fe4bgLEmQfkNfTfys%2B2y%2FFRpff2c%2BGvVU4q%2Ffbi7fhOewsd3JhXhGXXhaWcVXocM4i%2BIklbVyiaqyPq8KFlPzL25OQe7OY5%2BIjenus16djenQ4KvIO2dCbYaFjcb9Dz7a0zKCZXJKyUNpYF7Ezybi%2FyaKIjUqhnBauaN0HQ8Ih6N0hHU5lRUthAYAnk0xi4ScR%2FUxviGa84u%2BsQN2ykHR7qjAHjFHrJu9IUaHaoPSZgVvtYzoQW9USy0bQVBIG1J9pqECz70gMz4%2F%2BhllWCYEiL3IfKWJML3AUZJRCLodBnREAXP99eY8ZHQZJSuF32Ky6mmZLb1EEUYUpVI2H0JTJVzEzkJn6yV%2BRPIOg%2FzwV2KRIitTHHklFZZYba6vm1iK8rgqrVyLlQfNlEPSLXltaV%2BNNDV4WOBZRFNuOa1Mg3uUU4iIyZ8TiCla%2FGTkpBPwntGQlzIMQmataK%2Bcupp01xnLLQ0CnJi7hbkxO1SiKkibh9JfG92HiS0Zh7H1bZV0DAc7gdI9Rwi%2F%2Fsq3TxdbvYU1VOpk9vRz1FIgmfR9T0O1hjWzcL1wrSN0rTluCqzFtfWKCGIf3LTQIzja7qjn4fipi5F7o41UdapBZhxhB9ypfQApmt30iRecoMVFwzRBkHeUC4RmdniOStcIdzljHK0yieaXclsn%2FM8rDKZA3J2gqPwkEea%2BOVhbWjxmcweCCcHNIXfMZSQXuqXRTMZjQ13ZUzDv6vRKQMX2OXKMKXw5aIwDR97NEHgXA3ZkngPEU7l2EnEfa6cddW%2BBTG39isMp9hvd%2FXy8ejOTQcspYYUuw0sfrOvN%2Fv62e0LkgI710XbabavRl%2Bpg6H8JO7Uznnu43FxCRzucalw7c0L6yxr4D%2FUfRzgje3PaNfZzQtX1w5jtDs5knj%2FjF7z8cDRi%2F5Z7ep6B24YLU5EC6tdvYPBtBLvn9V8lcD3HvXPotgu1BTuZIjlHsZts4so24j3z%2B0YPYd87bpnz%2FF5GG53WMPdtkOGbQyHLZ%2BjiPfPcCjyPR%2B3nS7Q2haRtHF73EWUbcT753aKkzXx8EkZ3hSkNfLJ7cAnXdP0ZkZJv%2FWUD1Bw%2FYSCqhJ6j70lFUHEqbzFtjwrn2wHJNC18nLY3a5OQb9FYzh561jy%2FdrW3GP5cYUejYo3Znn5vesEZcT%2Bd5c6mFMKAT6DHRD3uHPcO4sfKpVHrqJyRbXl2%2BOeer2G9Y6EMcEvctcQc8WPeHTMJVE8dgyzwlFCfqywR0I4AQFnoH35VoTsgLOXrHyajpoUYs9jxOrNts254t6KjSq75LquKPZUnWexepOYqthzNxjaXSIWR29Jy7RAod0lYrHNFiAE04UN0COfoFidU7YoHtzKFjdldTasanIUr3vvrjrDb3D4HyxuFZE5R1LxNo134tAan1V2vFXSDjbGXTLWq6I5i%2Fs9CpOGlxiUXsHRW5ro%2F0VBX6EaNM3a0TvEzY6piLQ2RiNzVz%2BXwXzhTsXGAjINB3uZUT6yxXIAlTJTcQFfnED1T7L89bfAVev8VD6Jqpq2v%2FNzuQd00OuXdE0VZ6m8d9Nqtvb4BORbkKr5FUzH0c%2FeOEiF15ZwTiOhIPDBVZc7b5d6QFIi94mWItzL9Z1H4jhKCRxSfDXvXNrpBfbnq1cdP63u%2BG2Kks%2Fi%2BNn1QwIbszg6rqxq0vVWGTrlA2ZHCvp8MZfitIfqSGl%2FYqwfFvjw6V9fbz5%2Fubz6HWzUNf%2FMGVuKqwvQ8QpX1gY4s13F%2FpwCzmxdkWC%2F%2BY69VfllV6AR8pg5INfNlzsZjIYZ9EHb9fRLtpxzjkDsD9e4Kq9QIF%2FB5cMrsRiOPIIudgLdYZmNn%2F4Yh654NZR1IjTkP7fvgRVF4tu36Zo3%2FwM%3D%3C%2Fdiagram%3E%3C%2Fmxfile%3E)

- **Bronze**: Ingesta cruda desde archivos Parquet oficiales de NYC Taxi.
- **Silver**: Limpieza, estandarización de tipos de datos, renombrado de columnas, normalización de fechas y horas.  
- **Gold**: Modelos analíticos con un **fact table (`fct_trips`)** y varias dimensiones:  
  - `dim_date`  
  - `dim_zone`  
  - `dim_vendor`  
  - `dim_payment_type`  
  - `dim_rate_code`  
  - `dim_service_type`  
  - `dim_trip_type`  

La orquestación de cargas y backfill se hace con Mage, y el modelado incremental y la materialización se gestionan con dbt.

---

## Cobertura de datos  
- **Período:** Enero 2015 – Agosto 2025 (amarillo y verde).  
- **Formato origen:** archivos **Parquet** oficiales por mes/año.  

Se hicieron dos tablas de auditoría para ver el estado de carga de cada mes:

🔗 **Archivo completo con detalle por año, mes y servicio:**  
[data_quality/yellow_audit.csv](data_quality/yellow_audit.csv)
[data_quality/green_audit.csv](data_quality/green_audit.csv)

Como se puede observar, todos los meses fueron cargados sin ni un problema.
---

## Estrategia de backfill & idempotencia  
1. **Bronze:**  
  Durante el proceso se identificó que el volumen total de datos superaba los 700 millones de registros, lo que generaba problemas de performance y timeouts al intentar cargas masivas de una sola vez. Después de varias pruebas se adoptó una estrategia de carga incremental en chunks, procesando cada mes de manera controlada y dividiendo archivos grandes en bloques de hasta 1 millón de filas para evitar errores de memoria o límites de Snowflake.
  ### Estrategia implementada

- **Ingesta mensual controlada:**  
  Cada archivo Parquet representa un mes de viajes para un servicio (`yellow` o `green`).  
  Antes de procesar un archivo se construye dinámicamente la URL y se descarga con `requests`.

- **Carga por chunks:**  
  Se divide en lotes de ~1M de filas usando `pyarrow`.  
  Cada chunk se exporta directamente a Snowflake con `write_pandas`, lo que permite manejar grandes volúmenes sin saturar memoria.

- **Columnas de control agregadas para trazabilidad:**
  - `VENTANA_TEMPORAL`: marca de tiempo de ingesta.  
  - `CHUNK_MES`: indica el número de chunk dentro de ese mes.  
  - `YEAR` y `MONTH`: para auditoría y consultas históricas.  
  - `RUN_ID`: identificador único de ejecución para rastrear procesos.  

- **Registro de auditoría y control de idempotencia:**  
  - Cada mes procesado se registra en tablas de auditoría (`RAW.AUDIT_YELLOW` y `RAW.AUDIT_GREEN`) con el estado de la carga (`ok`, `fail`, `skip`).  
  - Antes de cargar un mes se consulta esta tabla: si ya existe con estado `ok`, el proceso lo **omite automáticamente** para evitar duplicados.  
  - Se definió una **PRIMARY KEY** sobre campos clave (`LPEP_PICKUP_DATETIME`, `LPEP_DROPOFF_DATETIME`, `PULOCATIONID`, `DOLOCATIONID`) para asegurar que no se inserten filas duplicadas si un proceso se vuelve a ejecutar.

- **Trazabilidad y auditoría:**  
  Cada ejecución genera un registro detallado con:
  - Estado (`ok` = cargado, `fail` = error, `skip` = mes omitido por ya estar cargado).  
  - Número de filas cargadas (`ROW_COUNT`).  
  - Mensaje de error (`ERROR_MESSAGE`) en caso de fallas.  
  - Timestamp de ingesta (`INGESTED_AT_UTC`) y `RUN_ID`.

2. **Silver:**  
   - Normalización de tipos (fechas).  
   - Conversión de tipos de pago.
   - Limpieza de valores nulos y outliers (ej. tarifas negativas, duraciones 0).  

3. **Gold:**  
   - Generación de **dimensiones** con llaves `*_sk` (surrogate keys).  
   - `fct_trips` enlaza dimensiones mediante FK y contiene métricas:  
     - `trip_distance_km`  
     - `fare_amount`, `tip_amount`, `total_amount`  
   - **Idempotencia:**  
     - Modelos **incrementales** en dbt.  
---

## Gestión de secretos  
| Secreto              | Propósito                                         | Rotación / Responsable |
| -------------------- | ------------------------------------------------- | ---------------------- |
| `SNOWFLAKE_ACCOUNT`         | Identificador de la cuenta Snowflake              |                        |
| `SNOWFLAKE_USER`            | Usuario de conexión                               |                        |
| `SNOWFLAKE_PASSWORD`        | Contraseña del usuario Snowflake                  |                        |
| `SNOWFLAKE_DEFAULT_WH`       | Warehouse usado para ejecución de queries         |                        |
| `SNOWFLAKE_DEFAULT_DB`        | Base de datos destino        
| `SNOWFLAKE_DEFAULT_SCHEMA`        | Schema de la base de datos    
| `SNOWFLAKE_ROLE`        | Rol                    


---

## Diseño Silver 
La capa **Silver** toma los datos crudos de **Bronze** (archivos Parquet de Yellow y Green Taxis) y los transforma para asegurar calidad y consistencia antes de usarlos en la capa analítica.

#### Reglas de limpieza y estandarización aplicadas
- **Unificación de datasets Yellow y Green**
  - Se crean separados para cada servicio y se añade la columna `service_type` (`'yellow'` o `'green'`).
  - Se combinan con `UNION ALL` para formar una única tabla homogénea.
- **Normalización de nombres y tipos**
  - Renombrado de columnas a `snake_case` (ej. `TPEP_PICKUP_DATETIME` → `pickup_datetime`).
  - Conversión explícita a `TIMESTAMP` para `pickup_datetime` y `dropoff_datetime`.
- **Limpieza de outliers y valores inválidos**
  - Eliminación de viajes con duración negativa o mayor a 24h:  
    `DATEDIFF('hour', pickup_datetime, dropoff_datetime) < 24`.
  - Exclusión de distancias o tarifas negativas.
  - Conversión de valores negativos a `NULL` en métricas como `passenger_count`, `trip_distance` y `fare_amount`.
- **Homogeneización de tipo de pago**
  - Mapeo de `payment_type` a descripciones legibles:  
    `1 → Credit Card`, `2 → Cash`, `3 → No Charge`, etc.
- **Enriquecimiento geográfico**
  - Joins con la dimensión `stg_taxi_zones` para obtener `pickup_borough`, `pickup_zone`, `dropoff_borough` y `dropoff_zone`.
- **Control de calidad con tests dbt**
  - `accepted_values` en `service_type` (`yellow`, `green`).
  - `not_null` en campos clave (`pickup_datetime`, `dropoff_datetime`, `pu_location_id`, `do_location_id`).
  - `relationships` para validar que zonas (`pu_location_id` y `do_location_id`) existan en `stg_taxi_zones`.

---

## Capa **Gold** — Modelo dimensional y tabla de hechos

La capa **Gold** implementa un modelo **estrella (star schema)** compuesto por dimensiones normalizadas y una tabla de hechos transaccional de alto volumen.

#### Tabla de hechos `fct_trips`
- **Granularidad**: 1 fila = 1 viaje.
- **Características principales**:
  - **Primary Key:** `trip_id` (hash generado a partir de `pickup_datetime`, `pu_location_id`, `do_location_id`, `vendor_id`).
  - **Foreign Keys** hacia dimensiones:
    - `pickup_date_sk` y `dropoff_date_sk` → `dim_date`.
    - `pu_zone_sk` y `do_zone_sk` → `dim_zone`.
    - `vendor_sk` → `dim_vendor`.
    - `rate_code_sk` → `dim_rate_code`.
    - `payment_type_sk` → `dim_payment_type`.
    - `service_type_sk` → `dim_service_type`.
    - `trip_type_sk` → `dim_trip_type`.
  - **Métricas incluidas**:
    - Distancia (`trip_distance`).
    - Tarifas y recargos (`fare_amount`, `tip_amount`, `tolls_amount`, `airport_fee`, etc.).
    - Totales calculados (`total_amount` y `tip_percentage`).
    - Duración del viaje (`trip_duration_min`) y velocidad promedio (`mph`).
  - **Datos desnormalizados** para consultas rápidas:
    - Año, mes, día, hora y día de la semana de `pickup_datetime`.

#### Dimensiones creadas
- `dim_date`: calendario completo con claves sustitutas y atributos como año, mes, día y día de la semana.
- `dim_zone`: zonas TLC con borough y nombre de zona.
- `dim_vendor`: catálogo de vendors.
- `dim_rate_code`: descripción de códigos de tarifa.
- `dim_payment_type`: descripción de tipos de pago.
- `dim_service_type`: tipos de servicio (`yellow`/`green`).
- `dim_trip_type`: modalidad del viaje (Street-hail, Dispatch, etc.).

#### Calidad en Gold
- Tests dbt aplicados:
  - `unique` y `not_null` en `trip_id` y claves sustitutas.
  - `relationships` para asegurar integridad referencial entre hechos y dimensiones.
  - `accepted_range` (dbt_utils) para validar métricas:
    - `trip_distance` entre 0 y 100.
    - `trip_duration_min` entre 0 y 1440 min.
    - `total_amount >= 0`.
---

## Clustering  
Para optimizar el rendimiento de consultas sobre la tabla de hechos `FCT_TRIPS`, se evaluaron los patrones de uso y se aplicó **clustering físico en Snowflake**.

**Selección de llaves de clustering**  
Se analizaron las consultas más frecuentes, que suelen filtrar por:  
- Fecha de recogida (`pickup_date`, `pickup_year`, `pickup_month`),  
- Zona de recogida (`pu_zone_sk`),  
- Tipo de servicio (`service_type_sk`).  

Con base en esto, se definió un **clustering compuesto** sobre:  

(pickup_date_sk, pu_zone_sk, service_type_sk)

Este orden favorece el partition pruning cuando se aplican filtros por fechas y zonas, con segmentación adicional por tipo de servicio.

**Medición antes del clustering**

Se ejecutó una consulta representativa y se capturó el Query Profile como línea base:

 - Duración total: ~1.1 s
 - Bytes escaneados: ~56.8 MB
 -Micro-particiones leídas: 120 de 549 (scan progress: 100 %)

**Medición después del clustering**

Tras clusterizar, se volvió a ejecutar la misma consulta:

 - Duración total: ~0.92 s
 Bytes escaneados: ~57 MB
 Micro-particiones leídas: 118 de 549 (scan progress: 100 %)

**Conclusiones**

El número de micro-particiones escaneadas bajó de 120 → 118 y el tiempo de respuesta mejoró levemente (~1.1 s → 0.92 s).

Aunque la mejora es moderada, se confirma que Snowflake puede hacer pruning adicional al usar llaves de clustering alineadas con los filtros comunes.

Se recomienda mantener este clustering compuesto a largo plazo y evitar sobre-clusterizar.

Para más detalle técnico y capturas del Query Profile, ver el documento completo de análisis de clustering: 
[Clustering](evidences/clustering.pdf)

---

## Pruebas implementadas (dbt tests) 

Se implementaron **tests automáticos en dbt** para garantizar la calidad e integridad de los datos en cada capa del modelo:

### Capa Silver (stg_taxi_zones y stg_taxi_trips)
- **`unique` + `not_null`**
  - `zone_id` en `stg_taxi_zones` → asegura que cada zona TLC sea única y no nula.
  - `pickup_datetime` y `dropoff_datetime` en `stg_taxi_trips` → cada viaje debe tener fechas válidas.
  - `pu_location_id` y `do_location_id` no pueden ser nulos.
- **`accepted_values`**
  - `service_type` restringido a valores válidos: `['yellow', 'green']`.
- **`relationships`**
  - `pu_location_id` y `do_location_id` deben existir en `stg_taxi_zones.zone_id`.

### Capa Gold (dimensiones y fct_trips)
- **`unique` + `not_null`**
  - `date_sk` en `dim_date`, `zone_sk` en `dim_zone`, `vendor_sk` en `dim_vendor`.
  - `rate_code_sk`, `payment_type_sk`, `service_type_sk`, `trip_type_sk` deben ser únicos y no nulos.
  - `trip_id` en `fct_trips` como clave primaria.
- **`relationships`**
  - `pickup_date_sk` y `dropoff_date_sk` deben existir en `dim_date.date_sk`.
  - `pu_zone_sk` y `do_zone_sk` deben existir en `dim_zone.zone_sk`.
  - `vendor_sk`, `rate_code_sk`, `payment_type_sk`, `service_type_sk`, `trip_type_sk` deben existir en sus respectivas dimensiones.
- **`accepted_range` (dbt_utils)**
  - `trip_distance` entre 0 y 100 millas.
  - `trip_duration_min` entre 0 y 1440 minutos.
  - `total_amount` mayor o igual a 0.


### Interpretación de resultados
- Si un **test falla**, dbt lo reporta durante la ejecución del pipeline, indicando qué tabla y columna no cumple.
- Esto permite detectar rápidamente problemas como:
  - Datos fuera de rango (viajes con distancia negativa o extremadamente larga).
  - Referencias inválidas entre hechos y dimensiones.
  - Falta de claves únicas o valores nulos en campos críticos.

 Para un análisis más detallado de todas las pruebas implementadas y ejemplos de resultados**, ver el documento:  
[Calidad y documentación](evidences/calidad%20y%20documentación.pdfdocumentación.pdf)

---

## Troubleshooting

### Errores frecuentes
- **Invalid identifier**  
  → Revisar que los alias coincidan (`s.service_type_sk` vs `service_type_desc`).  
- **Numeric value 'yellow' is not recognized**  
  → Se da al intentar castear `service_type` como número; debe mapearse a SK en `dim_service_type`.  
- **Lentitud al correr dbt**  
  → Las cargas iniciales pueden demorar si los datasets son grandes (2015–2025). Se recomienda usar `--full-refresh` solo la primera vez.
- **Fallas por volumen de datos en Bronze/Silver**
→ Implementar carga por chunks (batches de 1M filas). 
→ Mantener una tabla de auditoría (AUDIT_GREEN, AUDIT_YELLOW) para saltar meses ya cargados y garantizar idempotencia.
→ Definir primary keys en tablas Bronze para evitar duplicados.

### Costos
- Recomendable **clusterizar** y particionar por fecha.  
- Usar `incremental` para evitar recalcular todo el histórico.  

---

## Checklist de aceptación

- [x] Cargados todos los meses 2015–2025 (Parquet) de Yellow y Green; matriz de cobertura en README. NYC.gov  
- [x] Mage orquesta backfill mensual con idempotencia y metadatos por lote.  
- [x] Bronze (raw) refleja fielmente el origen; Silver unifica/escaliza; Gold en estrella con fct_trips y dimensiones clave.  
- [x] Clustering aplicado a fct_trips con evidencia antes/después (Query Profile, pruning). Snowflake Docs  
- [x] Secrets y cuenta de servicio con permisos mínimos (evidencias sin exponer valores).  
- [x] Tests dbt (not_null, unique, accepted_values, relationships) pasan; docs y lineage generados.  
- [x] Notebook con respuestas a las 5 preguntas de negocio desde gol  


---

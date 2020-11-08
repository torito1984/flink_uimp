## Flink DOJO

Conjunto de ejerecicios de Flink para un caso simulado de IOT. Durante los ejercicios, se explora el API de Flink version 1.11.2 al completo,
tanto para el API de bajo nivel, Table y SQL. Se deja abierto una conexion a wikipedia para explorar un caso de uso libre.

El proyecto esta autocontenido y es posible ejecutar los trabajos tanto de manera local en una JVM o en un JobManager local
de Flink 1.11.2. Para ejecutar un JobManager local consulte la pagina del manual
https://ci.apache.org/projects/flink/flink-docs-release-1.11/ops/deployment/cluster_setup.html

## Ejemplo de ejecucion local

Antes de ejecutar cualquier ejemplo, debe compilar el codigo con SBT y generar el jar.
```
$ sbt compile
$ sbt assemblyWithProvidedDependencies
```

La linea de comandos para ejecutar cualquiera de los ejemplos de streaming se puede ver en execute_sample.sh

## Ejemplo de ejecucion en el cluster de Flink

Para generar el jar de despliegue en Flink, debe compilar el codigo con SBT y generar el jar sin las dependencias 
marcadas como 'provided'. Con esto el jar es mucho mas liviano y se evitan conflictos de dependencias
```
$ sbt assembly
```

La linea de comandos para ejecutar cualquiera de los ejemplos de streaming se puede ver en execute_sample.sh

## Ejercicio de Queryable State

El ejercicio de Queryable State require conocer el jobId del trabajo que vamos a consultar el estado. Para que esto funcione,
es neceario que el proxy de consulta de estado este funcionando. Las instrucciones para un cluster mononodo se pueden
encontrar en  https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/queryable_state.html en la seccion
"Activating Queryable State". Fundamentalmente, consiste en 2 pasos:

1. Editar el flink-config.yaml con la propiedad queryable-state.enable a true
2. Mover el jar flink-queryable-state-runtime_2.11-1.11.2.jar de la carpeta opt/ a la carpeta lib/
3. ./bin/start-cluster.sh

En caso de no poder conectar, buscar en localhost:8081 la linea que ponga "Started the Queryable State Proxy Server @ ..."
para conocer la IP y puertos en los que el Proxy de Estado esta escuchando.


## Orden de lectura de los ejemplos

Se sugiere seguir el siguiente orden al leer los ejemplos, aunque todos son autocontenidos:

1. FlinkHelloWorld
2. BasicPrintJob
3. ToFahrenheit
4. FahrenheitNCelsius
5. HeatAlert
6. LatestObservation
7. MaxRegisteredTemperature
8. MaxRegisteredTemperatureAggregation
9. ATemperature
10. MATemperatureProcessingTime
11. MATemperatureEventTime
12. SecondarySensor
13. EfficientSecondarySensor
14. MeanPerformanceTracking
15. StateTemperatureTracking
16. VehicleStateTracking
17. async.VehicleOwnerAsyncEnrichmentJob
18. CustomerServiceJob
19. cep.DangerousValves
20. table.StolenCarsJob
21. sql.SQLCustomerServiceJob
22. queryable.QueryableCustomerServiceJob
23. queryable.CustomerServiceClient
24. wikiedits.WikipediaAnalysis

## Ejecución desde SBT

Para ejecutar y probar tu aplicacion usando SBT invoca: 'sbt run'

Para poder ejecutar tu aplicación dentro de IntelliJ, tienes que seleccionar el classpath del módulo 'mainRunner'
en la configuaración run/debug. Simplemente abre 'Run -> Edit configurations...' y selecciona 'mainRunner' en el 
desplegable de 'Use classpath of module'.

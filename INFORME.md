# Informe - Coordinación

Este informe explica las decisiones tomadas en **Sum**, **Aggregation** y **Join**

# Librerías 

## uuid en message_handler

Cuando un cliente se conecta, el Gateway le asigna un client_id generado con uuid.uuid4().

- De esta forma no necesito estar controlando la coordinación, ya que un contador incremental necesitaria un estado compartido entre ellos. 

- En Sum, Aggregation y Join todo se indexa por client_id, y el gateway lo usa al volver para entregar cada resultado al cliente que corresponde, necesito asegurar que sean unicos y que no se pisen.

## hashlib en Sum

Se usa para decidir a qué Aggregator va cada fruta:

```python
index_aggr = int(hashlib.md5(fruit_name.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT
```

- uso hashing porque necesito distribuir de forma pareja entre los Aggregators sin necesidad de estar compartiendo ids/tablas/etc.
- Se usa hashlib en vez de has de Python, ya que este ultimo se randomiza en cada proceso y necesito que todos los sum manden la misma fruta al mismo agregator para que las sumas queden juntas. Hashlib me da el mismo resultado en cualquier proceso y en cualquier ejecución.
- **md5:** se utiliza especificamente porque entre las opciones de hashlib es rápido (usa menos recursos que otras opciones de hashlib que se analizaron) y es determinística y con buena distribución.

Esto garantiza que todas las sumas parciales de una misma fruta terminan en el mismo Aggregator, sin importar de que Sum vengan.

# Canales de comunicación 


* Cola de input: tipo queue de RabbiMQ para comunicacion entre gateway y los Sum, entre los Aggregators y Join y entre Join y el Gateway. El gateway envia los registros a los Sum, Aggregator los tops parciales a Join y Join el top final al Gateway.

* Exchange de Aggregation: exchange direct, se usa una routing key por Aggregator y se envia de los sum a Aggregator que le corresponde. Envía los datos y contadores.

* Exchange de control: exchange direct con routing key "EOF". Es entre el Sum que recibe el eof y todos los Sum. El exchange de control se utilizó para resolver el problema que ocurre cuando el eof llega a un solo Sum y los demas tienen que enterarse.



# Aggregation

Cada Aggregator solo se bindea a su propia routing key, así que recibe unicamente los datos de las frutas que le corresponden. Para cada cliente guarda una suma por fruta y un acumulado de registros procesados.

Cuando el acumulado iguala el total informado por los COUNT, sé que ya llegaron todos los datos y puedo calcular mi top parcial y enviarslo a Join.

# Join

Recibe un top parcial por cada Aggregator y por cliente. Lleva un set de los agg_id que ya respondieron y, cuando se que llegué al AGGREGATION_AMOUNT, junto los resultados parciales, me quedo con los TOP_SIZE  y se envía al Gateway.

# Sum:

Los Sum consumen de la misma cola, por lo que los registros de un cliente se reparten entre ellas como se explica anteriormente. El problema surge de que pueden tener registros de ese cliente aun sin procesar. Si cada Sum enviara su estado apenas se recibe el eof, estos se pierden. Por eso el EOF trae el total de registros y cada Sum informa cuántos proceso, luego los Aggregators comprueban que la suma de lo informado iguala el total.

- amount_by_client: suma por fruta, por cliente.
- processed_by_client: cuantos registros de ese cliente procesó desde el ultimo flush.
- closed_clients: clientes cuyo eof ya se conoce, con su total.


## Mensajes entre Sum y Aggregator

Entre Sum y Aggregation se envian dos tipos de mensaje, distinguidos por un tag:

1.  **DATA**: su contenido es ["DATA", client_id, fruta, cantidad], se usa para la suma de la fruta de un cliente. DATA usa un exchange creado con una sola routing key y se entrega a una sola cola. Sum no bindea ninguna cola, solo envía, y cada Aggregator bindea únicamente la suya.

2. **COUNT**:  su contenido es ["COUNT", client_id, procesados, total]. El count es un mensaje de Sum hacia Aggregation, enviando cuantos registros proceso y cuantos se esperaban en total y sirve para que el Aggregator sepa que ya recibió todos los datos de un cliente y que así lo pueda cerrar. Viaja por el exchange de Aggregation, el mismo canal que DATA, pero solo lleva contadores. Count no depende del volumen, depende de SUM_AMOUNT * AGGREGATION_AMOUNT ya que cada SUM notifica una vez a cada Aggregator. El único punto debil a resaltar, se trata la excepción de cuando se recibe data luego que el cliente fue cerrado por recibir un eof: resulta en AGGREGATION_AMOUNT mensajes mandados, pero se espera que sea un caso poco frecuente.   


## Casos

- Llega un registro de un cliente abierto -> Solo lo acumula en memoria. No envía nada. 
- Llega el EOF del Gateway -> Lo reenvia por el exchange de control y no flushea todavia.
- Llega un mensaje de control -> marca al cliente como cerrado y hace flush
- Llega un registro de un cliente ya cerrado -> Lo acumula y hace flush inmediatamente.

El flush solo se hace al recibir el control o con un registro tardio (es decir que llega luego que se cerro), y no hace nada si el SUM no procesó registros de ese cliente. 

El flush envía a los Aggregators lo que el Sum acumuló de un cliente:

1. Para cada fruta, manda un DATA solo al Aggregator que le corresponde según el hash. Si no procesó ningún registro de ese cliente (processed == 0), no envía nada para evitar mensajes innecesarios.
2. Después manda un COUNT con la cantidad de registros que procesó y el total esperado, a todos los Aggregators.
3. Reinicia sus contadores y estado de ese cliente, asi no se contabiliza de más.

**Caso borde:** Con total siendo 0, se procesa 0 y ningun Sum enviaría nada, el Aggregator luego no cerraria el cliente. Por eso en esos casos donde no hay total, EOF manda directamente COUNT(0, 0).


# Escalabilidad
- Join, Aggregation y Sum, manejan estados internos, estos estan aislados y se guardan a partir del client_id con las ventajas que se explican anteriormente de tener un Id consistente e identificable entre procesos. 
- Los datos, al recibirse, no mandan un mensaje por cada fila. Si recibo 50 o 1000 datos con mi cliente abierto, sólo al recibir un mensaje desde control o un eof, se enviaran los mensajes correspondientes. A la vez se limpian los datos de las estructuras internas para evitar la sobrecarga de memoria con datos que ya fueron procesados y enviados. 
- En SUM no estoy guardando todos los mensajes, reduciendo la memoria usada que termina dependiendo de la cantidad de frutas unicas.
- Debido a la funcion de Hashing, se espera que la distribución de los aAgregators sea pareja, evitando asi sobrecargar un solo Aggregator y no utilizar los recursos en su totalidad.


# Threads

## Utilizacion de threads en Sum

Cada Sum necesita escuchar: la cola de input (registros) y el exchange de control (avisos de EOF). Start_consuming de mi middleaware es bloqueante por objeto, así que se usa un thread para cada una:

- El thread principal consume los registros.
- El thread de control consume el exchange de control.

Los dos comparten el estado del Sum y los exchanges de salida hacia los Aggregators, por lo que ambos accesos se protegen con un lock. Esto es necesario porque las conexiones de pika no son thread-safe.

Aggregation y Join usan un solo thread, porque reciben de un unico canal.

# Cierre graceful 

Ante un SIGTERM, cada control deja de consumir stop_consuming y cierra sus conexiones (colas y exchanges) para liberar los recursos antes de terminar. En Sum además se espera al thread de control.


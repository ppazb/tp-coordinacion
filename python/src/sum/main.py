import os
import hashlib
import logging
import threading
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

DATA = "DATA"
COUNT = "COUNT"


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        
        # Coordinacion con otros SUM, tengo que asegurarme que los eof lleguen a todos 
        self.control_exchange_sender = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, ["EOF"], bind_queue=False)
        self.control_exchange_receiver = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, ["EOF"])

        # creo un exchange por aggr con el mismo exchange y distinta routing key
        #luego solo envio a uno de ellos, el que corresponda segun el hash de la fruta
        self.data_output_exchanges = []
        for agg_id in range(AGGREGATION_AMOUNT):
            routing_key = f"{AGGREGATION_PREFIX}_{agg_id}"
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST,AGGREGATION_PREFIX,[routing_key],bind_queue=False,)
            self.data_output_exchanges.append(exchange)

        #frutas por cliente
        self.amount_by_client = {}

        # registros de cada cliente procesados desde el ultimo flush
        self.processed_by_client = {} 

        # clientes que ya recibieron eof
        self.closed_clients = {}     

        # Me aseguro de que se accedan a las secciones criticas correctamente
        self.lock = threading.Lock()

    def _send_count(self, client_id, processed, total):
        # envio la cantidad de datos procesados y el total esperado, para que sepa cuando enviar el top
        message = message_protocol.internal.serialize([COUNT, client_id, processed, total])
        for exchange in self.data_output_exchanges:
            exchange.send(message)

    def _flush(self, client_id):
        data = self.amount_by_client.pop(client_id, {})

        processed = self.processed_by_client.pop(client_id, 0)
        if processed == 0:
            # No hay datos para enviar 
            return
        
        for fruit_name, item in data.items():
            # calculo el hash de la fruta para enviar solo al aggregator correspondiente
            index_aggr = int(hashlib.md5(fruit_name.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT
            message_serialized = message_protocol.internal.serialize([DATA, client_id, item.fruit, item.amount])
            self.data_output_exchanges[index_aggr].send(message_serialized)

        self._send_count(client_id, processed, self.closed_clients[client_id])

    def _process_data(self, client_id, fruit, amount):
        # No tengo al cliente, lo inicializo
        if client_id not in self.amount_by_client:
            self.amount_by_client[client_id] = {}
            self.processed_by_client[client_id] = 0
        
        fruits = self.amount_by_client[client_id]
        new_item = fruit_item.FruitItem(fruit, int(amount))
        if fruit not in fruits:
            fruits[fruit] = new_item
        else:
            fruits[fruit] = fruits[fruit] + new_item
        self.processed_by_client[client_id] += 1

        #cubro el caso borde donde los datos llegan dsps q el eof, y ya se habia cerrado el cliente
        # envio de una 
        if client_id in self.closed_clients:
            self._flush(client_id)  


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        #es data
        if len(fields) == 3:
            with self.lock:
                self._process_data(*fields)
        #eof
        else:
            client_id, total = fields
            self.control_exchange_sender.send(message_protocol.internal.serialize([client_id, total]))
            #el cliente no tiene datos que enviar 
            if total == 0:  
                with self.lock:
                    self._send_count(client_id, 0, 0)
        ack()

    def process_control_message(self, message, ack, nack):
        # recibi un eof por el control exchange, tengo que cerrar el cliente y envio lo acumulado
        client_id, total = message_protocol.internal.deserialize(message)
        with self.lock:
            self.closed_clients[client_id] = total
            self._flush(client_id)
        ack()

    def start(self):
        # Creo un thread para recibir los mensajes de control y en el thread principal recibo los datos
        self.control_thread = threading.Thread(target=self.control_exchange_receiver.start_consuming, args=(self.process_control_message,),)
        self.control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

        self.control_thread.join()

    def stop(self):
        # Paro el consumo 
        self.input_queue.stop_consuming()
        self.control_exchange_receiver.stop_consuming()

        #cierro
        self.input_queue.close()
        self.control_exchange_receiver.close()
        self.control_exchange_sender.close()
        for exchange in self.data_output_exchanges:
            exchange.close()


def handle_sigterm(signum, frame, filter_instance):
    filter_instance.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    # Manejo de SIGTERM
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(s, f, sum_filter))
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
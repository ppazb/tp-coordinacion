import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


DATA = "DATA"
COUNT = "COUNT"


class AggregationFilter:
    def __init__(self):
        #Setteo una exchange con mi routing key para recibir los datos y count de los Sum
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"])

        #Setteo una queue para enviar los tops parciales
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        
        self.data_by_client = {}    

        #registros ya informados por sum
        self.processed_by_client = {}
        #total de registros que me envio el cliente
        self.total_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        # Si no existe el cliente, inicializo su diccionario de frutas
        if client_id not in self.data_by_client:
            self.data_by_client[client_id] = {}
        
        fruits = self.data_by_client[client_id]
        new_fruit = fruit_item.FruitItem(fruit, int(amount))
        if fruit not in fruits:
            fruits[fruit] = new_fruit
        else:
            fruits[fruit] = fruits[fruit] + new_fruit
    
    def _process_count(self, client_id, processed, total):
        self.processed_by_client[client_id] = self.processed_by_client.get(client_id, 0) + processed
        self.total_by_client[client_id] = total

        #llegue al total esperado, envio el top parcial
        if self.processed_by_client[client_id] == total:
            self._send_partial_top(client_id)

    def _send_partial_top(self, client_id):
        # ordeno los items 
        items = sorted(self.data_by_client.pop(client_id, {}).values())

        # me quedo con los TOP_SIZE mayores
        fruit_top = [(item.fruit, item.amount) for item in reversed(items[-TOP_SIZE:])]

        #lo envio
        message_serialized = message_protocol.internal.serialize([client_id, fruit_top, ID])
        self.output_queue.send(message_serialized)

        # limpio los datos del cliente
        self.processed_by_client.pop(client_id, None)
        self.total_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        kind, *fields = message_protocol.internal.deserialize(message)
        if kind == DATA:
            self._process_data(*fields)
        elif kind == COUNT:
            self._process_count(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        # Paro el consumo y cierro lo que use 
        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        self.output_queue.close()


def handle_sigterm(signum, frame, filter_instance):
    filter_instance.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    # Manejo de SIGTERM
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(s, f, aggregation_filter))
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
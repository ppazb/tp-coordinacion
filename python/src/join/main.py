import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        #recibo de los agregadores y envio al output
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.tops_by_client = {}
        self.agg_counts = {}

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id, partial_top, agg_id = fields

        # inicializo si no lo tengo
        if client_id not in self.tops_by_client:
            self.tops_by_client[client_id] = []
        if client_id not in self.agg_counts:
            self.agg_counts[client_id] = set()

        # agrego el agg_id a los que ya respondieron 
        self.agg_counts[client_id].add(agg_id)

        top = self.tops_by_client[client_id]
        for fruit, amount in partial_top:
            bisect.insort(top, fruit_item.FruitItem(fruit, amount))

        # Cuando responden todos los agregadores finaliza el trabajo
        if len(self.agg_counts[client_id]) == AGGREGATION_AMOUNT:
            # obtengo el top final y lo envio al output
            fruit_chunk = list(top[-TOP_SIZE:])
            fruit_chunk.reverse()

            final_top = []
            for item in fruit_chunk:
                final_top.append((item.fruit, item.amount))
            
            self.output_queue.send(message_protocol.internal.serialize([client_id, final_top]))

            # limpio los datos de ese cliente 
            self.tops_by_client.pop(client_id, None)
            self.agg_counts.pop(client_id, None)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        # dejo de consumir
        self.input_queue.stop_consuming()

        # cierro
        self.input_queue.close()
        self.output_queue.close()

def handle_sigterm(signum, frame, filter_instance):
    filter_instance.stop()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    #manejo de sigterm
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(s, f, join_filter))
    join_filter.start()
    return 0

if __name__ == "__main__":
    main()
import uuid
from common import message_protocol


class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())
        self.sent_records = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.sent_records += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id, self.sent_records])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        #me aseguro que el mensaje recibido es el mismo client_id y que tiene los campos correctos
        if fields and len(fields) == 2 and fields[0] == self.client_id:
            return fields[1]
        return None
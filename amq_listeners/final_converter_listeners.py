import stomp
import ujson


class FinalConverterListener(stomp.ConnectionListener):
    def __init__(self,
                 final_converter,
                 indexer_queue_name,
                 c):
        self.final_converter = final_converter
        self.indexer_queue_name = indexer_queue_name
        self.c = c

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        dict_message = ujson.loads(message)
        print('received a message "%s"' % dict_message)
        self.final_converter.convert_and_build_final_records(dict_message.get('clusters_to_convert'),
                                                             dict_message.get('clusters_to_delete'),
                                                             dict_message.get('expressions_to_delete'),
                                                             dict_message.get('timestamp'))
        print('processed message')
        self.final_converter.send_bulk_request_to_indexer(self.c,
                                                          self.indexer_queue_name)
        print('message to indexer sent')
        self.final_converter.flush_all()
        print('final_converter_flushed')
        self.c.ack(headers['message-id'], headers['subscription'])

    def on_disconnected(self):
        print('disconnected')
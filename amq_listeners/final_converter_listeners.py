import stomp


class FinalConverterListener(stomp.ConnectionListener):
    def __init__(self,
                 final_converter: FinalConverter,
                 c):
        self.final_converter = final_converter
        self.c = c

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        unpickled_message = pickle.loads(message)
        print('received a message "%s"' % unpickled_message)
        self.final_converter.convert_and_build_final_records(unpickled_message)
        print('processed message')
        self.final_converter.send_bulk_request_to_indexer(self.c)
        print('message to indexer sent')
        self.final_converter.flush_all()
        print('final_converter_flushed')

    def on_disconnected(self):
        print('disconnected')
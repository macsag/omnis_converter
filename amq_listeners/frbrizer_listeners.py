import ujson
import stomp


class FRBRizerListener(stomp.ConnectionListener):
    def __init__(self,
                 frbrizer,
                 c):

        self.frbrizer = frbrizer
        self.c = c

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        print('received a message "%s"' % message)
        dict_message = ujson.loads(message)
        print('received a message "%s"' % dict_message)
        self.frbrizer.frbrize_from_message(dict_message)
        print('processed message')
        # self.frbrizer.send_to_final_converter(self.c)
        # print('message to indexer sent')
        # self.frbrizer.flush_all()
        # print('final_converter_flushed')

    def on_disconnected(self):
        print('disconnected')

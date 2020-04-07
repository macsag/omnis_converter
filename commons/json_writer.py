from objects.helper_objects import ObjCounter


class JsonBufferOut(object):
    def __init__(self, item_file_out, manif_file_out, expr_file_out,
                 work_file_out, expr_data_file_out, work_data_file_out):
        self.item_buffer = [item_file_out, ObjCounter(), []]
        self.manif_buffer = [manif_file_out, ObjCounter(), []]
        self.expr_buffer = [expr_file_out, ObjCounter(), []]
        self.work_buffer = [work_file_out, ObjCounter(), []]
        self.expr_data_buffer = [expr_data_file_out, ObjCounter(), []]
        self.work_data_buffer = [work_data_file_out, ObjCounter(), []]

    def flush(self):
        for b in self.__dict__.values():
            with open(b[0], 'a', encoding='utf-8') as fp:
                for line in b[2]:
                    fp.write(f'{line}\n')


def write_to_json(json_line, json_buffer_out: JsonBufferOut, buffer):
    buff = getattr(json_buffer_out, buffer)

    buff[2].append(json_line)
    buff[1].add(1)

    if buff[1].count % 1000 == 0:
        with open(buff[0], 'a', encoding='utf-8') as fp:
            for line in buff[2]:
                fp.write(f'{line}\n')
        buff[2] = []

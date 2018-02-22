import csv


class SampleSheetLoader:
    BEGIN = 0
    HEADER = 1
    READS = 2
    SETTINGS = 3
    DATA_HEADER = 4
    DATA = 5

    def __init__(self, filename):
        state = SampleSheetLoader.BEGIN
        headers = {}
        reads = {}
        settings = {}
        data_header = []
        data_values = []
        with open(filename, "r") as f:
            for params in csv.reader(f):
                if params[0] == "[Header]":
                    state = SampleSheetLoader.HEADER
                    continue
                if params[0] == "[Reads]":
                    state = SampleSheetLoader.READS 
                    continue
                if params[0] == "[Settings]":
                    state = SampleSheetLoader.SETTINGS 
                    continue
                if params[0] == "[Data]":
                    state = SampleSheetLoader.DATA_HEADER
                    continue
                if state == SampleSheetLoader.HEADER:
                    if len(params[0].strip()) > 0:
                        headers[params[0]] = params[1:]
                elif state == SampleSheetLoader.READS:
                    if len(params[0].strip()) > 0:
                        reads[params[0]] = params[1:]
                elif state == SampleSheetLoader.SETTINGS:
                    if len(params[0].strip()) > 0:
                        settings[params[0]] = params[1:]
                elif state == SampleSheetLoader.DATA_HEADER:
                    data_header = params
                    state = SampleSheetLoader.DATA
                elif state == SampleSheetLoader.DATA:
                    data_values.append(params)
                else:
                    print("Error!")
        self.data_header = data_header
        self.data_values = data_values
        self.data = [dict(zip(self.data_header, values)) for values in self.data_values]
    

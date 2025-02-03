class CsvWriter:
    def __init__(self, filepath):
        self.filepath = filepath

    def __enter__(self):
        self.f = open(self.filepath, "at")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.close()

    def write(self, *row):
        line = ",".join(row)
        self.f.write(f"{line}\n")

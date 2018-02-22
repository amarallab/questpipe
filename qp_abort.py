import questpipe as qp
from os.path import expanduser
import sys

def main(pipeline_name):
    pipeline = qp.Pipeline.load_state(pipeline_name)
    result = pipeline.abort()
    pipeline.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("E: {} <pipeline_name>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(-1)

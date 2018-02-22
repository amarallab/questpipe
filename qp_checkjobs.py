import questpipe as qp
from os.path import expanduser
import sys


def main(pipeline_name):
    pipeline = qp.Pipeline.load_state(pipeline_name)
    queue_count, running_count, completed_count = pipeline.checkjobs()
    print("Completed: {}".format(completed_count))
    print("Running:   {}".format(running_count))
    print("Idles:     {}".format(queue_count))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("E: {} <pipeline_name>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(-1)
    pipeline_name = sys.argv[1]
    main(pipeline_name)
from . import Pipeline
import time


def wait_for_pipeline(state_filename):
    pipeline = Pipeline.load_state(state_filename)
    while True:
        time.sleep(60)
        queue_count, running_count, completed_count = pipeline.checkjobs()
        if running_count == 0 and queue_count == 0:
            pipeline.log("I: Completed!")
            break
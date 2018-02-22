import questpipe as qp
from os.path import expanduser
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
run_name = "Result_{}".format(timestamp)

arguments = qp.Arguments(
    run_name=run_name,

    basedir=expanduser("~"),
    project_name="example_test",
    project_dir="{basedir}/{project_name}",
    rundir="{project_dir}/{run_name}"
)

with qp.Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments) as pipeline:
    pipeline.debug_to_filename("{rundir}/pipeline.log", create_parent_folders=True)
    pipeline.save_state("{rundir}/pipeline.json")
    print(pipeline.parse_string("The pipeline results are stored on {rundir}"))
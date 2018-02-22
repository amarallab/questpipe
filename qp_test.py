import questpipe as qp
from os.path import expanduser
import sys


def main(pipeline_name):
    arguments = qp.Arguments(
        msub_arguments=[
            "-A XXX",      # intentionally removed
            "-q XXX",      # intentionally removed
            "-l walltime=24:00:00,nodes=1:ppn=8",
            "-m a",
            "-j oe",
            "-W umask=0113",
            "-N {job_name}"],
        basedir=expanduser("~/{}".format(pipeline_name)),
        workdir="{basedir}",
        outdir="{basedir}",
        errdir="{basedir}"
    )

    with qp.Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments) as pipeline:
        pipeline.debug_to_filename("{outdir}/pipeline.log", create_parent_folders=True)

        t1 = pipeline.create_job(name="first_task")
        t1.prepare_async_run("""
            sleep 5
            echo {job_name} >> test_file
            """)

        t2 = pipeline.create_job(name="second_task")
        t2.async_run("""
            sleep 10
            echo {job_name} >> test_file
            """)

        t3 = pipeline.create_job(name="third_task", dependences=[t1, t2])
        t3.async_run("""
            sleep 1
            echo {job_name} >> test_file
            """)

        t1.unhold()

        pipeline.save_state("{outdir}/pipeline.json")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("E: {} <pipeline_name>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(-1)
    pipeline_name = sys.argv[1]
    main(pipeline_name)
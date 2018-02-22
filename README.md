# QUESTPIPE

Submit jobs to Quest MOAB scheduler integrating it in your awesome python code.


## Install

* clone the code on your computer.
* create a virtualenv using Python 3.2 or newer.
* install the packages listed on requirements.txt

## Structure

After cloning the repo, the questpipe code will be stored on your computer, but, also, it should be on your Quest home. Be careful, everytime you
change the code (pull code from github, modify your pipeline...), you should synchronize your local copy and the
Quest home code.

When you are running your code on Quest, all the data should be on your Quest home or accessible online. Remember, the code you
are running on Quest cannot access to your computer local data. On the other hand, your results will be stored on Quest
and you should copy it back to your computer.


## Run Fabric

"Fabric [1] provides a basic suite of operations for executing local or remote shell commands and
uploading/downloading files".

Questpipe uses Fabric to connect to the Quest SSH console and runs the python code to start your
pipeline, check if your jobs finished, or to cancel them.

[1] http://www.fabfile.org

Fabric accepts this global arguments:

* -u &lt;username&gt;
* -H &lt;host&gt;

Fabric has these tasks:

* load_quest: loads the data to connect to the server. The parameter is the source of the 'questpipe' folder. You
  should use this parameter in order to run a pipeline.

* sync: copies (rsync) the files changed on the local folder and the questpipe folder:

    `
    fab -u <username> load_quest:~/src/questpipe sync
    `

* qb_run: runs the pipeline of the argument.

    `
    fab -u <username> load_quest:~/src/questpipe qp_run:my_pipeline,my_arg1,my_arg2,my_arg3...
    `

* checkjobs: checks if the jobs of the pipeline are completed.

    `
    fab -u <username> load_quest:~/src/questpipe qp_checkjobs:file_of_the_pipeline
    `

* abort_pipeline: aborts all the jobs of the pipeline:

    `
    fab -u <username> load_quest:~/src/questpipe qp_abort:file_of_the_pipeline
    `

Note: you can run two or more tasks in a single call:

    fab -u <username> load_quest:~/src/questpipe sync qp_run:test,example


## Connection to Quest

Fabric uses SSH Key to connect to quest. To do this, create or use a SSH key pair of your computer and copy the public key to your ~/.ssh/authorized_keys (see manual).

## Examples of pipelines

Just a few examples.

### Example 1: just a pipeline

In this example, a pipeline is created using different parameters. It is not creating jobs:

    # filename: simple_pipeline.py
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

NOTE: You should use the fullpath to this file when you call the `qb_run` task and copy this file 
      to your quest folder. Otherwise, you can save this file in the questpipe folder and sync it
      using the `sync` task. 

To run the pipeline:

    fab -u <user> load_quest:~/src/questpipe sync qb_run:simple_pipeline

The console message will be something like:

    The pipeline results are stored on /home/<user>/example_test/Result_20171012_134540

And, to check if the jobs are done:

    fab -u <user> load_quest:~/src/questpipe sync qp_checkjobs:~/example_test/Result_20171012_134540

The result:

    [quest.northwestern.edu] out: Completed: 0
    [quest.northwestern.edu] out: Running:   0
    [quest.northwestern.edu] out: Idles:     0


### Example 2: Three tasks sync

The file `qp_test.py` is a simple pipeline that synchronize three tasks:

    import questpipe as qp
    from os.path import expanduser
    import sys


    def main(pipeline_name):
        arguments = qp.Arguments(
            msub_arguments=[
                #"-A xxx",      # intentionally removed
                #"-q xxx",    # intentionally removed
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

NOTE: Please, change the `-A` and `-q` parameters of the qp.Arguments object.

To run the pipeline:

    fab -u <user> load_quest:~/src/questpipe sync qb_run:qp_test,three_tasks

This pipeline creates a folder called "three_tasks" on your home folder and stores all the
data inside.

To check if the jobs are done:

    fab -u <user> load_quest:~/src/questpipe sync qp_checkjobs:~/three_tasks

The result:

    [quest.northwestern.edu] out: Completed: 0
    [quest.northwestern.edu] out: Running:   0
    [quest.northwestern.edu] out: Idles:     3

When the pipeline finishes, they will appear these files on the three_tasks folder:

* first_task.oNUMBER: output of the first task
* second_task.oNUMBER: output of the second task
* three_task.oNUMBER: output of the third task
* pipeline.log: log of the pipeline task creation process
* pipeline.json: the pipeline run data. This file contains the process ID for all the jobs created.
* test_file: text file where the three tasks wrote text in order. Should be:
    
    `first_task`

    `second_task`

    `third_task`


### Example 3: seq pipeline

Based on CETO pipeline [2], you can find the pipeline `seq_pipeline.py` that converts from BCL to FASQ files, creates FASTQC files, alignes, and creates a count file.

[2] https://github.com/ebartom/NGSbartom

## Version status

This code was used successfully aligning and analyzing mRNA sequences. It will be great if you
use it and give us your feedback. Also, you are really welcome to improve it!
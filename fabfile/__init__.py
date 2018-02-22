from fabric.api import cd, env, task, local, run, settings
from fabric.contrib.project import rsync_project
import questpipe as qp

import json


env.hosts = []

@task
def load_quest(src):
    env.hosts = ["quest.northwestern.edu"]
    env.environment = "quest"
    env.questpipe_folder = src


@task
def sync():
    with settings(user=env.user):
        run("mkdir -p {}".format(env.questpipe_folder))
        rsync_project(local_dir=".", remote_dir=env.questpipe_folder, exclude=[".git", "ssh_keys"])


@task
def qp_run(filename, *args):
    args = [json.dumps(arg)[1:-1] for arg in args]
    args = [('"{}"' if ' ' in arg else "{}").format(arg) for arg in args]
    args = " ".join(args)
    with settings(user=env.user), cd(env.questpipe_folder):
        if not filename.endswith(".py"):
            filename += ".py"
        run("module load python/anaconda3.6 ; python3.6 {} {}".format(filename, args))


@task
def qp_checkjobs(pipeline_name):
    with settings(user=env.user), cd(env.questpipe_folder):
        run("module load python/anaconda3.6 ; python3.6 qp_checkjobs.py {}".format(pipeline_name))


@task
def qp_abort(pipeline_name):
    with settings(user=env.user), cd(env.questpipe_folder):
        run("module load python/anaconda3.6 ; python3.6 qp_abort.py {}".format(pipeline_name))
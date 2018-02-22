import json
import os
import pathlib
import re
import subprocess


class Arguments:
    def __init__(self, **kwargs):
        self.values = kwargs

    @staticmethod
    def from_json(data):
        return Arguments(**data)

    def get(self, key, default_value):
        return self.values.get(key, default_value)

    def to_json(self):
        return self.values

    def combine(self, other):
        values = self.values.copy()
        if other is not None:
            for k, v in other.values.items():
                values[k] = v 
        return Arguments(**values)

    def __repr__(self):
        return self.values.__repr__()        


class MJob:
    CREATED = 0
    RUNNING = 1
    COMPLETED = 2

    def __init__(self, pipeline, name, msub_arguments, dependences, notokdependences, workdir, outdir, errdir, arguments, moab_job_name, moab_job_id, status, command):
        self.pipeline = pipeline
        self.name = name
        self.msub_arguments = msub_arguments
        self.dependences = dependences
        self.notokdependences = notokdependences
        self.workdir = workdir
        self.outdir = outdir
        self.errdir = errdir
        self.arguments = arguments
        self.moab_job_name = moab_job_name
        self.moab_job_id = moab_job_id
        self.status = status
        self.command = command

    @staticmethod
    def create_new(pipeline, name, msub_arguments, dependences, notokdependences, workdir, outdir, errdir, arguments):
        return MJob(pipeline, name, msub_arguments, dependences, notokdependences, workdir, outdir, errdir, arguments, None, None, MJob.CREATED, None)

    @staticmethod
    def from_json(pipeline, msub_arguments, data):
        return MJob(
            pipeline, 
            data["name"], 
            msub_arguments,
            [],   # fix dependences later
            [],   # fix notok dependences later
            data["workdir"],
            data["outdir"],
            data["errdir"],
            pipeline.arguments,
            data["moab_job_name"],
            data["moab_job_id"],
            data["status"],
            data["command"])

    def to_json(self):
        result = {
            "name": self.name,
            "workdir": self.workdir,
            "outdir": self.outdir,
            "errdir": self.errdir,
            "moab_job_name": self.moab_job_name,
            "moab_job_id": self.moab_job_id,
            "status": self.status,
            "command": self.command}
        if self.dependences is not None:
            result["dependences"] = [job.moab_job_id for job in self.dependences]
        if self.notokdependences is not None:
            result["notokdependences"] = [job.moab_job_id for job in self.notokdependences]
        return result

    def __parse_string(self, value, max_recursive_loops=10):
        for i in range(max_recursive_loops):
            new_value = value.format(job_name=self.name, **self.arguments.values)
            if new_value != value:
                value = new_value
            else:
                return new_value
        raise Exception("recursive parsing")

    def prepare_async_run(self, command, NUMBER_OF_ATTEMPTS=3):
        self.__async_run(command, hold=True, NUMBER_OF_ATTEMPTS=NUMBER_OF_ATTEMPTS)

    def async_run(self, command, NUMBER_OF_ATTEMPTS=3):
        self.__async_run(command, hold=False, NUMBER_OF_ATTEMPTS=NUMBER_OF_ATTEMPTS)

    def __async_run(self, command, hold, NUMBER_OF_ATTEMPTS=3):
        if self.status != MJob.CREATED:
            raise Exception("MJob is running")
        self.command = command
        eff_command = self.__parse_string(command)
        eff_msub_arguments = [self.__parse_string(arg) for arg in self.msub_arguments]
        if self.dependences is not None and len(self.dependences) > 0:
            self.pipeline.log("I: dep: {}".format([job.moab_job_id for job in self.dependences]))
            for mjob in self.dependences:
                if mjob.moab_job_id is None:
                    raise Exception("MJob must be running in order to be dependence")
            moab_job_ids = [mjob.moab_job_id for mjob in self.dependences]
            eff_msub_arguments.append("-l depend=afterok:{}".format(":".join(moab_job_ids)))

        if self.notokdependences is not None and len(self.notokdependences) > 0:
            self.pipeline.log("I: notok dep: {}".format([job.moab_job_id for job in self.notokdependences]))
            for mjob in self.notokdependences:
                if mjob.moab_job_id is None:
                    raise Exception("MJob must be running in order to be not ok dependence")
            moab_job_ids = [mjob.moab_job_id for mjob in self.notokdependences]
            eff_msub_arguments.append("-l depend=afternotok:{}".format(":".join(moab_job_ids)))

        workdir = self.__parse_string(self.workdir)
        errdir = self.__parse_string(self.errdir)
        outdir = self.__parse_string(self.outdir)

        eff_msub_arguments.append("-d \"{}\"".format(workdir))
        eff_msub_arguments.append("-e \"{}\"".format(errdir))
        eff_msub_arguments.append("-o \"{}\"".format(outdir))
        
        if hold:
            eff_msub_arguments.append("-h")
    
        for i in range(NUMBER_OF_ATTEMPTS):
            self.pipeline.log("I: msub attempt {}: {}".format(i + 1, eff_msub_arguments))
            stdin, stdout, stderr = self.pipeline.exec_command("msub", eff_msub_arguments, input=eff_command)
            result = len(stderr) == 0
            if result:
                self.moab_job_name = stdout.decode('utf8').strip()
                self.moab_job_id = self.moab_job_name.split(".")[0]
                self.status = MJob.RUNNING
                self.pipeline.log("I: Running {}".format(self.moab_job_id))
                break
        else:
            # when for exit is not because break
            raise Exception("Cannot start job after {} attemps: {}".format(NUMBER_OF_ATTEMPTS, stderr))
        return self

    def unhold(self):
        self.pipeline.log("I: unholding {}".format(self.moab_job_id))
        stdin, stdout, stderr = self.pipeline.exec_command("mjobctl", ["-u", "all", self.moab_job_id])
        result = len(stderr) == 0
        if result:
            self.status = MJob.RUNNING
            self.pipeline.log("I: Unhold {}".format(self.moab_job_id))
        else:
            self.pipeline.log("E: Error unholding job {}: {}".format(self.moab_job_id, stderr))
            self.status = MJob.CREATED       
        
    def cancel(self):
        self.pipeline.log("I: cancelling {}".format(self.moab_job_id))
        stdin, stdout, stderr = self.pipeline.exec_command("mjobctl", ["-c", self.moab_job_id])
        result = len(stderr) == 0
        if result:
            self.status = MJob.COMPLETED
            self.pipeline.log("I: Cancelled {}".format(self.moab_job_id))
        else:
            self.pipeline.log("E: Error cancelling job {}: {}".format(self.moab_job_id, stderr))
            self.status = MJob.CREATED       
        
    @property
    def is_running(self):
        if self.status in [MJob.CREATED, MJob.COMPLETED]:
            return False
        stdin, stdout, stderr = self.pipeline.exec_command("checkjob", ["-v {}".format(self.moab_job_id)])
        for line in stdout.splitlines():
            line = line.decode("utf8")
            if line.endswith('\n'):
                line = line[:-1]
            index = line.find(':')
            if index == -1:
                continue
            name = line[0:index].strip()
            if name != 'State':
                continue
            param = line[index+1:].strip()
            if param == "Completed":
                self.status = MJob.COMPLETED
                return False
            else:
                return True
        return False 


class Pipeline:
    def __init__(self, name, join_command_arguments=False, arguments=None, abort_jobs_on_exception=True):
        self.name = name
        self.join_command_arguments = join_command_arguments
        self.arguments = arguments if arguments is not None else Arguments({})
        self.jobs = []
        self.debug_file = None
        self.abort_jobs_on_exception = abort_jobs_on_exception

    def debug_to_filename(self, filename, create_parent_folders=False):
        if self.debug_file is not None:
            raise Exception("Cannot debug to more than one file")
        eff_filename = self.parse_string(filename)
        if create_parent_folders:
            parent = pathlib.Path(eff_filename).parent
            parent.mkdir(parents=True, exist_ok=True)

        self.debug_file = open(eff_filename, "wt")

    def log(self, str):
        if self.debug_file is not None:
            self.debug_file.write("{}\n".format(str))
            self.debug_file.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.debug_file is not None:
            self.debug_file.close()
        if exc_type is not None:
            if self.abort_jobs_on_exception:
                self.log("E: Aborting all the jobs (exception raised)")
                self.abort()
            return None
        return self

    @staticmethod
    def load_state(filename):
        if os.path.isdir(filename):
            filename = "{}/pipeline.json".format(filename)
        with open(filename, "rt") as f:
            data = json.loads(f.read())
        return Pipeline.from_json(data)

    @staticmethod
    def from_json(data):
        name = data["name"]
        join_command_arguments = data["join_command_arguments"]
        arguments = Arguments.from_json(data["arguments"])
        msub_arguments = arguments.get("msub_arguments", [])
        pipeline = Pipeline(name, join_command_arguments, arguments)
        jobs = [(d.get("dependences", []), d.get("notokdependences", []), MJob.from_json(pipeline, msub_arguments, d)) for d in data["jobs"]]
        job_ids = {job.moab_job_id: job for _, _, job in jobs}
        for dependences, notokdependences, job in jobs:
            job.dependences = [job_ids[dependence] for dependence in dependences]
            job.notokdependences = [job_ids[dependence] for dependence in notokdependences]
        pipeline.jobs = [job for (dependence, notokdependence, job) in jobs]
        return pipeline

    def to_json(self):
        return {
            "name": self.name,
            "join_command_arguments": self.join_command_arguments,
            "arguments": self.arguments.to_json(),
            "jobs": [job.to_json() for job in self.jobs]}

    def save_state(self, filename):
        eff_filename = self.parse_string(filename)
        with open(eff_filename, "wt") as f:
            f.write(json.dumps(
                self.to_json(), 
                sort_keys=True, 
                indent=4, 
                separators=(',', ': ')))
        return eff_filename

    def checkjobs(self):
        # If the job is not appearing on 'showq', it means, the job is done
        job_ids = [job.moab_job_id for job in self.jobs]

        QUEUE_STATES = "HQTWS"
        RUNNING_STATES = "R"

        queue_count = 0
        running_count = 0
        completed_count = 0

        p = subprocess.Popen("qstat", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()

        is_header = True
        for line in stdout.splitlines():
            line = line.decode("utf8")
            if line.endswith('\n'):
                line = line[:-1]
            line = line.strip()
            if len(line) == 0:
                continue
            if is_header:
                if re.match("^[- ]*$", line):
                    is_header = False
                continue

            line = re.sub("\s+", " ", line)
            params = line.split(" ")
            if len(params) < 5:
                continue
            job_id = params[0].split(".")[0]
            if job_id not in job_ids:
                continue
            state = params[4]
    
            if state in QUEUE_STATES:
                queue_count += 1
            elif state in RUNNING_STATES:
                running_count += 1
            else:
                completed_count += 1

        remain = len(job_ids) - (queue_count + running_count + completed_count)
        completed_count += remain
        return queue_count, running_count, completed_count

    def abort(self):
        job_states = {job.moab_job_id: MJob.COMPLETED for job in self.jobs}  
        for job in self.jobs:
            p = subprocess.Popen("mjobctl -c {}".format(job.moab_job_id), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()
            if len(stdout) > 0:
                self.log("I: abort says: {}".format(stdout))
            if len(stderr) > 0:
                self.log("E: abort failed: {}".format(stderr))

    def exec_command(self, command, command_arguments, input=None):
        if command_arguments is None:
            command_arguments = []
            
        if self.join_command_arguments:
            eff_command = " ".join([command] + command_arguments)
        else:
            eff_command = [command] + command_arguments

        self.log("I: {} / {}".format(eff_command, input if input is not None else "None"))
        if input is not None:
            p = subprocess.Popen(eff_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate(bytes(input, "utf-8"))
            return None, stdout, stderr
        else:
            p = subprocess.Popen(eff_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()
            return None, stdout, stderr

    def create_job(self, name, local_arguments=None, dependences=None, notokdependences=None, workdir=None, outdir=None, errdir=None):
        self.arguments = self.arguments.combine(local_arguments)
        msub_arguments = self.arguments.get("msub_arguments", [])
        if dependences is None:
            dependences = []
        if workdir is None:
            workdir = self.arguments.get("workdir", ".")
        if outdir is None:
            outdir = self.arguments.get("outdir", ".")
        if errdir is None:
            errdir = self.arguments.get("errdir", ".")
        job = MJob.create_new(self, name, msub_arguments, dependences, notokdependences, workdir, outdir, errdir, self.arguments)
        self.jobs.append(job)
        return job

    def parse_string(self, value, max_recursive_loops=10):
        for i in range(max_recursive_loops):
            new_value = value.format(**self.arguments.values)
            if new_value != value:
                value = new_value
            else:
                return new_value
        raise Exception("recursive parsing")

    def run(self, command):
        eff_command = self.parse_string(command)
        p = subprocess.Popen(eff_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()
        return None, stdout, stderr
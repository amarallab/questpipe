import questpipe as qp
import questpipe.illumina as qpi
import time


def main(project_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = "Result_{}".format(timestamp)

    arguments = qp.Arguments(
        num_processors=10,
        run_name=run_name,

        msub_arguments=[
            "-A XXX",
            "-q XXX",
            "-l walltime=24:00:00,nodes=1:ppn={num_processors}",
            "-m a",
            "-j oe",
            "-W umask=0113",
            "-N {job_name}"],

        basedir="/projects/XXXX/",
        project_id=project_id,
        project_name=project_name,
        project_dir="{basedir}/{project_name}",

        rundir="{project_dir}/{run_name}",
        illumina_csv_sheet="{project_dir}/SampleSheet.csv",

        tophat_read_mismatches=2,
        tophat_read_edit_dist=2,
        tophat_max_multihits=5,
        tophat_transcriptome_index="/projects/XXX/XXX.cuff",  # tophat uses GFF files and it does not like the extension :-(
        tophat_bowtie_index="/projects/XXX",
        quantification_transcriptome_index="/projects/XXX/XXX.cuff.gtf",

        workdir="{project_dir}/",
        outdir="{rundir}/logs/",
        errdir="{rundir}/logs/",
    )

    state_filename = run_pipeline(run_name, arguments=arguments)

    with qp.Pipeline(name=name, join_command_arguments=True, arguments=arguments) as pipeline:
        pipeline.debug_to_filename("{rundir}/pipeline.log", create_parent_folders=True)

        # STEP 1: Create the folders to store the data
        _, stdout, stderr = pipeline.run("""
            mkdir -p "{rundir}"
            mkdir -p "{rundir}/logs"
            mkdir -p "{rundir}/00_fastq"
            mkdir -p "{rundir}/01_fastqc"
            mkdir -p "{rundir}/02_trimmed"
            mkdir -p "{rundir}/03_fastqc"
            mkdir -p "{rundir}/04_alignment"
            mkdir -p "{rundir}/05_quantification"
            mkdir -p "{rundir}/06_eda"
        """)

        # STEP 2: Create fastq files
        t1 = pipeline.create_job(name="00_bcl2fastq")
        t1.prepare_async_run("""
            module load bcl2fastq/2.17.1.14
            bcl2fastq -R {project_dir} -r {num_processors} -d {num_processors} -p {num_processors} -w {num_processors}
            """)

        # STEP 3: Create the fastqc files from fastq
        step3_tasks = []
        sample_sheet_filename = pipeline.parse_string("{illumina_csv_sheet}")
        ssr = qpi.SampleSheetLoader(sample_sheet_filename)
        for index, data in enumerate(ssr.data):
            if data["Sample_Project"] != arguments.values["project_id"]:
                continue

            tasks = []
            fastq_filenames = []
            for line in [1, 2, 3, 4]:
                sample_filename = "{}_S{}_L{:03}_R1_001".format(data["Sample_Name"], index+1, line)
                fastq_filenames.append("{rundir}/02_trimmed/{sample_filename}.trimmed.fastq.gz")
                current_t = pipeline.create_job(
                    name="01_fastqc_{sample_filename}", 
                    dependences=[t1],
                    local_arguments=qp.Arguments(
                        sample_id=data["Sample_ID"],
                        sample_name=data["Sample_Name"],
                        sample_filename=sample_filename))

                current_t.async_run("""
                    module load fastqc/0.11.5
                    module load java
                    
                    cp {project_dir}/Data/Intensities/BaseCalls/{project_id}/{sample_id}/{sample_filename}.fastq.gz \
                        {rundir}/00_fastq
                    fastqc -o {rundir}/01_fastqc {rundir}/00_fastq/{sample_filename}.fastq.gz
                    java -jar /projects/b1038/tools/Trimmomatic-0.36/trimmomatic-0.36.jar SE \
                        -threads {num_processors} \
                        -phred33 {rundir}/00_fastq/{sample_filename}.fastq.gz \
                        {rundir}/02_trimmed/{sample_filename}.trimmed.fastq \
                        TRAILING:30 MINLEN:20 
                    gzip {rundir}/02_trimmed/{sample_filename}.trimmed.fastq
                    fastqc -o {rundir}/03_fastqc {rundir}/02_trimmed/{sample_filename}.trimmed.fastq.gz
                    """)
                tasks.append(current_t)
            
            # Run tophat
            tophat_t = pipeline.create_job(
                name="02_tophat_{sample_name}",
                dependences=tasks,
                local_arguments=qp.Arguments(
                    sample_name=data["Sample_Name"],
                    fastq_filenames=",".join(fastq_filenames)))

            tophat_t.async_run("""
                module load tophat/2.1.0
                module load samtools
                module load bowtie2/2.2.6
                module load boost
                module load gcc/4.8.3
                module load python

                tophat --no-novel-juncs \
                    --read-mismatches {tophat_read_mismatches} \
                    --read-edit-dist {tophat_read_edit_dist} \
                    --num-threads {num_processors} \
                    --max-multihits {tophat_max_multihits} \
                    --transcriptome-index {tophat_transcriptome_index} \
                    -o {rundir}/04_alignment/{sample_name} \
                    {tophat_bowtie_index} \
                    {fastq_filenames}
                ln -s {rundir}/04_alignment/{sample_name}/accepted_hits.bam {rundir}/04_alignment/{sample_name}.bam
                samtools index {rundir}/04_alignment/{sample_name}.bam
                htseq-count -f bam -q -m intersection-nonempty \
                    -s reverse -t exon -i gene_id \
                    {rundir}/04_alignment/{sample_name}.bam \
                    {quantification_transcriptome_index} \
                    > {rundir}/04_alignment/{sample_name}.htseq.counts
                """)
            step3_tasks.append(tophat_t)

        t4 = pipeline.create_job(
            name="03_alignment_report",
            dependences=step3_tasks)
        t4.async_run("""
            module load R/3.3.1

            Rscript /projects/p20742/tools/createTophatReport.R --topHatDir={rundir}/04_alignment/ --nClus={num_processors}
            """)

        t5 = pipeline.create_job(
            name="04_quantification",
            dependences=[t4])
        t5.async_run("""
            perl /projects/p20742/tools/makeHTseqCountsTable.pl {rundir}/04_alignment \
                {quantification_transcriptome_index} \
                {rundir}/05_quantification
            rm -f {project_dir}/latest
            ln -s {rundir} {project_dir}/latest
            """)

        t1.unhold()

        state_filename = pipeline.save_state("{rundir}/pipeline.json")
        print("Stored at {}".format(state_filename))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("E: {} <project_id>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(-1)
    pipeline_name = sys.argv[1]
    main(pipeline_name)

from spex_common.config import load_config
from spex_common.modules.logging import get_logger
from spex_common.modules.database import db_instance
from spex_common.services.Timer import every
import spex_common.services.Pipeline as PipelineService
import spex_common.services.Job as JobService
import logging

logger = get_logger("pipeline_manager")
collection = "pipeline"


def update_job_status(job_data, new_status):
    current_status = job_data.get("status")
    if current_status == new_status or current_status == 100:
        return
    if (new_status < 0 or new_status == 100) or current_status is None:
        job_id = job_data.get("id")
        update_data = {"status": new_status}
        logger.debug(f"Updating job {job_id} with {update_data}")
        JobService.update_job(job_id, update_data)


def get_minimum_task_status(tasks):
    return min([task.get("status", 0) for task in tasks if task.get("status") is not None])


def update_statuses_for_jobs(job_list):
    for job in job_list:
        if tasks := job.get("tasks"):
            min_task_status = get_minimum_task_status(tasks)
            if job.get('status') == 0 and min_task_status == -2:
                min_task_status = 0
            if job.get('status') != min_task_status and job.get('status') != 100:
                update_job_status(job, min_task_status)
        if child_jobs := job.get("jobs"):
            update_statuses_for_jobs(child_jobs)


def start_ready_jobs(job_list):
    for job in job_list:
        if job.get("status") == 100:
            if child_jobs := job.get("jobs"):
                for child_job in child_jobs:
                    if child_job.get("status") == -2:
                        update_job_status(child_job, 0)
                start_ready_jobs(child_jobs)


def is_nearly_equal(n1, n2, epsilon=1):
    return abs(n1 - n2) <= epsilon


def process_pipelines():
    logger.info("working")
    pipelines = db_instance().select(collection, "FILTER doc.complete <= 100")
    logger.info(f"Uncompleted pipelines: {len(pipelines)}")

    for pipeline in pipelines:
        pipeline_data = PipelineService.get_tree(pipeline["_key"])
        if not pipeline_data:
            continue

        first_level_jobs = pipeline_data[0].get("jobs", [])
        update_statuses_for_jobs(first_level_jobs)
        start_ready_jobs(first_level_jobs)

        job_ids = PipelineService.get_jobs(first_level_jobs)
        jobs_status_data = JobService.select_jobs(condition="in", _id=job_ids)

        if jobs_status_data:
            total_status = sum(job.get('status', 0) for job in jobs_status_data)
            average_status = int(round(total_status / len(jobs_status_data), 0))
            if not is_nearly_equal(pipeline_data[0].get("status", -100), average_status):
                PipelineService.update(
                    pipeline_data[0].get('id'),
                    data={"complete": average_status},
                    collection='pipeline'
                )
                logger.debug(f"Updated pipeline {pipeline['_key']} status to {average_status}")


if __name__ == "__main__":
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    load_config()
    every(10, process_pipelines)

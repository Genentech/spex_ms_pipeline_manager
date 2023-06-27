from spex_common.config import load_config
from spex_common.modules.logging import get_logger
from spex_common.modules.database import db_instance
from spex_common.services.Timer import every
import spex_common.services.Pipeline as PipelineService
import spex_common.services.Job as JobService
import logging

logger = get_logger("pipeline_manager")
collection = "pipeline"


def update_box_status(data, status: int = None):
    if data.get("status") == status or data.get("status") == 100:
        return

    job_id = data.get("id")
    data = {"status": status}
    logger.debug(f"job {job_id} updating {data}")
    JobService.update_job(job_id, data)


def check_update_status_job(jobs_list: list[dict]):

    for job in jobs_list:
        if tasks := job.get("tasks", []):
            tasks_status = min(
                [
                    task.get("status", 0)
                    for task in tasks if task.get("status") is not None
                ]
            )
            if job.get('status') == 0 and tasks_status == -2:
                tasks_status = 0
            if job.get('status') != tasks_status and job.get('status') != 100:
                update_box_status(job, tasks_status)
            if child_jobs := job.get("jobs", []):
                check_update_status_job(child_jobs)


def start_next_job(jobs_list: list[dict]):
    for job in jobs_list:
        if job.get("status") == 100:
            if child_jobs := job.get("jobs", []):
                for child_job in child_jobs:
                    if child_job.get("status") == -2 and child_job.get("status") != 100:
                        update_box_status(child_job, 0)
                start_next_job(child_jobs)
            else:
                return


def nearly_equal(n1, n2, epsilon=1):
    return abs(n1 - n2) <= epsilon


def get_box():
    logger.info("working")
    lines = db_instance().select(collection, " FILTER doc.complete <= 100")
    logger.info(f"uncompleted pipelines: {len(lines)}")
    for line in lines:
        logger.debug(f"processing pipeline: {line['_key']}")
        if data := PipelineService.get_tree(line["_key"]):

            check_update_status_job(data[0].get("jobs", []))
            start_next_job(data[0].get("jobs", []))
            job_ids = PipelineService.get_jobs(data[0].get('jobs', []))
            jobs = JobService.select_jobs(condition="in", _id=job_ids)
            pipeline_status = 0
            if jobs is not None:
                for job in jobs:
                    pipeline_status += job.get('status', 0)
                pipeline_status = int(round(pipeline_status / len(jobs), 0))
                if not nearly_equal(data[0].get("status", -100), pipeline_status):
                    updated = PipelineService.update(
                        data[0].get('id'),
                        data={"complete": pipeline_status},
                        collection='pipeline'
                    )
                    if updated:
                        logger.debug(f"processing pipeline: {line['_key']} changed status to: {pipeline_status}")


if __name__ == "__main__":
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    load_config()
    every(10, get_box)

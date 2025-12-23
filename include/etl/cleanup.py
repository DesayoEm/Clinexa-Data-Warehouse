import logging
from airflow.models.variable import Variable


class CleanUp:
    def __init__(self, context):
        self.context = context
        self.execution_date = self.context["ds"]
        self.log = logging.getLogger("airflow.task")

    def clear_all_checkpoints(self):
        """
        Clear all checkpoint variables for this DAG run after successful completion.
        Must be the last task to run in the DAG.
        """
        dag = self.context["dag"]

        task_ids = [task.task_id for task in dag.tasks]

        self.log.info(f"Starting checkpoint cleanup for {self.execution_date}...")
        self.log.info(f"DAG has {len(task_ids)} tasks")
        self.log.info(f"-------------")

        cleared_count = 0
        not_found_count = 0

        for task_id in task_ids:
            checkpoint_key = f"{task_id}_{self.execution_date}"
            try:
                Variable.delete(checkpoint_key)
                self.log.info(f"Cleared checkpoint: {checkpoint_key}")
                cleared_count += 1
            except KeyError:
                self.log.debug(f"No checkpoint for: {checkpoint_key}")
                not_found_count += 1
            except Exception as e:
                self.log.error(f"Failed to clear {checkpoint_key}: {e}")

        self.log.info(f"total tasks: {len(task_ids)}")
        self.log.info(f"Cleared: {cleared_count}")
        self.log.info(f"Not found: {not_found_count}")

        return {
            "execution_date": self.execution_date,
            "total_tasks": len(task_ids),
            "checkpoints_cleared": cleared_count,
            "checkpoints_not_found": not_found_count,
        }

# -*- coding: utf-8 -*-

from collections import defaultdict

from airflow.models import BaseOperator


class AbstractOperator(BaseOperator):

    def execute(self, **context):
        self._pre_execute(context)
        result = self._execute(**context)
        return self._post_execute(**context) or result

    def _pre_execute(self, context):
        """ Pre execute logic. This method is executed before the main logic in the 'execute' method
            Default approach should exists here like get task_config """

        context = dict(context.get('context', {}))

        # get default params
        self.task_instance = context.get('task_instance', None)
        self.depends_on = context.get('depends_on', None)

        # get task config from default kwargs (for tests and mocks)
        self.task_config = context.get('task_config', None)

        # if not pass task_config, lets get from task instance
        if not self.task_config and self.depends_on:
            self.task_config = defaultdict(list)
            for config in self.task_instance.xcom_pull(task_ids=self.depends_on):
                if config and isinstance(config, dict):
                    for key, value in config.items():
                        self.task_config[key].append(value)

    def _execute(self, **context):
        return None

    def _post_execute(self, **context):
        return None

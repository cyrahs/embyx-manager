"""The merge Job helpers of `merge/kube.py` (WP3): names, the Job built from the template, outcomes.

`job_name` is part of the merge Job contract (a DNS label of at most 52 characters that the
service looks Jobs up by); `build_job` fills the GitOps template in; `job_outcome` and
`job_failure` read a Job's status. The Rust port (`embyx_clients::kube`) must give the same
values and refuse a broken template with the same exception class and text.
"""

from __future__ import annotations

import copy
from typing import Any

from embyx_manager.merge import kube

_AVIDS = [
    'SQTEVR-009',
    'A' * 80,
    'X_Y.Z' * 20,
    'abc-001',
    '--ABC--001--',
    '中文-001',
    'İPZ-123',
    'ß-1',
    '',
    '---',
    'FC2-PPV-1234567',
    'a' * 33 + '-b',
    'a' * 34 + '-b',
    'a' * 35 + '-b',
]

_TASK_IDS = [1, 7, 12345, 0, -3, 10**15]

_TEMPLATE = {
    'apiVersion': 'batch/v1',
    'kind': 'Job',
    'metadata': {'labels': {'app': 'embyx-merge'}},
    'spec': {
        'backoffLimit': 0,
        'template': {
            'spec': {
                'containers': [
                    {'name': 'merge', 'command': ['embyx-manager', 'merge-worker'], 'image': 'placeholder'},
                    {'name': 'sidecar'},
                ]
            }
        },
    },
}


def _templates() -> list[Any]:
    bare = {'spec': {'template': {'spec': {'containers': [{}]}}}}
    labelled_pod = copy.deepcopy(_TEMPLATE)
    labelled_pod['spec']['template']['metadata'] = {'labels': {'app': 'pod'}, 'annotations': {'a': 'b'}}
    return [
        _TEMPLATE,
        bare,
        labelled_pod,
        {'metadata': {}, 'spec': {'template': {'spec': {'containers': [{'name': 'merge', 'args': ['old']}]}}}},
        {
            'metadata': {'name': 'old', 'labels': {kube.TASK_LABEL: 'old'}},
            'spec': {'template': {'spec': {'containers': [{}]}}},
        },
        {},
        {'spec': {}},
        {'spec': {'template': {}}},
        {'spec': {'template': {'spec': {}}}},
        {'spec': {'template': {'spec': {'containers': []}}}},
        {'spec': {'template': {'spec': {'containers': {}}}}},
        {'spec': {'template': {'spec': {'containers': 'x'}}}},
        {'metadata': None, 'spec': {'template': {'spec': {'containers': [{}]}}}},
        {'metadata': {'labels': None}, 'spec': {'template': {'spec': {'containers': [{}]}}}},
        {'spec': None},
        {'spec': [1]},
        {'spec': 'x'},
        {'spec': {'template': {'spec': 'x'}}},
        {'spec': {'template': {'spec': {'containers': [{}, 'x']}}}},
        {'spec': {'template': {'spec': {'containers': ['x']}}}},
        {'spec': {'template': {'metadata': 5, 'spec': {'containers': [{}]}}}},
        {'spec': {'template': {'spec': {'containers': [None]}}}},
        {'spec': {'template': {'spec': {'containers': [[1]]}}}},
        [],
    ]


def _job_names() -> list[dict[str, Any]]:
    return [
        {'task_id': task_id, 'avid': avid, 'name': kube.job_name(task_id, avid)}
        for task_id in _TASK_IDS
        for avid in _AVIDS
    ]


def _outcome(function: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return {'value': function(*args, **kwargs)}
    except Exception as exc:  # noqa: BLE001 - the class and text are the golden
        return {'error': {'type': type(exc).__name__, 'message': str(exc)}}


def _build_jobs() -> list[dict[str, Any]]:
    cases = []
    for template in _templates():
        before = copy.deepcopy(template)
        outcome = _outcome(
            kube.build_job,
            template,
            name='embyx-merge-1-abc',
            image='ghcr.io/cyrahs/embyx-manager@sha256:1',
            args=['--output', 'o', '中文'],
            task_id=1,
        )
        if template != before:
            msg = 'build_job changed its template'
            raise AssertionError(msg)
        cases.append({'template': template, **outcome})
    return cases


def _jobs() -> list[Any]:
    def condition(kind: str, status: Any, **extra: Any) -> dict[str, Any]:
        return {'type': kind, 'status': status, **extra}

    return [
        {},
        {'status': None},
        {'status': {}},
        {'status': {'conditions': [condition('Complete', 'True')]}},
        {'status': {'conditions': [condition('Failed', 'True')]}},
        {'status': {'conditions': [condition('Failed', 'True', reason='DeadlineExceeded')]}},
        {
            'status': {
                'conditions': [
                    condition(
                        'Failed',
                        'True',
                        reason='BackoffLimitExceeded',
                        message='Job has reached the specified backoff limit',
                    )
                ]
            }
        },
        {'status': {'conditions': [condition('Failed', 'True', message='', reason='R')]}},
        {'status': {'conditions': [condition('Failed', 'True', message=None, reason=None)]}},
        {'status': {'conditions': [condition('Failed', 'True', message=5)]}},
        {'status': {'conditions': [condition('Failed', 'False', message='not yet')]}},
        {'status': {'conditions': [condition('Complete', 'False'), condition('Failed', 'True', message='second')]}},
        {'status': {'conditions': [condition('Suspended', 'True'), condition('Complete', 'True')]}},
        {'status': {'conditions': [condition('Complete', status=True)]}},
        {'status': {'conditions': None, 'succeeded': 1}},
        {'status': {'conditions': [], 'failed': 1}},
        {'status': {'failed': 1}},
        {'status': {'failed': 0, 'succeeded': 0, 'active': 1}},
        {'status': {'active': 1}},
        {'status': {'succeeded': 2, 'failed': 1}},
        {'status': {'conditions': [condition('Failed', 'True')], 'succeeded': 1}},
        {'status': {'conditions': [condition('FailureTarget', 'True', message='target')], 'failed': 1}},
    ]


def _job_status() -> list[dict[str, Any]]:
    return [{'job': job, 'outcome': kube.job_outcome(job), 'failure': kube.job_failure(job)} for job in _jobs()]


GOLDENS = {
    'kube/job_names.json': _job_names,
    'kube/build_job.json': _build_jobs,
    'kube/job_status.json': _job_status,
}

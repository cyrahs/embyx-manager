"""The merge Jobs client (`merge/kube.py`), the merge Job contract's Kubernetes side (WP3).

`kube.steps` builds the baseline's `KubeJobs` against the fake Kubernetes API (plain HTTP,
no CA), with the Job template and the token read from the seeded `mount` root, then runs
`args.steps` in order: `create_job`, `get_job`, `delete_job`, `log_tail`, `own_image`, and
`write_template` (rewrites the template between steps, to show it is read for each Job). A
fetched Job is reported with its `job_outcome` and `job_failure`. An error is
`{type, message}` with `type(exc).__name__` and `str(exc)`.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from embyx_manager.merge.kube import KubeJobs, job_failure, job_outcome

if TYPE_CHECKING:
    from runner import Context


async def _run(kube: KubeJobs, ctx: Context, op: str, args: dict[str, Any]) -> Any:  # noqa: PLR0911 - one case per step
    match op:
        case 'create_job':
            return await kube.create_job(name=args['name'], args=args['args'], task_id=args['task_id'])
        case 'get_job':
            job = await kube.get_job(args['name'])
            if job is None:
                return None
            return {'job': job, 'outcome': job_outcome(job), 'failure': job_failure(job)}
        case 'delete_job':
            return await kube.delete_job(args['name'])
        case 'log_tail':
            return await kube.log_tail(args['name'], lines=args.get('lines', 20))
        case 'own_image':
            return await kube.own_image()
        case 'write_template':
            (ctx.roots['mount'] / 'job.json').write_text(json.dumps(args['template']), encoding='utf-8')
            return None
    msg = f'unknown step {op!r}'
    raise ValueError(msg)


async def steps(ctx: Context) -> Any:
    kube = KubeJobs(
        api_url=ctx.http_fakes['kubernetes'],
        namespace=ctx.args['namespace'],
        pod_name=ctx.args['pod_name'],
        template_path=ctx.roots['mount'] / 'job.json',
        token_path=ctx.roots['mount'] / 'token',
        ca_path=None,
    )
    reported = []
    try:
        for step in ctx.args['steps']:
            entry: dict[str, Any] = {'op': step['op']}
            try:
                entry['result'] = await _run(kube, ctx, step['op'], step.get('args', {}))
            except Exception as exc:  # noqa: BLE001 - each step's failure is part of the transcript
                entry['error'] = {'type': type(exc).__name__, 'message': str(exc)}
            reported.append(entry)
    finally:
        await kube.aclose()
    return {'steps': reported}


OPERATIONS = {'kube.steps': steps}

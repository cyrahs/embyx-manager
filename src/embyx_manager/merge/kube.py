"""Just enough of the Kubernetes API to run merge Jobs from inside the cluster.

The Job's shape lives in the GitOps repository as a template the deployment
mounts (``EMBYX_MANAGER_MERGE_JOB_TEMPLATE``); this module only names it, points
it at the image this pod runs, and passes the worker its arguments. The pod's
service account may create, read and delete Jobs and read pods in its own
namespace, nothing more.
"""

import copy
import json
import os
import re
import ssl
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx

SERVICE_ACCOUNT_DIR = Path('/var/run/secrets/kubernetes.io/serviceaccount')
TEMPLATE_ENV = 'EMBYX_MANAGER_MERGE_JOB_TEMPLATE'
REQUEST_TIMEOUT_SECONDS = 30.0
#: Job names are DNS labels; the Job controller appends to them for its pods.
MAX_JOB_NAME = 52
TASK_LABEL = 'embyx-manager/merge-task'


class KubeError(Exception):
    """The Kubernetes API refused or could not be reached."""


def job_name(task_id: int, avid: str) -> str:
    slug = re.sub(r'[^a-z0-9]+', '-', avid.lower()).strip('-')
    return f'embyx-merge-{task_id}-{slug}'[:MAX_JOB_NAME].rstrip('-')


def build_job(template: dict[str, Any], *, name: str, image: str, args: Sequence[str], task_id: int) -> dict[str, Any]:
    """The template with this run's name, image, arguments and task label filled in."""
    job = copy.deepcopy(template)
    metadata = job.setdefault('metadata', {})
    metadata['name'] = name
    metadata.setdefault('labels', {})[TASK_LABEL] = str(task_id)
    pod = job['spec']['template']
    pod.setdefault('metadata', {}).setdefault('labels', {})[TASK_LABEL] = str(task_id)
    container = pod['spec']['containers'][0]
    container['image'] = image
    container['args'] = list(args)
    return job


def job_outcome(job: dict[str, Any]) -> str:
    """``succeeded``, ``failed`` or ``running`` from a Job's status."""
    status = job.get('status') or {}
    for condition in status.get('conditions') or ():
        if condition.get('status') != 'True':
            continue
        if condition.get('type') == 'Complete':
            return 'succeeded'
        if condition.get('type') == 'Failed':
            return 'failed'
    if status.get('succeeded'):
        return 'succeeded'
    if status.get('failed'):
        return 'failed'
    return 'running'


class KubeJobs:
    def __init__(  # noqa: PLR0913
        self,
        *,
        api_url: str,
        namespace: str,
        pod_name: str,
        template_path: Path,
        token_path: Path,
        ca_path: Path | None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._namespace = namespace
        self._pod_name = pod_name
        self._template_path = template_path
        self._token_path = token_path
        self._image: str | None = None
        verify: ssl.SSLContext | bool = ssl.create_default_context(cafile=str(ca_path)) if ca_path else True
        self._client = httpx.AsyncClient(
            base_url=api_url,
            verify=verify,
            timeout=REQUEST_TIMEOUT_SECONDS,
            transport=transport,
        )

    @classmethod
    def from_environment(cls) -> 'KubeJobs | None':
        """The in-cluster client, or None outside a cluster or without a mounted template."""
        template = os.environ.get(TEMPLATE_ENV)
        host = os.environ.get('KUBERNETES_SERVICE_HOST')
        port = os.environ.get('KUBERNETES_SERVICE_PORT', '443')
        pod_name = os.environ.get('POD_NAME')
        token_path = SERVICE_ACCOUNT_DIR / 'token'
        if not (template and host and pod_name and token_path.exists()):
            return None
        namespace = os.environ.get('POD_NAMESPACE') or (SERVICE_ACCOUNT_DIR / 'namespace').read_text().strip()
        if ':' in host:
            host = f'[{host}]'
        ca_path = SERVICE_ACCOUNT_DIR / 'ca.crt'
        return cls(
            api_url=f'https://{host}:{port}',
            namespace=namespace,
            pod_name=pod_name,
            template_path=Path(template),
            token_path=token_path,
            ca_path=ca_path if ca_path.exists() else None,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    def template(self) -> dict[str, Any]:
        """Read on every Job so a template change applies without a restart."""
        return json.loads(self._template_path.read_text(encoding='utf-8'))

    async def own_image(self) -> str:
        """The exact image this pod runs, by digest, so the Job runs the same code."""
        if self._image is None:
            pod = await self._request('GET', f'/api/v1/namespaces/{self._namespace}/pods/{self._pod_name}')
            statuses = (pod.get('status') or {}).get('containerStatuses') or []
            image_id = str(statuses[0].get('imageID', '')) if statuses else ''
            image_id = image_id.removeprefix('docker-pullable://')
            self._image = image_id if '@sha256:' in image_id else str(pod['spec']['containers'][0]['image'])
        return self._image

    async def create_job(self, *, name: str, args: Sequence[str], task_id: int) -> None:
        """Start the Job; one that already exists under the name counts as started."""
        job = build_job(self.template(), name=name, image=await self.own_image(), args=args, task_id=task_id)
        await self._request(
            'POST',
            f'/apis/batch/v1/namespaces/{self._namespace}/jobs',
            json=job,
            accept=(409,),
        )

    async def get_job(self, name: str) -> dict[str, Any] | None:
        return await self._request('GET', f'/apis/batch/v1/namespaces/{self._namespace}/jobs/{name}', accept=(404,))

    async def delete_job(self, name: str) -> None:
        await self._request(
            'DELETE',
            f'/apis/batch/v1/namespaces/{self._namespace}/jobs/{name}',
            json={'propagationPolicy': 'Background'},
            accept=(404,),
        )

    async def log_tail(self, name: str, *, lines: int = 20) -> str:
        """The last lines the Job's pod printed, or '' when there is no pod to ask."""
        pods = await self._request(
            'GET',
            f'/api/v1/namespaces/{self._namespace}/pods',
            params={'labelSelector': f'job-name={name}'},
        )
        items = (pods or {}).get('items') or []
        if not items:
            return ''
        pod = items[-1]['metadata']['name']
        response = await self._send(
            'GET',
            f'/api/v1/namespaces/{self._namespace}/pods/{pod}/log',
            params={'tailLines': str(lines)},
        )
        return response.text.strip() if response.is_success else ''

    async def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            token = self._token_path.read_text().strip()
        except OSError as exc:
            msg = f'cannot read the service account token: {exc}'
            raise KubeError(msg) from exc
        try:
            return await self._client.request(method, path, headers={'Authorization': f'Bearer {token}'}, **kwargs)
        except httpx.HTTPError as exc:
            msg = f'Kubernetes API unreachable: {exc}'
            raise KubeError(msg) from exc

    async def _request(
        self,
        method: str,
        path: str,
        *,
        accept: Sequence[int] = (),
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        response = await self._send(method, path, **kwargs)
        if response.status_code in accept:
            return None
        if not response.is_success:
            msg = f'Kubernetes API {method} {path} answered {response.status_code}: {response.text[:300]}'
            raise KubeError(msg)
        return response.json()

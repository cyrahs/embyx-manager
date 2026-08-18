import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from embyx_manager.api import create_app, make_mutation_auth
from embyx_manager.fill_actor.api import (
    ActorFeedView,
    JobProgressView,
    create_fill_actor_router,
    fill_actor_health,
    fill_actor_lifespan,
    handle_fill_actor_error,
)
from embyx_manager.fill_actor.errors import FillActorError
from embyx_manager.fill_actor.feeds import RSSHubFeedWarmer
from embyx_manager.fill_actor.jobs import FillActorJobManager
from embyx_manager.fill_actor.persistence import (
    JOB_CANCELLED_ERROR_CODE,
    JobFeedRecord,
    JobFeedState,
    JobOperation,
    JobProgress,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
    MemoryFillActorRepository,
)
from embyx_manager.fill_actor.ports import AcquisitionOutcome
from embyx_manager.fill_actor.service import (
    FillActorPaths,
    FillActorRuntime,
    FillActorService,
    static_runtime,
)
from embyx_manager.fill_actor.subscriptions import SubscribedActor


class ActorCatalog:
    async def list_video_ids(self, _actor_id: str) -> list[str]:
        return ['ABC-001']


class BlockingActorCatalog:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    async def list_video_ids(self, _actor_id: str) -> list[str]:
        self.started.set()
        while not self.release.is_set():  # noqa: ASYNC110 - bridges TestClient's thread and event loop
            await asyncio.sleep(0.005)
        return ['ABC-001']


class AcquisitionGateway:
    async def submit_missing(self, _video_id: str) -> AcquisitionOutcome:
        return AcquisitionOutcome.NO_MAGNET


class BrandResolver:
    def resolve_brand(self, _video_id: str) -> str:
        return 'ABC'


def make_client(
    tmp_path: Path,
    *,
    api_token: str | None = None,
    max_request_bytes: int = 65_536,
    actor_catalog=None,
    feed_warmer_factory=None,
    freshrss_url: str | None = None,
    freshrss_rsshub_url: str | None = None,
    existing_actor_lookup=None,
    avid_actor_lookup=None,
    apply_enabled: bool = True,
) -> tuple[TestClient, FillActorPaths, MemoryFillActorRepository]:
    paths = FillActorPaths.from_iterable(
        actor_brand_path=tmp_path / 'actor',
        additional_brand_paths=(tmp_path / 'additional',),
        move_in_path=tmp_path / 'move-in',
    )
    for path in (paths.actor_brand_path, *paths.additional_brand_paths, paths.move_in_path):
        path.mkdir()
    repository = MemoryFillActorRepository()
    service = FillActorService(
        runtime=static_runtime(
            paths=paths,
            apply_enabled=apply_enabled,
        ),
        actor_catalog=actor_catalog or ActorCatalog(),
        acquisition_gateway=AcquisitionGateway(),
        brand_resolver=BrandResolver(),
        repository=repository,
    )
    feed_warmer = feed_warmer_factory(repository) if feed_warmer_factory is not None else None
    jobs = FillActorJobManager(service=service, repository=repository, feed_warmer=feed_warmer)
    app = create_app(
        app_ready=repository.health_check,
        routers=(
            create_fill_actor_router(
                service=service,
                repository=repository,
                jobs=jobs,
                mutation_auth=make_mutation_auth(api_token),
                freshrss_url=freshrss_url,
                freshrss_rsshub_url=freshrss_rsshub_url,
                existing_actor_lookup=existing_actor_lookup,
                avid_actor_lookup=avid_actor_lookup,
            ),
        ),
        feature_health={'fill_actor': fill_actor_health(service=service, repository=repository)},
        exception_handlers={FillActorError: handle_fill_actor_error},
        lifespans=(fill_actor_lifespan(service=service, repository=repository, jobs=jobs),),
        api_token=api_token,
        max_request_bytes=max_request_bytes,
    )
    return TestClient(app), paths, repository


def test_resolves_an_avid_to_javbus_actors(tmp_path: Path) -> None:
    looked_up: list[str] = []

    async def avid_actor_lookup(avid: str):
        looked_up.append(avid)
        return (('a123', '演员甲'), ('B456', '演员乙'))

    client, _, _ = make_client(tmp_path, avid_actor_lookup=avid_actor_lookup)
    with client:
        response = client.post('/api/fill-actor/avid-actors', json={'avid': ' abc-123 '})

    assert response.status_code == 200
    assert response.json() == {
        'avid': 'ABC-123',
        'actors': [
            {'actor_id': 'a123', 'name': '演员甲'},
            {'actor_id': 'B456', 'name': '演员乙'},
        ],
    }
    assert looked_up == ['ABC-123']


def test_reports_when_javbus_has_no_actors_for_an_avid(tmp_path: Path) -> None:
    async def avid_actor_lookup(_avid: str):
        return ()

    client, _, _ = make_client(tmp_path, avid_actor_lookup=avid_actor_lookup)
    with client:
        response = client.post('/api/fill-actor/avid-actors', json={'avid': 'ABC-123'})

    assert response.status_code == 404
    assert response.json() == {'error': {'code': 'avid_actors_not_found'}}


def wait_for_plan(client: TestClient, plan_id: str) -> dict:
    for _ in range(100):
        response = client.get(f'/api/fill-actor/plans/{plan_id}')
        payload = response.json()
        if payload['job']['state'] not in {'queued', 'running'}:
            return payload
        time.sleep(0.005)
    pytest.fail('plan job did not complete')


def wait_for_apply_job(client: TestClient, job_id: str) -> dict:
    for _ in range(100):
        response = client.get(f'/api/fill-actor/apply-jobs/{job_id}')
        assert response.status_code == 200
        payload = response.json()
        if payload['job']['state'] not in {'queued', 'running'}:
            return payload
        time.sleep(0.005)
    pytest.fail('apply job did not complete')


def test_plan_checks_freshrss_subscriptions_before_readiness_and_requires_confirmation(tmp_path: Path) -> None:
    lookups: list[tuple[str, ...]] = []

    async def existing_actor_lookup(actor_ids):
        lookups.append(tuple(actor_ids))
        return (SubscribedActor(actor_id='ACTOR', actor_name='演员甲'),)

    client, paths, _ = make_client(tmp_path, existing_actor_lookup=existing_actor_lookup)
    paths.move_in_path.rmdir()

    with client:
        conflict = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        continued = client.post(
            '/api/fill-actor/plans',
            json={'actor_ids': ['actor'], 'continue_if_subscribed': True},
        )

    assert conflict.status_code == 409
    assert conflict.json() == {
        'error': {
            'code': 'actors_already_subscribed',
            'actor_ids': ['actor'],
            'actors': [{'actor_id': 'actor', 'actor_name': '演员甲'}],
        }
    }
    assert continued.status_code == 503
    assert continued.json() == {'error': {'code': 'not_ready'}}
    assert lookups == [('actor',)]


def test_plan_reports_freshrss_subscription_check_failure_without_starting_scan(tmp_path: Path) -> None:
    async def failing_lookup(_actor_ids):
        msg = 'FreshRSS unavailable'
        raise RuntimeError(msg)

    client, _, _ = make_client(tmp_path, existing_actor_lookup=failing_lookup)

    with client:
        response = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})

    assert response.status_code == 502
    assert response.json() == {'error': {'code': 'freshrss_subscription_check_failed'}}


def test_plan_job_and_apply_end_to_end(tmp_path: Path) -> None:
    client, paths, _ = make_client(tmp_path)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')

    with client:
        response = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        assert response.status_code == 202
        assert response.headers['cache-control'] == 'no-store'
        assert response.json()['job']['progress']['stage'] == 'queued'
        assert response.json()['job']['progress']['total'] == 1
        plan_id = response.json()['job']['plan_id']
        payload = wait_for_plan(client, plan_id)
        assert payload['job']['state'] == 'completed'
        assert payload['job']['progress']['stage'] == 'done'
        assert payload['job']['progress']['eta_seconds'] == 0
        candidate = payload['plan']['videos'][0]['move_candidates'][0]

        applied = client.post(
            f'/api/fill-actor/plans/{plan_id}/apply',
            json={
                'revision': payload['plan']['revision'],
                'candidate_ids': [candidate['candidate_id']],
            },
        )

    assert applied.status_code == 200
    assert applied.json()['state'] == 'succeeded'
    assert applied.json()['results'][0]['state'] == 'moved'
    assert not source.exists()
    assert (paths.move_in_path / source.name).read_bytes() == b'video'


def test_persistent_apply_job_moves_serially_and_is_idempotent(tmp_path: Path) -> None:
    client, paths, _ = make_client(tmp_path)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    request_id = 'apply-request-0001'

    with client:
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        plan_id = created.json()['job']['plan_id']
        plan = wait_for_plan(client, plan_id)['plan']
        candidate_id = plan['videos'][0]['move_candidates'][0]['candidate_id']
        request = {
            'revision': plan['revision'],
            'candidate_ids': [candidate_id, candidate_id],
            'request_id': request_id,
        }

        started = client.post(f'/api/fill-actor/plans/{plan_id}/apply-jobs', json=request)
        terminal = wait_for_apply_job(client, request_id)
        repeated = client.post(f'/api/fill-actor/plans/{plan_id}/apply-jobs', json=request)
        conflict = client.post(
            f'/api/fill-actor/plans/{plan_id}/apply-jobs',
            json={**request, 'candidate_ids': []},
        )
        cancelled = client.post(f'/api/fill-actor/plans/{request_id}/cancel')

    assert started.status_code == 202
    assert started.json()['job']['operation'] == 'apply'
    assert terminal['job']['state'] == 'completed'
    assert terminal['job']['progress']['completed'] == 1
    assert terminal['job']['progress']['total'] == 1
    assert terminal['job']['progress']['current'] is None
    assert terminal['result']['state'] == 'succeeded'
    assert terminal['result']['results'][0]['state'] == 'moved'
    assert repeated.status_code == 202
    assert repeated.json() == terminal
    assert conflict.status_code == 409
    assert conflict.json() == {'error': {'code': 'apply_request_conflict'}}
    assert cancelled.status_code == 409
    assert cancelled.json() == {'error': {'code': 'apply_job_not_cancellable'}}
    assert not source.exists()
    assert (paths.move_in_path / source.name).read_bytes() == b'video'


def test_empty_apply_job_recovers_by_id_when_readiness_changes(tmp_path: Path) -> None:
    client, paths, _ = make_client(tmp_path)
    request_id = 'empty-apply-request-0001'
    with client:
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        plan_id = created.json()['job']['plan_id']
        plan = wait_for_plan(client, plan_id)['plan']
        request = {'revision': plan['revision'], 'candidate_ids': [], 'request_id': request_id}
        started = client.post(f'/api/fill-actor/plans/{plan_id}/apply-jobs', json=request)
        terminal = wait_for_apply_job(client, request_id)
        paths.move_in_path.rmdir()

        recovered = client.get(f'/api/fill-actor/apply-jobs/{request_id}')
        repeated = client.post(f'/api/fill-actor/plans/{plan_id}/apply-jobs', json=request)
        unavailable = client.post(
            f'/api/fill-actor/plans/{plan_id}/apply-jobs',
            json={**request, 'request_id': 'empty-apply-request-0002'},
        )

    assert started.status_code == 202
    assert terminal['job']['state'] == 'completed'
    assert terminal['job']['progress']['completed'] == 0
    assert terminal['job']['progress']['total'] == 0
    assert terminal['job']['progress']['percent'] == 100.0
    assert terminal['result']['state'] == 'succeeded'
    assert terminal['result']['results'] == []
    assert recovered.status_code == 200
    assert recovered.json() == terminal
    assert repeated.status_code == 202
    assert repeated.json() == terminal
    assert unavailable.status_code == 503
    assert unavailable.json() == {'error': {'code': 'not_ready'}}


def test_apply_job_requires_a_published_create_plan(tmp_path: Path) -> None:
    client, _, repository = make_client(tmp_path)
    with client:
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        plan_id = created.json()['job']['plan_id']
        plan = wait_for_plan(client, plan_id)['plan']
        assert client.portal is not None
        assert client.portal.call(repository.delete_plan, plan_id)

        missing = client.post(
            f'/api/fill-actor/plans/{plan_id}/apply-jobs',
            json={
                'revision': plan['revision'],
                'candidate_ids': [],
                'request_id': 'missing-plan-request-0001',
            },
        )

    assert missing.status_code == 404
    assert missing.json() == {'error': {'code': 'unknown_plan'}}


def test_apply_job_does_not_accept_an_apply_operation_as_its_parent(tmp_path: Path) -> None:
    client, _, repository = make_client(tmp_path)
    now = datetime.now(UTC)
    impostor = JobRecord(
        job_id='not-a-create-plan',
        plan_id='some-parent',
        operation=JobOperation.APPLY,
        state=JobState.COMPLETED,
        created_at=now,
        updated_at=now,
    )
    with client:
        assert client.portal is not None
        client.portal.call(repository.save_job, impostor)
        response = client.post(
            f'/api/fill-actor/plans/{impostor.job_id}/apply-jobs',
            json={
                'revision': 'revision',
                'candidate_ids': [],
                'request_id': 'operation-check-request-0001',
            },
        )

    assert response.status_code == 404
    assert response.json() == {'error': {'code': 'unknown_plan'}}


def test_plan_envelope_exposes_persisted_feed_status_and_freshrss_action(tmp_path: Path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={'content-type': 'application/xml', 'rsshub-cache-status': 'HIT'},
            content=b'<rss version="2.0"><channel /></rss>' if request.method == 'GET' else b'',
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def feed_warmer_factory(repository):
        return RSSHubFeedWarmer(
            repository=repository,
            rsshub_url='http://rsshub.internal.test',
            freshrss_url='https://freshrss.example.test',
            freshrss_rsshub_url='https://rsshub.example.test',
            client=http_client,
            poll_interval=0.001,
        )

    client, _, _ = make_client(
        tmp_path,
        feed_warmer_factory=feed_warmer_factory,
        freshrss_url='https://freshrss.example.test',
        freshrss_rsshub_url='https://rsshub.example.test',
        apply_enabled=False,
    )
    with client:
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        assert created.status_code == 202
        assert created.json()['feeds'][0]['actor_id'] == 'actor'
        plan_id = created.json()['job']['plan_id']
        payload = wait_for_plan(client, plan_id)

    assert payload['job']['state'] == 'completed'
    assert payload['feeds'] == [
        {
            'actor_id': 'actor',
            'state': 'ready',
            'attempts': 2,
            'updated_at': payload['feeds'][0]['updated_at'],
            'error_code': None,
            'freshrss_add_url': (
                'https://freshrss.example.test/i/?c=feed&a=add&url_rss='
                'https%3A%2F%2Frsshub.example.test%2Fjavbus%2Fstar%2Factor'
            ),
            'freshrss_url': 'https://freshrss.example.test',
        }
    ]


def test_feed_view_rebuilds_legacy_add_url_from_current_configuration() -> None:
    updated_at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    record = JobFeedRecord(
        job_id='legacy-job',
        actor_id='actor',
        state=JobFeedState.READY,
        attempts=2,
        updated_at=updated_at,
        freshrss_add_url='https://legacy.example.test/i/?c=feed&a=add',
    )

    payload = ActorFeedView.from_record(
        record,
        freshrss_url='https://current-freshrss.example.test',
        freshrss_rsshub_url='https://current-rsshub.example.test',
    ).model_dump()

    assert payload['freshrss_add_url'] == (
        'https://current-freshrss.example.test/i/?c=feed&a=add&url_rss='
        'https%3A%2F%2Fcurrent-rsshub.example.test%2Fjavbus%2Fstar%2Factor'
    )
    assert payload['freshrss_url'] == 'https://current-freshrss.example.test'


def test_feed_view_hides_legacy_freshrss_actions_when_current_configuration_is_disabled() -> None:
    record = JobFeedRecord(
        job_id='legacy-job',
        actor_id='actor',
        state=JobFeedState.READY,
        attempts=2,
        updated_at=datetime(2026, 7, 13, 12, 0, tzinfo=UTC),
        freshrss_add_url='https://legacy.example.test/i/?c=feed&a=add',
    )

    payload = ActorFeedView.from_record(record).model_dump()

    assert payload['freshrss_add_url'] is None
    assert payload['freshrss_url'] is None


def test_mutations_require_configured_bearer_token(tmp_path: Path) -> None:
    token = 'test-bearer-value'
    client, _, _ = make_client(tmp_path, api_token=token)
    with client:
        denied = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        allowed = client.post(
            '/api/fill-actor/plans',
            json={'actor_ids': ['actor']},
            headers={'Authorization': f'Bearer {token}'},
        )
        denied_apply_poll = client.get('/api/fill-actor/apply-jobs/predictable-request')
        allowed_apply_poll = client.get(
            '/api/fill-actor/apply-jobs/predictable-request',
            headers={'Authorization': f'Bearer {token}'},
        )

    assert denied.status_code == 401
    assert denied.json() == {'error': {'code': 'unauthorized'}}
    assert 'WWW-Authenticate' in denied.headers
    assert allowed.status_code == 202
    assert denied_apply_poll.status_code == 401
    assert denied_apply_poll.json() == {'error': {'code': 'unauthorized'}}
    assert 'WWW-Authenticate' in denied_apply_poll.headers
    assert allowed_apply_poll.status_code == 404
    assert allowed_apply_poll.json() == {'error': {'code': 'unknown_apply_job'}}


def test_auth_session_gates_the_login_screen(tmp_path: Path) -> None:
    token = 'login-screen-token'
    client, _, _ = make_client(tmp_path, api_token=token)
    with client:
        denied = client.get('/api/auth/session')
        wrong = client.get('/api/auth/session', headers={'Authorization': 'Bearer nope'})
        allowed = client.get('/api/auth/session', headers={'Authorization': f'Bearer {token}'})
        health = client.get('/api/health')

    assert denied.status_code == 401
    assert denied.json() == {'error': {'code': 'unauthorized'}}
    assert wrong.status_code == 401
    assert allowed.status_code == 200
    assert allowed.json() == {'auth_required': True}
    assert health.json()['auth_required'] is True


def test_auth_session_accepts_anyone_when_no_token_is_configured(tmp_path: Path) -> None:
    client, _, _ = make_client(tmp_path)
    with client:
        response = client.get('/api/auth/session')

    assert response.status_code == 200
    assert response.json() == {'auth_required': False}


def test_cancel_running_plan_is_authenticated_idempotent_and_does_not_require_roots(tmp_path: Path) -> None:
    token = 'cancel-token'
    actor_catalog = BlockingActorCatalog()
    client, paths, _ = make_client(tmp_path, api_token=token, actor_catalog=actor_catalog)
    headers = {'Authorization': f'Bearer {token}'}
    try:
        with client:
            created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']}, headers=headers)
            plan_id = created.json()['job']['plan_id']
            assert actor_catalog.started.wait(timeout=1)
            paths.move_in_path.rmdir()

            denied = client.post(f'/api/fill-actor/plans/{plan_id}/cancel')
            cancelled = client.post(f'/api/fill-actor/plans/{plan_id}/cancel', headers=headers)
            repeated = client.post(f'/api/fill-actor/plans/{plan_id}/cancel', headers=headers)
            current = client.get(f'/api/fill-actor/plans/{plan_id}')

        assert denied.status_code == 401
        assert cancelled.status_code == 200
        assert cancelled.json()['job']['state'] == 'failed'
        assert cancelled.json()['job']['error_code'] == JOB_CANCELLED_ERROR_CODE
        assert cancelled.json()['plan'] is None
        assert repeated.status_code == 200
        assert repeated.json()['job']['error_code'] == JOB_CANCELLED_ERROR_CODE
        assert current.status_code == 200
        assert current.json()['job']['error_code'] == JOB_CANCELLED_ERROR_CODE
        assert current.json()['plan'] is None
    finally:
        actor_catalog.release.set()


def test_cancel_unknown_or_completed_plan_has_stable_response(tmp_path: Path) -> None:
    client, _, _ = make_client(tmp_path)
    with client:
        unknown = client.post('/api/fill-actor/plans/not-found/cancel')
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        plan_id = created.json()['job']['plan_id']
        assert wait_for_plan(client, plan_id)['job']['state'] == 'completed'
        completed = client.post(f'/api/fill-actor/plans/{plan_id}/cancel')

    assert unknown.status_code == 404
    assert unknown.json() == {'error': {'code': 'unknown_plan'}}
    assert completed.status_code == 409
    assert completed.json() == {'error': {'code': 'plan_not_cancellable'}}


def test_api_maps_service_errors_without_raw_messages(tmp_path: Path) -> None:
    client, _, _ = make_client(tmp_path)
    with client:
        invalid = client.post('/api/fill-actor/plans', json={'actor_ids': ['bad actor']})
        malformed = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor'], 'unexpected': True})
        invalid_request_id = client.post(
            '/api/fill-actor/plans/not-found/apply-jobs',
            json={'revision': 'revision', 'candidate_ids': [], 'request_id': 'a' * 129},
        )
        unknown = client.get('/api/fill-actor/plans/not-found')

    assert invalid.status_code == 422
    assert invalid.json() == {'error': {'code': 'invalid_actor_id'}}
    assert malformed.status_code == 422
    assert malformed.json() == {'error': {'code': 'invalid_request'}}
    assert invalid_request_id.status_code == 422
    assert invalid_request_id.json() == {'error': {'code': 'invalid_request'}}
    assert unknown.status_code == 404
    assert unknown.json() == {'error': {'code': 'unknown_plan'}}


def test_request_size_limit_and_health(tmp_path: Path) -> None:
    client, _, _ = make_client(tmp_path, max_request_bytes=20)
    with client:
        oversized = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        health = client.get('/api/health')

    assert oversized.status_code == 413
    assert oversized.json() == {'error': {'code': 'request_too_large'}}
    assert health.status_code == 200
    assert health.json() == {
        'status': 'ok',
        'database': True,
        'auth_required': False,
        'fill_actor': {
            'configured': True,
            'roots': True,
            'cloud': True,
            'legacy_journal': True,
            'apply_enabled': True,
            'scan_ready': True,
            'apply_ready': True,
        },
    }


def test_unavailable_fill_actor_roots_do_not_mark_the_whole_app_unhealthy(tmp_path: Path) -> None:
    client, paths, _ = make_client(tmp_path)
    # An unmounted library root is fill-actor's problem; the monitor pipelines and the
    # settings page keep working, so probes and the top bar must not see a dead app.
    paths.move_in_path.rmdir()

    with client:
        health = client.get('/api/health')
        scan = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})

    assert health.status_code == 200
    assert health.json()['status'] == 'ok'
    assert health.json()['fill_actor'] == {
        'configured': True,
        'roots': False,
        'cloud': True,
        'legacy_journal': True,
        'apply_enabled': True,
        'scan_ready': False,
        'apply_ready': False,
    }
    assert scan.status_code == 503
    assert scan.json() == {'error': {'code': 'not_ready'}}


def test_unconfigured_fill_actor_reports_itself_without_failing_the_app() -> None:
    repository = MemoryFillActorRepository()
    service = FillActorService(
        runtime=FillActorRuntime,
        actor_catalog=ActorCatalog(),
        acquisition_gateway=AcquisitionGateway(),
        brand_resolver=BrandResolver(),
        repository=repository,
    )
    jobs = FillActorJobManager(service=service, repository=repository)
    app = create_app(
        app_ready=repository.health_check,
        routers=(
            create_fill_actor_router(
                service=service,
                repository=repository,
                jobs=jobs,
                mutation_auth=make_mutation_auth(None),
            ),
        ),
        feature_health={'fill_actor': fill_actor_health(service=service, repository=repository)},
        exception_handlers={FillActorError: handle_fill_actor_error},
        lifespans=(fill_actor_lifespan(service=service, repository=repository, jobs=jobs),),
    )

    with TestClient(app) as client:
        health = client.get('/api/health')
        scan = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})

    assert health.status_code == 200
    assert health.json()['status'] == 'ok'
    assert health.json()['fill_actor']['configured'] is False
    assert health.json()['fill_actor']['scan_ready'] is False
    assert scan.status_code == 503
    assert scan.json() == {'error': {'code': 'not_ready'}}


def test_disabled_apply_is_exposed_without_affecting_scan_or_readiness(tmp_path: Path) -> None:
    client, paths, _ = make_client(tmp_path, apply_enabled=False)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')

    with client:
        health = client.get('/api/health')
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        plan_id = created.json()['job']['plan_id']
        payload = wait_for_plan(client, plan_id)
        candidate = payload['plan']['videos'][0]['move_candidates'][0]
        applied = client.post(
            f'/api/fill-actor/plans/{plan_id}/apply',
            json={
                'revision': payload['plan']['revision'],
                'candidate_ids': [candidate['candidate_id']],
            },
        )

    assert health.status_code == 200
    assert health.json()['status'] == 'ok'
    assert health.json()['fill_actor']['apply_enabled'] is False
    assert created.status_code == 202
    assert payload['job']['state'] == 'completed'
    assert applied.status_code == 503
    assert applied.json() == {'error': {'code': 'move_disabled'}}
    assert source.read_bytes() == b'video'
    assert not (paths.move_in_path / source.name).exists()


def test_running_job_does_not_publish_or_apply_plan(tmp_path: Path) -> None:
    actor_catalog = BlockingActorCatalog()
    client, _, _ = make_client(tmp_path, actor_catalog=actor_catalog)
    try:
        with client:
            created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
            plan_id = created.json()['job']['plan_id']
            assert actor_catalog.started.wait(timeout=1)

            current = client.get(f'/api/fill-actor/plans/{plan_id}')
            applied = client.post(
                f'/api/fill-actor/plans/{plan_id}/apply',
                json={'revision': 'not-published', 'candidate_ids': []},
            )

            assert current.status_code == 200
            assert current.json()['job']['state'] == 'running'
            assert current.json()['plan'] is None
            assert applied.status_code == 409
            assert applied.json() == {'error': {'code': 'plan_not_ready'}}
            actor_catalog.release.set()
            assert wait_for_plan(client, plan_id)['job']['state'] == 'completed'
    finally:
        actor_catalog.release.set()


def test_completed_job_without_plan_returns_not_found(tmp_path: Path) -> None:
    client, _, repository = make_client(tmp_path)
    with client:
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        plan_id = created.json()['job']['plan_id']
        assert wait_for_plan(client, plan_id)['job']['state'] == 'completed'
        assert client.portal is not None
        assert client.portal.call(repository.delete_plan, plan_id)

        missing = client.get(f'/api/fill-actor/plans/{plan_id}')

    assert missing.status_code == 404
    assert missing.json() == {'error': {'code': 'unknown_plan'}}


def test_job_progress_view_derives_stage_eta_and_activity_ages() -> None:
    started = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    view = JobProgressView.from_record(
        JobProgress(
            stage=JobStage.LIBRARY_SCAN,
            completed=4,
            total=10,
            unit=JobProgressUnit.VIDEOS,
            current='ABC-004',
            stage_started_at=started,
            updated_at=started + timedelta(seconds=30),
        ),
        state=JobState.RUNNING,
        now=started + timedelta(seconds=40),
    )

    assert view.percent == 40.0
    assert view.eta_seconds == 60
    assert view.elapsed_seconds == 40
    assert view.last_progress_seconds == 10

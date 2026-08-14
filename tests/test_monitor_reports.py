import logging

from embyx_manager.monitor.reports import RunContext


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-reports'))


def _boom() -> None:
    msg = 'disk detached'
    raise OSError(msg)


def test_exception_records_the_cause_in_the_tail() -> None:
    # The dashboard only ever sees the recorded tail, so the line must carry
    # the exception itself, not just the caller's message.
    ctx = make_ctx()
    try:
        _boom()
    except OSError:
        ctx.exception('%s run failed', 'mapping')

    assert ctx.log_tail == ("ERROR: mapping run failed: OSError('disk detached')",)
    assert ctx.errors == ("ERROR: mapping run failed: OSError('disk detached')",)


def test_exception_outside_handler_records_plain_message() -> None:
    ctx = make_ctx()
    ctx.exception('mapping run failed')

    assert ctx.log_tail == ('ERROR: mapping run failed',)

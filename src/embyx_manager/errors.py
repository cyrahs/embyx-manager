"""The HTTP error envelope every feature router shares.

Lives outside any one feature so a router never has to reach into a sibling's
module for it — `monitor` used to raise `config`'s error type for that reason.
"""


class ApiError(Exception):
    """Renders as `{"error": {"code": ...}}` with an explicit status code."""

    def __init__(self, status_code: int, code: str) -> None:
        self.status_code = status_code
        self.code = code
        super().__init__(code)

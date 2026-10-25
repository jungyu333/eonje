from __future__ import annotations


class ClientError(Exception):
    pass


class TransientError(ClientError):
    pass


class ApiError(ClientError):
    def __init__(self, result_code: str, message: str) -> None:
        super().__init__(f"{result_code}: {message}")
        self.result_code = result_code


class UnexpectedResponseError(ClientError):
    pass

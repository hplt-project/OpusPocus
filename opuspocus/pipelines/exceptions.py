class PipelineInitError(Exception):
    """Error class for reporting pipeline initialization errors."""

    def __init__(self, message):  # noqa: ANN001,ANN204
        self.message = message
        super().__init__(self.message)


class PipelineStateError(Exception):
    """Error class for reporting pipeline state-related errors."""

    def __init__(self, message):  # noqa: ANN001,ANN204
        self.message = message
        super().__init__(self.message)

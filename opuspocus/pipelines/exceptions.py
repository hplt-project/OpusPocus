class PipelineInitError(Exception):
    """Error class for reporting pipeline initialization errors."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

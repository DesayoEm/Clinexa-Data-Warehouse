from include.etl.extraction.extraction import Extractor
from include.tests.failure_generators import FailureGenerator


class ExtractorWithFailureInjection(Extractor):
    """Test-only wrapper that injects errors to test failure patterns"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failure_generator = FailureGenerator(True, 1.0)

    def save_response(self, page_number, data):
        # NOTE:
        # Failure injection is intentionally placed at the save_response stage rather than
        # during request execution. The goal of this test is not to simulate HTTP/network
        # failures, but to validate the extractor’s ability to:
        #   - persist state correctly,
        #   - checkpoint progress,
        #   - and recover safely from a mid-extraction failure.
        #
        # save_response is a stable per-page boundary where page_number is known and
        # extractor state is about to mutate, making it the most reliable point for
        # failure testing.

        if page_number == 3:
            self.failure_generator.maybe_fail_extraction(page_number)
        super().save_response(page_number, data)

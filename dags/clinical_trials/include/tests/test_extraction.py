from clinical_trials.include.etl.extraction.extraction import Extractor
from clinical_trials.include.tests.failure_generators import FailureGenerator


class ExtractorWithFailureInjection(Extractor):
    """Test-only wrapper that injects failures"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failure_generator = FailureGenerator(True, 1.0)

    def make_requests(self):
        if self.last_saved_page == 3:
            self.failure_generator.maybe_fail_extraction(self.last_saved_page)
        return super().make_requests()


import time
from math import log2, log
from typing import Any, Dict


# Code copied from here
# https://github.com/wikimedia/mediawiki-extensions-CirrusSearch/blob/master/includes/BuildDocument/Completion/QualityScore.php
# A TODO here would be to do an evaluation of the scoring method used here and evaluate
# what factors we consider important and adjust/change/rewrite as needed.
class Scorer():
    INCOMING_LINKS_MAX_DOCS_FACTOR = 0.1

    EXTERNAL_LINKS_NORM = 20
    PAGE_SIZE_NORM = 50000
    HEADING_NORM = 20
    REDIRECT_NORM = 30

    INCOMING_LINKS_WEIGHT = 0.6
    EXTERNAL_LINKS_WEIGHT = 0.1
    PAGE_SIZE_WEIGHT = 0.1
    HEADING_WEIGHT = 0.2
    REDIRECT_WEIGHT = 0.1

    QSCORE_WEIGHT = 1
    # 0.04% of the total page views is the max we accept
    POPULARITY_WEIGHT = 0.4
    POPULARITY_MAX = 0.0004

    SCORE_RANGE = 10_000_000

    max_docs: int
    incoming_links_norm: int

    def __init__(self, max_docs: int) -> None:
        self.max_docs = max_docs
        self.incoming_links_norm = int(
                self.max_docs * self.INCOMING_LINKS_MAX_DOCS_FACTOR)

    def score(self, doc: Dict) -> float:
        incoming_links = self._score_norm_log2(
                doc.get('incoming_links', 0),
                self.incoming_links_norm)
        page_size = self._score_norm_log2(
                doc.get('text_bytes', 0),
                self.PAGE_SIZE_NORM)
        external_links = self._score_norm(
                len(doc.get('external_links', [])),
                self.EXTERNAL_LINKS_NORM)
        heading = self._score_norm(
                len(doc.get('heading', [])),
                self.HEADING_NORM)
        redirect = self._score_norm(
                len(doc.get('redirect', [])),
                self.REDIRECT_NORM)

        score = incoming_links * self.INCOMING_LINKS_WEIGHT

        score += external_links * self.EXTERNAL_LINKS_WEIGHT
        score += page_size * self.PAGE_SIZE_WEIGHT
        score += heading * self.HEADING_WEIGHT
        score += redirect * self.REDIRECT_WEIGHT

        # We have a standardized composite score between 0 and 1
        score = score / (
                self.INCOMING_LINKS_WEIGHT + self.EXTERNAL_LINKS_WEIGHT +
                self.PAGE_SIZE_WEIGHT + self.HEADING_WEIGHT + self.REDIRECT_WEIGHT
        ) * self.QSCORE_WEIGHT

        popularity = doc.get('popularity_score', 0)
        if popularity > self.POPULARITY_MAX:
            popularity = 1
        else:
            log_base = 1 + self.POPULARITY_MAX * self.max_docs
            if log_base > 1:
                popularity = log(1 + (popularity * self.max_docs), log_base)
            else:
                popularity = 0

        score += popularity * self.POPULARITY_WEIGHT
        score /= self.QSCORE_WEIGHT + self.POPULARITY_WEIGHT

        return int(score * self.SCORE_RANGE)

    def _score_norm_log2(self, value: int, norm: int) -> float:
        return log2(2 if value < norm else (value / norm) + 1)

    def _score_norm(self, value: int, norm: int) -> float:
        return log(1 if value < norm else value / norm)


class Builder():

    scorer: Scorer
    batch_id: int

    def __init__(self, max_docs: int = 6_500_000) -> None:
        self.batch_id = int(time.time_ns()/1000)
        self.scorer = Scorer(max_docs)

    def build(self, id: str, doc: Dict[str, Any]):
        title = doc.get("title", "")

        # TODO the original wikimedia scoring mechanism extracts
        # similars from the redirects in a doc and uses that as additional inputs
        # into the suggestions
        similars = []
        inputs = [title]
        inputs.extend(similars)
        score = self.scorer.score(doc)

        return {
            "batch_id": self.batch_id,
            "doc_id": id,
            "title": title,
            "suggest": {
                "input": inputs,
                "weight": score,
            },
            "suggest-stop": {
                "input": inputs,
                "weight": score,
            },
        }

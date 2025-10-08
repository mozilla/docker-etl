import attr
import cattrs
from datetime import date, datetime, timedelta
import json
import pytz
import re
import tldextract

from typing import List, Optional


@attr.s(auto_attribs=True)
class Branch:
    """Defines a branch of a Nimbus experiement from Experimenter."""

    slug: str
    ratio: int
    features: Optional[dict]


@attr.s(auto_attribs=True)
class NimbusExperiment:
    """Represents a v8 Nimbus experiment from Experimenter. Most of these values get read from
        Nimbus's GET experiment endpoint's json response. The notable exception is batch_duration,
        which currently comes from the experiment's configuration in config.json, but we add it
        to this model so we can conveniently figure out when the data should be collected.

    Attributes:
        appId:                  Id of the app we're experimenting on, something like 'firefox-desktop'.
        appName:                Name of the app we're experimenting on, something like 'firefox_desktop'.
        batchDuration:          The DAP agreggation time interval.
        branches:               A list of Branch objects for the experiment's branch data.
        bucketConfig:
        channel:                The release channel for this experiment, something like 'nightly'.
        featureIds:             A list of all the features used in this experiment.
        proposedEnrollment:
        referenceBranch:        The slug of the control branch.
        slug:                   Normandy slug that uniquely identifies the experiment
                                in Nimbus.
        targeting:              A string of js that evaluates to a boolean value indicating
                                targeting based on region, channel, and user prefs.

        startDate:              The day the experiment will begin enrolling users.
        endDate:                The day the experiment will be turned off.
        enrollmentEndDate:      The day the experiment's enrollment phase ends.

    """

    appId: str
    appName: str
    batchDuration: int
    branches: List[Branch]
    bucketConfig: dict
    channel: str
    featureIds: list[str]
    proposedEnrollment: int
    referenceBranch: Optional[str]
    slug: str
    targeting: str

    startDate: date
    endDate: Optional[date]
    enrollmentEndDate: Optional[date]

    @classmethod
    def from_dict(cls, d) -> "NimbusExperiment":
        """Load an experiment from dict."""
        converter = cattrs.BaseConverter()
        converter.register_structure_hook(
            date,
            lambda num, _: datetime.fromisoformat(num.replace("Z", "+00:00"))
            .astimezone(pytz.utc)
            .date(),
        )
        converter.register_structure_hook(
            Branch,
            lambda b, _: Branch(
                slug=b["slug"], ratio=b["ratio"], features=b["features"]
            ),
        )
        return converter.structure(d, cls)

    def latest_collectible_batch_start(self) -> date:
        batch_interval_start = self.startDate
        # If the experiment's start date is today or in the future, return it
        if batch_interval_start >= self.todays_date():
            return self.startDate

        # While the batch_interval_start variable is before the batch that includes today...
        while batch_interval_start <= self.todays_date():
            # Increment the latest_collectible_batch_start by the batch interval.
            batch_interval_start = batch_interval_start + timedelta(
                seconds=self.batchDuration
            )
        # After the loop, the batch batch_interval_start is the next batch after the batch that includes today.
        # We need to go back two batch interval start dates to get the start of the latest collectible batch.

        # This handles the edge case where today's date is within the first batch of the experiment
        if ((batch_interval_start - timedelta(seconds=2 * self.batchDuration)) < self.startDate):
            return self.startDate

        # Now we can return the previous batch, which is now complete and ready for collection.
        return batch_interval_start - timedelta(seconds=2 * self.batchDuration)

    def latest_collectible_batch_end(self) -> date:
        return self.latest_collectible_batch_start() + timedelta(
            seconds=self.batchDuration, days=-1
        )

    def collect_today(self) -> bool:
        return self.latest_collectible_batch_end() < self.todays_date()

    def todays_date(self) -> date:
        return date.today()


def get_country_from_targeting(targeting: str) -> Optional[str]:
    """Parses the region/country from the targeting string and
    returns a JSON formatted list of country codes."""
    match = re.search(r"region\s+in\s+\[([^]]+)]", targeting)

    if match:
        inner = match.group(1)
        regions = [r.strip().strip("'\"") for r in inner.split(",")]
        return json.dumps(regions)
    return None


def normalize_url(url: str) -> str:
    # Replace wildcard with a dummy protocol and subdomain so urlparse can handle it
    normalized = re.sub(r"^\*://\*\.?", "https://", url)
    return normalized


def get_advertiser_from_url(url: str) -> Optional[str]:
    """Parses the advertiser name (domain) from the url"""
    # tldextract cannot handle wildcards, replace with standard values.
    normalized = normalize_url(url)
    ext = tldextract.extract(normalized)
    return ext.domain


@attr.s(auto_attribs=True, auto_detect=True, eq=True)
class IncrementalityBranchResultsRow:
    """This object encapsulates all the data for an incrementality experiment branch that uses the
    Nimbus dapTelemetry feature. It is used as an intermediate data structure, first to hold the
    info from the experiment metadata which is later used in the DAP collection, then to store
    the actual count values fetched from DAP, and finally to write most of these attributes to
    a BQ results row.

    Attributes:
        advertiser:         Derived from from the urls stored in the visitCountingExperimentList
                            key within Nimbus's dapTelemetry feature.
        batch_start:        The start date of the collection period that we're getting counts for from DAP, inclusive.
        batch_end:          The end date of the collection period that we're getting counts from DAP, inclusive.
        batch_duration:     The duration of the collection period that we're requeting counts for from DAP.
        branch:             A Nimbus experiment branch. Each experiment may have multiple
                            branches (ie control, treatment-a).
        bucket:             Stored in Nimbus experiment metadata. Each exeriment branch specifies
                            the corresponding DAP bucket where the visit counts for that branch
                            can be collected.
        country_codes:      The countries where the experiment is active, as an array of ISO country code strings.
        experiment_slug:    The Nimbus experiment's URL slug
        metric:             Currently hardcoded to "unique_client_organic_visits" for incrementality.
        task_id:            Stored in Nimbus experiment metadata. The task id is returned when setting
                            up DAP counting, and is used to collect the experiment result counts.
        task_veclen:        Stored in Nimbus experiment metadata. The task_veclen is configured when
                            setting up DAP counting, and is needed to collect the experiment results.
        value_count:        The url visits count value collected from DAP for this experiment branch.
    """

    advertiser: str
    batch_start: date
    batch_end: date
    batch_duration: int
    branch: str
    bucket: int
    country_codes: Optional[str]
    experiment_slug: str
    metric: str
    task_id: str
    task_veclen: int
    value_count: int

    def __init__(
        self,
        experiment: NimbusExperiment,
        branch_slug: str,
        visitCountingExperimentListItem: dict,
    ):
        self.advertiser = "not_set"
        urls = visitCountingExperimentListItem.get("urls")
        # Default to the first url in the list to determine the advertiser.
        if len(urls) > 0:
            self.advertiser = get_advertiser_from_url(urls[0])
        self.branch = branch_slug
        self.bucket = visitCountingExperimentListItem.get("bucket")
        self.batch_start = experiment.latest_collectible_batch_start()
        self.batch_end = experiment.latest_collectible_batch_end()
        self.batch_duration = experiment.batchDuration
        self.country_codes = get_country_from_targeting(experiment.targeting)
        self.experiment_slug = experiment.slug
        self.metric = "unique_client_organic_visits"
        self.task_id = visitCountingExperimentListItem.get("task_id")
        self.task_veclen = visitCountingExperimentListItem.get("task_veclen")
        # This will be populated when we successfully fetch the count from DAP
        self.value_count = None

    def __str__(self):
        return str(
            f"IncrementalityBranchResultsRow(advertiser='{self.advertiser}', branch='{self.branch}', "
            f"bucket='{self.bucket}', batch_start='{self.batch_start}', batch_end='{self.batch_end}', "
            f"country_codes='{self.country_codes}', experiment_slug='{self.experiment_slug}', metric='{self.metric}', "
            f"task_id='{self.task_id}', task_veclen='{self.task_veclen}', value_count='redacted')"
        )

    __repr__ = __str__

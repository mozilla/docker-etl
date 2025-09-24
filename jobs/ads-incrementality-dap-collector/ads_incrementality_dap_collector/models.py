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
    endDate: Optional[datetime]
    enrollmentEndDate: Optional[datetime]

    # Experiment results are removed from DAP after 2 weeks, so our window to collect results for
    # an experiment is up to 2 weeks after the experiment's current_batch_end.
    # However, we don't need to collect every day of those two weeks (this job runs daily). So this
    # constant defines how many days after current_batch_end to go out and collect and write to BQ.
    COLLECT_RETRY_DAYS = 7

    @classmethod
    def from_dict(cls, d) -> "NimbusExperiment":
        """Load an experiment from dict."""
        converter = cattrs.BaseConverter()
        converter.register_structure_hook(
            datetime,
            lambda num, _: datetime.fromisoformat(
                num.replace("Z", "+00:00")
            ).astimezone(pytz.utc),
        )
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

    def current_batch_start(self) -> date:
        current_batch_start = self.startDate
        if current_batch_start >= date.today():
            return current_batch_start

        while current_batch_start < (
            date.today() - timedelta(seconds=self.batchDuration)
        ):
            current_batch_start = current_batch_start + timedelta(
                seconds=self.batchDuration
            )
        return current_batch_start - timedelta(seconds=self.batchDuration)

    def current_batch_end(self) -> date:
        return self.current_batch_start() + timedelta(seconds=self.batchDuration)

    def next_collect_date(self) -> date:
        return self.current_batch_end() + timedelta(days=1)

    def collect_today(self) -> bool:
        return (
            self.current_batch_end()
            < date.today()
            < (self.current_batch_end() + timedelta(days=self.COLLECT_RETRY_DAYS))
        )


def get_country_from_targeting(targeting: str) -> Optional[str]:
    """Parses the region/country from the targeting string and
    returns a JSON formatted list of country codes."""
    # match = re.findall(r"region\s+in\s+(^]+)", targeting)
    match = re.search(r"region\s+in\s+\[([^]]+)]", targeting)

    if match:
        inner = match.group(1)
        regions = [r.strip().strip("'\"") for r in inner.split(",")]
        # logging.info("regions: %s", regions)
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
        batch_start:        The start date of the collection period that we're requesting counts for from DAP.
        batch_end:          The end date of the collection period that we're requesting counts from DAP.
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
    batch_duration: date
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
        self.batch_start = experiment.current_batch_start()
        self.batch_end = experiment.current_batch_end()
        self.batch_duration = experiment.batchDuration
        self.country_codes = get_country_from_targeting(experiment.targeting)
        self.experiment_slug = experiment.slug
        self.metric = "unique_client_organic_visits"
        self.task_id = visitCountingExperimentListItem.get("task_id")
        self.task_veclen = visitCountingExperimentListItem.get("task_veclen")
        # This will be populated when we successfully fetch the count from DAP
        self.value_count = None

    def __str__(self):
        return f"IncrementalityBranchResultsRow(advertiser='{self.advertiser}', branch='{self.branch}', \
            bucket='{self.bucket}', experiment_slug='{self.experiment_slug}', metric='{self.metric}', \
            task_id='{self.task_id}', task_veclen='redacted', value_count='redacted')"

    __repr__ = __str__


@attr.s(auto_attribs=True)
class BQConfig:
    """Encapsulates everything the job needs to connect to BigQuery

    Attributes:
        project:         GCP project
        namespace:       BQ namespace for ads incrementality
        table:           BQ table where incrementality results go
    """

    project: str
    namespace: str
    table: str


@attr.s(auto_attribs=True)
class DAPConfig:
    """Encapsulates everything the job needs to connect to DAP

    Attributes:
        auth_token:         Token defined in the collector credentials, used to authenticate to the leader
        hpke_private_key:   Private key defined in the collector credentials, used to decrypt shares from the leader
                            and helper
        hpke_config:        base64 url-encoded version of public key defined in the collector credentials
        batch_start:        Start of the collection interval, as the number of seconds since the Unix epoch
    """

    auth_token: str
    hpke_private_key: str
    hpke_config: str
    batch_start: int


@attr.s(auto_attribs=True)
class ExperimentConfig:
    """Encapsulates the experiments that should be collected from DAP and how far back to collect,
    in seconds

    Attributes:
        slug:               Experiment slug
        batch_duration:     Duration of the collection batch interval, in seconds
    """

    slug: str
    batch_duration: int


@attr.s(auto_attribs=True)
class NimbusConfig:
    """Encapsulates everything the job needs to connect to Nimbus

    Attributes:
        experiments:    Config for the incrementality experiments. Nimbus experiments
                        branches store DAP task info that allows for branch results
                        collection from DAP.
        api_url:        API URL for fetching the Nimbus experiment info
    """

    experiments: list[ExperimentConfig]
    api_url: str


@attr.s(auto_attribs=True)
class IncrementalityConfig:
    """Encapsulates everything the job needs to connect to various 3rd party services

    Attributes:
        bq:         BigQuery config
        dap:        Divviup's DAP service config
        nimbus:     Nimbus Experiment framework config
    """

    bq: BQConfig
    dap: DAPConfig
    nimbus: NimbusConfig


class ConfigEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__

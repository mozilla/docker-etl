import attr
import cattrs
import datetime
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
    """Represents a v8 Nimbus experiment from Experimenter."""

    slug: str  # Normandy slug
    startDate: Optional[datetime.datetime]
    endDate: Optional[datetime.datetime]
    enrollmentEndDate: Optional[datetime.datetime]
    proposedEnrollment: int
    branches: List[Branch]
    referenceBranch: Optional[str]
    appName: str
    appId: str
    channel: str
    targeting: str
    bucketConfig: dict
    featureIds: list[str]

    @classmethod
    def from_dict(cls, d) -> "NimbusExperiment":
        """Load an experiment from dict."""
        converter = cattrs.BaseConverter()
        converter.register_structure_hook(
            datetime.datetime,
            lambda num, _: datetime.datetime.fromisoformat(
                num.replace("Z", "+00:00")
            ).astimezone(pytz.utc),
        )
        converter.register_structure_hook(
            Branch,
            lambda b, _: Branch(
                slug=b["slug"], ratio=b["ratio"], features=b["features"]
            ),
        )
        return converter.structure(d, cls)

def get_country_from_targeting(targeting: str) -> Optional[str]:
    """Parses the region/country from the targeting string and
    returns a JSON formatted list of country codes."""
    # match = re.findall(r"region\s+in\s+(^]+)", targeting)
    match = re.search(r"region\s+in\s+\[([^]]+)]", targeting)

    if match:
        inner = match.group(1)
        regions = [r.strip().strip("'\"") for r in inner.split(',')]
        # logging.info("regions: %s", regions)
        return json.dumps(regions)
    return None

def normalize_url(url: str) -> str:
    # Replace wildcard with a dummy protocol and subdomain so urlparse can handle it
    normalized = re.sub(r'^\*://\*\.?', 'https://', url)
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
            branch:             A Nimbus experiment branch. Each experiment may have multiple
                                branches (ie control, treatment-a).
            bucket:             Stored in Nimbus experiment metadata. Each exeriment branch specifies
                                the corresponding DAP bucket where the visit counts for that branch
                                can be collected.
            experiment_slug:    The Nimbus experiment's URL slug
            metric:             Currently hardcoded to "unique_client_organic_visits" for incrementality.
            task_id:            Stored in Nimbus experiment metadata. The task id is returned when setting
                                up DAP counting, and is used to collect the experiment result counts.
            task_veclen:        Stored in Nimbus experiment metadata. The task_veclen is configured when
                                setting up DAP counting, and is needed to collect the experiment results.
            value_count:        The url visits count value collected from DAP for this experiment branch.
    """

    advertiser: str
    branch: str
    bucket: int
    experiment_slug: str
    metric: str
    task_id: str
    task_veclen: int
    value_count: int

    def __init__(self, experiment: NimbusExperiment, branch_slug: str, visitCountingExperimentListItem: dict):
        self.advertiser = "not_set"
        urls = visitCountingExperimentListItem.get("urls")
        # Default to the first url in the list to determine the advertiser.
        if len(urls) > 0:
            self.advertiser = get_advertiser_from_url(urls[0])
        self.branch = branch_slug
        self.bucket = visitCountingExperimentListItem.get("bucket")
        self.country_codes = get_country_from_targeting(experiment.targeting)
        self.experiment_slug = experiment.slug
        self.metric = "unique_client_organic_visits"
        self.task_id = visitCountingExperimentListItem.get("task_id")
        self.task_veclen = visitCountingExperimentListItem.get("task_veclen")
        # This will be populated when we successfully fetch the count from DAP
        self.value_count = None


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
            hpke_token:         Token defined in the collector credentials, used to authenticate to the leader
            hpke_private_key:   Private key defined in the collector credentials, used to decrypt shares from the leader and helper
            hpke_config:        base64url-encoded version of hpke_config defined in the collector credentials
            batch_start:        Start of the collection interval, as the number of seconds since the Unix epoch
    """

    hpke_token: str
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
    def default(self, o): return o.__dict__

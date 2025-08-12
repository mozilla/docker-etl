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


@attr.s(auto_attribs=True, auto_detect=True)
class IncrementalityBranchData:
    """This info is contained within a Nimbus Experiment branch's VisitCountExperiment data.
        It associates the Nimbus experiment branch to the DAP bucket."""

    advertiser: str
    branch: str
    bucket: int
    experiment_slug: str
    metric: str
    task_id: str
    targeting: int
    task_veclen: int
    value_count: int

    def __init__(self, experiment, branch_slug, visitCountingExperimentListItem):
        urls = visitCountingExperimentListItem.get("urls")
        # Default to the first url in the list to determine the advertiser.
        self.advertiser = "not_set"
        if len(urls) > 0:
            self.advertiser = get_advertiser_from_url(urls[0])
        self.branch = branch_slug
        self.bucket = visitCountingExperimentListItem.get("bucket")
        self.country_codes = get_country_from_targeting(experiment.targeting)
        self.experiment_slug = experiment.slug
        self.metric = "unique_client_organic_visits"
        self.task_id = visitCountingExperimentListItem.get("task_id")
        self.targeting = experiment.targeting
        # Store the length associated with the task, needed for collector process.
        self.task_veclen = visitCountingExperimentListItem.get("task_veclen")

# @attr.s(auto_attribs=True)
# class IncrementalityBranchResult:
#     """Defines the Incrementality results data for a particular branch of an Incrementality experiment.
#         It brings together data specified in the Nimbus experiment branch with the metrics and results fetched from DAP"""

#     advertiser: str
#     country_codes: list[str]
#     dap_bucket: str
#     experiment_slug: str
#     experiment_branch_slug: str
#     metrics: str
#     value_count: int

#     def __init__(self, branch_config: IncrementalityBranchConfig):
#         self.advertiser = branch_config.advertiser
#         self.country_codes = get_country_from_targeting(branch_config.targeting)
#         self.dap_bucket = branch_config.bucket
#         self.experimnent_slug = branch_config.experiment_slug
#         self.experiment_branch_slug = branch_config.branch_slug
#         self.metrics = "unique_client_organic_visits"

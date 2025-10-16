import attr
import cattrs
from datetime import date, datetime, timedelta
import json
import pytz
import re

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
        Nimbus's GET experiment endpoint's json response. The notable exceptions are batch_duration,
        which currently comes from the experiment's configuration in config.json. We also add the
        process_date to this model, which comes from Airflow's job run. Both are present on this model
        so we can conveniently figure out when the data should be collected and what "batch interval"
        start and end date to request collection from DAP.

    Attributes:
        batchDuration:          The DAP agreggation time interval.
        branches:               A list of Branch objects for the experiment's branch data.
        bucketConfig:
        featureIds:             A list of all the features used in this experiment.
        slug:                   Normandy slug that uniquely identifies the experiment
                                in Nimbus.
        targeting:              A string of js that evaluates to a boolean value indicating
                                targeting based on region, channel, and user prefs.

        startDate:              The day the experiment will begin enrolling users.
        endDate:                The day the experiment will be turned off.
        processDate:            The date of batch collection (comes from airflow's daily run of the job)
    """

    batchDuration: int
    branches: List[Branch]
    bucketConfig: dict
    featureIds: list[str]
    slug: str
    targeting: str
    startDate: date
    endDate: Optional[date] = None
    processDate: Optional[date] = None

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

    def set_process_date(self, date: date) -> None:
        self.processDate = date

    def latest_collectible_batch_start(self) -> date:
        # If the experiment's start date is on or after the processing date,
        # Or the processing date is in the experiment's first batch (excluding end date),
        # Then return the experiment's start date as latest_collectible_batch_start
        if (self.startDate >= self.processDate) or (
            self.startDate + timedelta(seconds=self.batchDuration, days=-1)
            > self.processDate
        ):
            return self.startDate

        batch_interval_start = self.startDate
        # While the batch_interval_start variable is before the batch that includes the processing date...
        while batch_interval_start <= self.processDate:
            # Increment the batch_interval_start by the batch interval.
            batch_interval_start = batch_interval_start + timedelta(
                seconds=self.batchDuration
            )
        # After the loop, the batch_interval_start is for the batch after the one that includes processing date.

        # First, handle the edge case where the processing date is the end of the previous batch
        # So the previous batch is the latest collectible batch
        if self.processDate == (batch_interval_start - timedelta(days=1)):
            return batch_interval_start - timedelta(seconds=self.batchDuration)

        # Otherwise, we're still within the batch that includes processing date
        # So latest collectible batch is two inteverals back
        return batch_interval_start - timedelta(seconds=2 * self.batchDuration)

    def latest_collectible_batch_end(self) -> date:
        return self.latest_collectible_batch_start() + timedelta(
            seconds=self.batchDuration, days=-1
        )

    def should_collect_batch(self) -> bool:
        return self.latest_collectible_batch_end() == self.processDate


def get_metric_from_feature(feature: dict) -> str:
    metric_type = feature.get("measurementType")
    if metric_type == "referrerMeasurement":
        return feature.get("referrerUrls")[0].get("metric_name")
    if metric_type == "visitMeasurement":
        return feature.get("visitCountUrls")[0].get("metric_name")
    raise Exception(
        f"Unknown measurementType '{metric_type}' in dapIncrementality feature."
    )


def get_bucket_from_feature_urls(feature: dict) -> int:
    metric_type = feature.get("measurementType")
    if metric_type == "referrerMeasurement":
        return feature.get("referrerUrls")[0].get("bucket")
    if metric_type == "visitMeasurement":
        return feature.get("visitCountUrls")[0].get("bucket")
    raise Exception(
        f"Unknown measurementType '{metric_type}' in dapIncrementality feature."
    )


def get_country_from_targeting(targeting: str) -> Optional[str]:
    """Parses the region/country from the targeting string and
    returns a JSON formatted list of country codes."""
    match = re.search(r"region\s+in\s+\[([^]]+)]", targeting)

    if match:
        inner = match.group(1)
        regions = [r.strip().strip("'\"") for r in inner.split(",")]
        return json.dumps(regions)
    return None


@attr.s(auto_attribs=True, auto_detect=True, eq=False)
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
        task_length:        Stored in Nimbus experiment metadata. The task_length is configured when
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
    task_length: int
    value_count: int

    def __eq__(self, other):
        return (
            self.advertiser == other.advertiser
            and self.batch_start == other.batch_start
            and self.batch_end == other.batch_end
            and self.batch_duration == other.batch_duration
            and self.branch == other.branch
            and self.bucket == other.bucket
            and self.experiment_slug == other.experiment_slug
            and self.metric == other.metric
            and self.task_id == other.task_id
            and self.task_length == other.task_length
            and self.value_count == other.value_count
        )

    def __init__(
        self,
        experiment: NimbusExperiment,
        branch_slug: str,
        feature: dict,
    ):
        self.advertiser = feature.get("advertiser")
        self.branch = branch_slug
        self.bucket = get_bucket_from_feature_urls(feature)
        self.batch_start = experiment.latest_collectible_batch_start()
        self.batch_end = experiment.latest_collectible_batch_end()
        self.batch_duration = experiment.batchDuration
        self.country_codes = get_country_from_targeting(experiment.targeting)
        self.experiment_slug = experiment.slug
        self.metric = get_metric_from_feature(feature)
        self.task_id = feature.get("taskId")
        self.task_length = feature.get("length")
        # This will be populated when we successfully fetch the count from DAP
        self.value_count = None

    def __str__(self):
        return str(
            f"IncrementalityBranchResultsRow(advertiser='{self.advertiser}', branch='{self.branch}', "
            f"bucket='{self.bucket}', batch_start='{self.batch_start}', batch_end='{self.batch_end}', "
            f"country_codes='{self.country_codes}', experiment_slug='{self.experiment_slug}', metric='{self.metric}', "
            f"task_id='{self.task_id}', task_length='{self.task_length}', value_count='redacted')"
        )

    __repr__ = __str__

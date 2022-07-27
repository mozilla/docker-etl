import click
import json
from typing import Mapping, Any, Optional

import logging
import logging.config

from elasticsearch import Elasticsearch

from wiki.filemanager import FileManager
from wiki import indexer

cfg = {
    'version': 1,
    'formatters': {
        'json': {
            '()': 'dockerflow.logging.JsonLogFormatter',
            'logger_name': 'wikipedia-indexer'
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'json'
        },
    },
    'loggers': {
        'wikipedia-indexer': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
    }
}

logging.config.dictConfig(cfg)
logger = logging.getLogger('wikipedia-indexer')


@click.command()
@click.option("--elasticsearch-hostname", default="http://35.192.164.92:9200/")
@click.option("--elasticsearch-alias", default="enwiki")
@click.option("--total-docs", default=6_400_000)
@click.option("--export-base-url",
              default="https://dumps.wikimedia.org/other/cirrussearch/current/")
@click.option("--gcs-bucket", default="wikipedia-search-dumps")
@click.option("--gcs-project", default="wstuckey-sandbox")
def main(elasticsearch_hostname: str,
         elasticsearch_alias: str,
         total_docs: int,
         export_base_url: str,
         gcs_bucket: str,
         gcs_project: str):
    es_client = Elasticsearch(hosts=[elasticsearch_hostname], request_timeout=60)
    file_manager = FileManager(export_base_url, gcs_bucket, gcs_project)
    latest = file_manager.get_latest_gcs()
    if latest.name:
        logger.info("Ensuring latest dump is on GCS",
                    extra={"latest_name": latest.name})
        file_manager.stream_latest_dump_to_gcs(latest)
        index_name = indexer.get_index_name(latest.name)
        logger.info("Ensuring index exists", extra={"index": index_name})
        if indexer.ensure_index(es_client, index_name)["acknowledged"]:
            prior: Optional[Mapping[str, Any]] = None
            logger.info("Start indexing", extra={"index": index_name})
            for i, line in enumerate(file_manager.stream_from_gcs(latest)):
                doc = json.loads(line)
                if prior and (i + 1) % 2 == 0:
                    indexer.enqueue(index_name, (prior, doc))
                    indexer.index_docs(es_client, False)
                    prior = None
                else:
                    prior = doc
                perc_done = round((i/2)/total_docs*100, 5)
                if perc_done > 1 and perc_done % 2 == 0:
                    logger.info("Indexing progress: {}%".format(perc_done),
                                extra={"source": latest.name,
                                       "index": index_name,
                                       "percent_complete": perc_done,
                                       "completed": i,
                                       "total_size": total_docs})
            indexer.index_docs(es_client, True)
            logger.info("Completed indexing", extra={
                "latest_name": latest.name,
                "index": index_name
            })

            es_client.indices.refresh(index=index_name)
            logger.info("Refreshed index", extra={"index": index_name})

            indexer.flip_alias_to_latest(es_client, index_name, elasticsearch_alias)
            logger.info("Flipped alias to latest index", extra={
                "index": index_name,
                "alias": elasticsearch_alias
            })
        else:
            raise Exception("could not create the index")
    else:
        raise Exception("Could not ensure latest file on GCS")


if __name__ == "__main__":
    main()

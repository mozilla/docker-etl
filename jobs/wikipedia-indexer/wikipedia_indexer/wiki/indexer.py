import logging
from typing import List, Mapping, Any, Dict, Tuple
from elasticsearch import Elasticsearch

from . import settings
from .suggestion import Builder

logger = logging.getLogger('wikipedia-indexer')

# Maximum queue length
MAX_LENGTH = 5000

# queue
queue: List[Mapping[str, Any]] = []
suggestion_builder: Builder = Builder()


def enqueue(index_name: str, tpl: Tuple[Mapping[str, Any], ...]):
    op, doc = parse_tuple(index_name, tpl)
    queue.append(op)
    queue.append(doc)


def index_docs(client: Elasticsearch, force: bool):
    qlen = len(queue)
    if qlen > 0 and (qlen >= MAX_LENGTH or force is True):
        res = None
        try:
            res = client.bulk(operations=queue)
            if res["errors"] is not False:
                raise Exception(res["errors"])
        except Exception as e:
            print(res)
            raise e
        queue.clear()


def parse_tuple(index_name: str,
                tpl: Tuple[Mapping[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    op, doc = tpl
    if "index" not in op:
        raise Exception("invalid operation")
    # re use the wikipedia ID (this keeps the indexing
    # operation idempotent from our side)
    id = op["index"]["_id"]
    # TODO make this more generic
    op = {"index": {"_index": index_name, "_id": id}}
    suggestion = suggestion_builder.build(id, dict(doc))
    return op, suggestion


def get_index_name(file_name: str) -> str:
    return "-".join(file_name.split("-")[:2])


def ensure_index(client: Elasticsearch, index_name: str):
    indices_client = client.indices
    exists = indices_client.exists(index=index_name)
    if not exists:
        return indices_client.create(index=index_name,
                                     mappings=settings.SUGGEST_MAPPING,
                                     settings=settings.SUGGEST_SETTINGS)
    return {"acknowledged": True}


def flip_alias_to_latest(client: Elasticsearch, current_index: str, alias: str):
    # fetch previous index using alias so we know what to delete
    actions: List[Mapping[str, Any]] = [
        {"add": {"index": current_index, "alias": alias}}
    ]

    if client.indices.exists_alias(name=alias):
        indices = client.indices.get_alias(name=alias)
        for idx in indices:
            logger.info("adding index to be removed from alias", extra={
                "index": idx, "alias": alias})
            actions.append({"remove": {"index": idx, "alias": alias}})

    client.indices.update_aliases(actions=actions)

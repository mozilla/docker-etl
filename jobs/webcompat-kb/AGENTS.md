## Comments

Limit the amount of comments you put in the code to a strict minimum. You should almost never add comments, except sometimes on non-trivial code and function definitions if the arguments aren't self-explanatory.

Do not remove existing comments unless they are directly related to what you are changing.

## Reference docs

Before editing `data/` or running a `webcompat-*` command, read the matching doc in `docs/` (`data/sql/` is covered by `schema.md`).

## Running commands

Run `webcompat-*` commands only with `--no-write --stage`. Ask the user before running one without `--no-write`.

# Looker Utils

Looker utilities CLI tooling to perform cleanup and maintenance on the Looker instance and projects. 

## Usage

This script is intended to be run in a docker container.

Build the docker image with:

```sh
docker build -t looker-utils .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with:

```sh   
python3 -m looker_utils.main
```

This will list all available commands. Call specific commands to perform an operation:

```sh
python looker_utils/main.py --client-id xxx --client-secret xxx --instance-uri https://mozilla.cloud.looker.com delete-branches
```

### Give new users Standard access by default

New users get the Viewer role from their SAML group. `sync-standard-users` adds them to the
Looker-managed group `Standard Users - User`, except users in the `Viewer Only` group. Run it
on a schedule.

```sh
python looker_utils/main.py --client-id xxx --client-secret xxx --instance-uri https://mozilla.cloud.looker.com sync-standard-users --dry-run
```

To make someone a Viewer, add them to `Viewer Only` and remove them from `Standard Users - User`.

## Development

`flake8` and `black` are included for code linting and formatting:

```sh
pytest --black --flake8
```

or

```sh
flake8 looker_utils/ tests/
black --diff looker_utils/ tests/
```

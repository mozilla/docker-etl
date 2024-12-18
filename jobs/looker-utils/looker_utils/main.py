import click
import os
import looker_sdk
from looker_sdk import methods40 as methods
from looker_sdk import models40 as models 
from datetime import datetime, timedelta, timezone

class LookerConnection:

    def __init__(self, client_id, client_secret, url):

        if url[-1] == '/':
            self.url = url[:-1] + ':19999/api/3.0/'
        else:
            self.url = url + ':19999/api/3.0/'
        self.headers = self.connect(client_id, client_secret)
        
    def connect(self, client_id, client_secret):
        """Gets Access Token from Looker, setting token on LookerConnection"""

        login = requests.post(
            url=compose_url(self.url, 'login'),
            data={'client_id': client_id, 'client_secret': client_secret})

        try:
            access_token = login.json()['access_token']
            headers = {'Authorization': 'token {}'.format(access_token)}
        except KeyError:
            headers = None

        return headers
    
    def _get(self, endpoint, endpointid=None, subendpoint=None, subendpointid=None):
        
        r = requests.get(
            url=compose_url(self.url, endpoint, endpointid=endpointid, subendpoint=subendpoint, subendpointid=subendpointid),
            headers=self.headers)
       
        return r

    def _delete(self, endpoint, endpointid=None, subendpoint=None, subendpointid=None):

        r = requests.delete(
            url=compose_url(self.url, endpoint, endpointid=endpointid, subendpoint=subendpoint, subendpointid=subendpointid),
            headers=self.headers)
        
        return r

    def _patch(self, endpoint, endpointid=None, subendpoint=None, subendpointid=None, payload=None):

        r = requests.patch(
            url=compose_url(self.url, endpoint, endpointid=endpointid, subendpoint=subendpoint, subendpointid=subendpointid),
            headers=self.headers,
            json=payload)
        
        return r
    

def setup_sdk(client_id, client_secret, instance) -> methods.Looker40SDK:
    os.environ["LOOKERSDK_BASE_URL"] = instance
    os.environ["LOOKERSDK_API_VERSION"] = "4.0"
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true"
    os.environ["LOOKERSDK_TIMEOUT"] = "9000"
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret

    return looker_sdk.init40()


@click.group()
@click.option("--client_id", "--client-id", envvar="LOOKER_API_CLIENT_ID", required=True)
@click.option("--client_secret", "--client-secret", envvar="LOOKER_API_CLIENT_SECRET", required=True)
@click.option("--instance_uri", "--instance-uri", envvar="LOOKER_INSTANCE_URI", required=True)
@click.pass_context
def cli(ctx: dict, client_id: str, client_secret: str, instance_uri: str):
    sdk = setup_sdk(client_id, client_secret, instance_uri)
    ctx.obj["SDK"] = sdk
    pass


@cli.command()
@click.option("--project", help="Looker project name", multiple=True, default=["spoke-default", "looker-hub"])
@click.option("--n_days", "--n-days", help="Delete branches that haven't been updated within the last n days", default=180)
@click.option("--exclude", multiple=True, help="Branches to exclude from deletion", default=["main", "master"])
@click.pass_context
def delete_branches(ctx, project, n_days, exclude):
    sdk = ctx.obj["SDK"]
    date_cutoff = datetime.now().replace(tzinfo=timezone.utc) - timedelta(days=n_days)

    for lookml_project in project:
        branches = sdk.all_git_branches(project_id=lookml_project)

        for branch in branches:
            commit_date = datetime.fromtimestamp(branch.commit_at, timezone.utc)

            if commit_date < date_cutoff and not branch.name.startswith('dev') and branch.name not in exclude:
                print(f"Deleting branch {branch.name}, last commit on {commit_date}")


    # branch_count = 0

    # for branch in branches.json():
    #     commit_date = datetime.utcfromtimestamp(branch['commit_at'])
    #     branch_name = branch['name']
    #     if commit_date < date_cutoff and not branch_name.startswith('dev') and branch_name != 'master':
    #         print("Deleting branch '{}', last commit on {}".format(branch_name, commit_date.strftime('%Y-%m-%d')))
    #         delete_branch = conn._delete('projects',id,'git_branch',branch_name)
    #         branch_count += 1
            
    # print(branch_count)


if __name__ == "__main__":
    cli(obj={})
from prefect.deployments import Deployment
from etl_to_gcs_yellow_green_2019_2020 import github_gcs_dbt_etl
from prefect.filesystems import GitHub

github_block = GitHub.load("github-block")

deployment = Deployment.build_from_flow(
            flow=github_gcs_dbt_etl,
            name="github-gcs_dbt_etl",
            version="1.0",
            storage=github_block,
            entrypoint= 'etl_to_gcs_yellow_green_2019_2020.py:github_gcs_dbt_etl'
            )

if __name__=='__main__':
    deployment.apply()
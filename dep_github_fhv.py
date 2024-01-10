from prefect.deployments import Deployment
from etl_to_gcs_fhv_2019 import github_gcs_dbt_etl_fhv
from prefect.filesystems import GitHub

github_block = GitHub.load("github-block")

deployment = Deployment.build_from_flow(
            flow=github_gcs_dbt_etl_fhv,
            name="github-gcs_dbt_etl_fhv",
            version="1.0",
            storage=github_block,
            entrypoint= 'etl_to_gcs_fhv_2019.py:github_gcs_dbt_etl_fhv'
            )

if __name__=='__main__':
    deployment.apply()
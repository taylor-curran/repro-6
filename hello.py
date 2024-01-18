from prefect import flow


@flow
def hello_flow():
    print("Hello, world!")


if __name__ == "__main__":
    hello_flow.deploy(
        name="hello-dep",
        work_pool_name="my-k8s-pool",
        image="docker.io/taycurran/hello-m:demo",
        push=True,
        triggers=[
            {
                "match_related": {
                    "prefect.resource.id": "prefect-cloud.webhook.172ec9ec-164a-4706-9f6d-ce5390acdccc"
                },
                "expect": {"webhook.called"},
            }
        ],
    )

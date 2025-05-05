import dagster as dg


@dg.asset
def my_asset(context: dg.AssetExecutionContext):
    context.log.info("Hello, world!")
    return "Hi, there!"

@dg.asset(deps=[my_asset])
def my_other_asset(context: dg.AssetExecutionContext):
    context.log.info("Hello, world! from my_other_asset")
    return "Hello, world!"
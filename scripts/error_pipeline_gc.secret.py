from apache_beam.pipeline import PipelineOptions


options = PipelineOptions(
    runner="DataflowRunner",
    project="strokach-playground",
    temp_location="gs://strokach/dataflow_temp",
    sdk_location=os.path.expanduser(
        "~/workspace/beam/sdks/python/dist/apache-beam-2.15.0.dev0.tar.gz"
    ),
)

print(options)

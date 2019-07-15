import gc
import os
import sys
import tempfile
import time
import weakref

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.pipeline import PipelineOptions


class Foo:
    def __init__(self, name):
        self.name = name
        self._finalizer = weakref.finalize(
            self,
            lambda name: print("Finalizing Foo instance '{}'.".format(name)),
            self.name,
        )


def test_foo_gc():
    foo_a = Foo("a")

    gc.collect()
    refs = gc.get_referrers(foo_a)
    print("foo_a has {} referrers.".format(len(refs)))
    del foo_a
    gc.collect()
    print("foo_a has been deleted.")


def test_foo_with_pipeline_gc():
    foo_b = Foo("b")

    options = PipelineOptions(runner="DirectRunner")

    with tempfile.TemporaryDirectory() as tmpdir:
        with beam.Pipeline(options=options) as p:
            _ = (
                p
                | beam.Create(range(100))
                | beam.io.WriteToText(os.path.join(tmpdir, "pipeline-gc-test2"))
            )
            p._dep = foo_b
    del p

    gc.collect()
    refs = gc.get_referrers(foo_b)
    print("foo_b has {} referrers.".format(len(refs)))
    del foo_b
    gc.collect()
    print("foo_b has been deleted.")


if __name__ == "__main__":
    test_foo_gc()
    # [out] foo_a has 0 referrers.
    # [out] Finalizing Foo instance 'a'.
    # [out] foo_a has been deleted.

    test_foo_with_pipeline_gc()
    # [out] foo_b has 1 referrers.
    # [out] foo_b has been deleted.

    print("Sleeping...")
    time.sleep(3)
    # [out] Sleeping...
    # [out] Finalizing Foo instance 'b'.

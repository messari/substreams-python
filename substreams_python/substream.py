#!/usr/bin/env python3
import base64
import os
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Optional

import grpc
import pandas as pd
from google.protobuf.json_format import MessageToDict

DEFAULT_ENDPOINT = "api.streamingfast.io:443"


@dataclass
class SubstreamOutput:
    module_name: str
    snapshots: Optional[pd.DataFrame] = None
    data: Optional[pd.DataFrame] = None


class Substream:
    def __init__(
        self, spkg_path: str, token: Optional[str] = None, regenerate: bool = False
    ):
        if not Path(spkg_path).exists() or not spkg_path.endswith(".spkg"):
            raise Exception("Must provide a valid .spkg file!")
        if not Path("sf/substreams").exists() or regenerate:
            # generate sf/ directory
            command = f"""
            alias protogen_py="python3 -m grpc_tools.protoc --descriptor_set_in={spkg_path} --python_out=. --grpc_python_out=.";
            protogen_py sf/substreams/v1/substreams.proto;
            protogen_py sf/substreams/v1/package.proto;
            protogen_py sf/substreams/v1/modules.proto;
            protogen_py sf/substreams/v1/clock.proto;
            """
            subprocess.run(
                command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )

        from sf.substreams.v1.package_pb2 import Package
        from sf.substreams.v1.substreams_pb2_grpc import StreamStub

        with open(spkg_path, "rb") as f:
            self.pkg = Package()
            self.pkg.ParseFromString(f.read())
        self.token: Optional[str] = os.getenv("SUBSTREAMS_API_TOKEN") or token
        if not self.token:
            raise Exception("set SUBSTREAMS_API_TOKEN")

        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.access_token_call_credentials(self.token),
        )
        channel = grpc.secure_channel(DEFAULT_ENDPOINT, credentials=credentials)
        self.service: StreamStub = StreamStub(channel)
        package_meta = self.pkg.package_meta[0]
        self.version = package_meta.version
        self.name = package_meta.name

    def _parse_snapshot_deltas(self, snapshot: dict) -> list[dict]:
        return snapshot["deltas"].get("deltas", list())

    def _parse_data_deltas(self, data: dict) -> list[dict]:
        deltas = list()
        for output in data["outputs"]:
            store_deltas = output["storeDeltas"]
            if store_deltas:
                raw_deltas = store_deltas["deltas"]
                for delta in raw_deltas:
                    delta.update(data["clock"])
                    deltas.append(delta)
        return deltas

    @cached_property
    def supported_output_modules(self) -> list[str]:
        return [f.name for f in self.pkg.modules.ListFields()[0][1]]

    # TODO how do I type annotate this stuff?
    def poll(self, output_modules: list[str], start_block: int, end_block: int):
        from sf.substreams.v1.substreams_pb2 import STEP_IRREVERSIBLE, Request

        for module in output_modules:
            if module not in self.supported_output_modules:
                raise Exception(f"module '{module}' is not supported for {self.name}")

        stream = self.service.Blocks(
            Request(
                start_block_num=start_block,
                stop_block_num=end_block,
                fork_steps=[STEP_IRREVERSIBLE],
                modules=self.pkg.modules,
                output_modules=output_modules,
                initial_store_snapshot_for_modules=output_modules,
            )
        )
        raw_results = defaultdict(lambda: {"data": list(), "snapshots": list()})
        for response in stream:
            snapshot = MessageToDict(response.snapshot_data)
            data = MessageToDict(response.data)
            if snapshot:
                module_name: str = snapshot["moduleName"]
                snapshot_deltas = self._parse_snapshot_deltas(snapshot)
                raw_results[module_name]["snapshots"].extend(snapshot_deltas)
            if data:
                module_name: str = data["outputs"][0]["name"]
                data_deltas = self._parse_data_deltas(data)
                raw_results[module_name]["data"].extend(data_deltas)

        results = []
        for output_module in output_modules:
            result = SubstreamOutput(module_name=output_module)
            data_dict: dict = raw_results.get(output_module)
            for k, v in data_dict.items():
                df = pd.DataFrame(v)
                df["output_module"] = output_module
                df = df.rename(
                    columns={"newValue": "new_value", "oldValue": "old_value"}
                )
                if "new_value" in df:
                    df["new_value"] = df["new_value"].apply(
                        lambda x: base64.b64decode(x) if type(x) is not float else x
                    )
                if "old_value" in df:
                    df["old_value"] = df["old_value"].apply(
                        lambda x: base64.b64decode(x) if type(x) is not float else x
                    )
                setattr(result, k, df)
            results.append(result)
        return results

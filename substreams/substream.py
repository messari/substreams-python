#!/usr/bin/env python3
import base64
import os
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Optional

import grpc
import pandas as pd
from google.protobuf.descriptor_pb2 import DescriptorProto
from google.protobuf.json_format import MessageToDict

DEFAULT_ENDPOINT = "api.streamingfast.io:443"


def retrieve_class(module_name: str, class_name: str):
    module = __import__(module_name)
    return getattr(module, class_name)


def generate_pb2_files(spkg_path: str, commands: str) -> None:
    command = f"""
    alias protogen_py="python3 -m grpc_tools.protoc --descriptor_set_in={spkg_path} --python_out=. --grpc_python_out=.";
    {commands}
    unalias protogen_py;
    """
    subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


@dataclass
class SubstreamOutput:
    module_name: str
    snapshots: Optional[pd.DataFrame] = None
    data: Optional[pd.DataFrame] = None


class Substream:
    def __init__(
        self, spkg_path: str, token: Optional[str] = None, regenerate: bool = False
    ):
        self.token: Optional[str] = os.getenv("SUBSTREAMS_API_TOKEN", None) or token
        if not self.token:
            raise Exception("Must set SUBSTREAMS_API_TOKEN")
        if not Path(spkg_path).exists() or not spkg_path.endswith(".spkg"):
            raise Exception("Must provide a valid .spkg file!")
        if not Path("sf/substreams").exists() or regenerate:
            # generate sf/ directory
            commands = """
            protogen_py sf/substreams/v1/substreams.proto;
            protogen_py sf/substreams/v1/package.proto;
            protogen_py sf/substreams/v1/modules.proto;
            protogen_py sf/substreams/v1/clock.proto;
            """
            generate_pb2_files(spkg_path, commands)

        from sf.substreams.v1.package_pb2 import Package
        from sf.substreams.v1.substreams_pb2_grpc import StreamStub

        with open(spkg_path, "rb") as f:
            self.pkg = Package()
            self.pkg.ParseFromString(f.read())

        custom_proto_files: str = "".join(
            [
                f"protogen_py {file};"
                for file in self.proto_file_map.values()
                if not file.startswith("sf/") and not file.startswith("google/")
            ]
        )
        generate_pb2_files(spkg_path, custom_proto_files)

        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.access_token_call_credentials(self.token),
        )
        channel = grpc.secure_channel(DEFAULT_ENDPOINT, credentials=credentials)
        self.service: StreamStub = StreamStub(channel)
        package_meta = self.pkg.package_meta[0]
        self.version = package_meta.version
        self.name = package_meta.name

    def _class_from_module(self, module_name: str):
        # Retrieve out put type and import from module
        raw_output_type: str = self.output_modules.get(module_name)["output_type"]
        if raw_output_type.startswith("proto:"):
            output_type = raw_output_type.split(".")[-1]
        else:
            output_type = raw_output_type

        raw_module_path: str = self.proto_file_map.get(output_type)
        if raw_module_path is None:
            return None
        module_path: str = raw_module_path.split("/")[-1].split(".proto")[0]
        pb2_path: str = f"{module_path}_pb2"
        return retrieve_class(pb2_path, output_type)

    def _parse_from_string(self, raw: str, key: str, output_class) -> dict:
        decoded: bytes = base64.b64decode(raw)
        obj = {}
        if output_class is None:
            # PASS THE VALUE TYPE HERE TO SANITIZE BASE64 DECODE(bigint OR string)
            obj["value"] = str(decoded).split("b'")[1].split("'")[0]
            if ":" in key:
                split_key = key.split(":")
                obj[split_key[0]] = split_key[1]
        else:
            obj = output_class()
            obj.ParseFromString(decoded)
            obj = MessageToDict(obj)
        return obj

    def _parse_snapshot_deltas(self, snapshot: dict) -> list[dict]:
        module_name: str = snapshot["moduleName"]
        obj_class = self._class_from_module(module_name)
        return [
            self._parse_from_string(x["newValue"], x["key"], obj_class)
            for x in snapshot["deltas"].get("deltas", list())
        ]

    def _parse_data_deltas(self, data: dict) -> list[dict]:
        module_name: str = data["outputs"][0]["name"]
        obj_class = self._class_from_module(module_name)
        deltas = list()
        for output in data["outputs"]:
            store_deltas = output["storeDeltas"]
            if store_deltas:
                raw_deltas = store_deltas["deltas"]
                for delta in raw_deltas:
                    raw = delta["newValue"]
                    key = delta["key"]
                    d = self._parse_from_string(raw, key, obj_class)
                    d.update(data["clock"])
                    deltas.append(d)
        return deltas

    def _parse_data_outputs(self, data: dict) -> list[dict]:
        outputs = list()
        for output in data["outputs"]:
            map_output = output["mapOutput"]
            for key, items in map_output.items():
                if key == "items":
                    for item in items:
                        outputs.append(item)
        return outputs

    @cached_property
    def output_modules(self) -> dict[str, Any]:
        module_map = {}
        for module in self.pkg.modules.ListFields()[0][1]:
            map_output_type = module.kind_map.output_type
            store_output_type = module.kind_store.value_type
            if map_output_type != "":
                output_type = map_output_type
            else:
                output_type = store_output_type

            module_map[module.name] = {
                "is_map": map_output_type != "",
                "output_type": output_type,
                "initial_block": module.initial_block,
            }
        return module_map

    @cached_property
    def proto_file_map(self) -> dict[str, DescriptorProto]:
        name_map = {}
        for pf in self.pkg.proto_files:
            for mt in pf.message_type:
                name_map[mt.name] = pf.name
        return name_map

    # TODO how do I type annotate this stuff?
    def poll(
        self,
        output_modules: list[str],
        start_block: int,
        end_block: int,
        initial_snapshot=False,
    ):
        # TODO make this general
        from sf.substreams.v1.substreams_pb2 import STEP_IRREVERSIBLE, Request

        for module in output_modules:
            if module not in self.output_modules:
                raise Exception(f"module '{module}' is not supported for {self.name}")
            self._class_from_module(module)

        stream = self.service.Blocks(
            Request(
                start_block_num=start_block,
                stop_block_num=end_block,
                fork_steps=[STEP_IRREVERSIBLE],
                modules=self.pkg.modules,
                output_modules=output_modules,
                initial_store_snapshot_for_modules=output_modules
                if initial_snapshot
                else None,
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
                print("data block #", data["clock"]["number"])
                if self.output_modules[module]["is_map"]:
                    parsed = self._parse_data_outputs(data)
                else:
                    parsed = self._parse_data_deltas(data)
                module_name: str = data["outputs"][0]["name"]
                raw_results[module_name]["data"].extend(parsed)

        results = []
        for output_module in output_modules:
            result = SubstreamOutput(module_name=output_module)
            data_dict: dict = raw_results.get(output_module)
            for k, v in data_dict.items():
                df = pd.DataFrame(v)
                df["output_module"] = output_module
                setattr(result, k, df)
            results.append(result)
        return results

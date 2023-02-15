#!/usr/bin/env python3
import base64
import os, sys
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
from importlib import import_module

DEFAULT_ENDPOINT = "api.streamingfast.io:443"

def retrieve_class(module_name: str, class_name: str):
    module = import_module(module_name)
    return getattr(module, class_name)


def generate_pb2_files(spkg_path: str, commands: str, out_path: str) -> None:
    command = f"""
    alias protogen_py="python3 -m grpc_tools.protoc --descriptor_set_in={spkg_path} --python_out={out_path} --grpc_python_out={out_path}";
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
        self, spkg_path: str, token: Optional[str] = None, regenerate: bool = False, sf_out_dir: str = '.'
    ):
        self.token: Optional[str] = os.getenv("SUBSTREAMS_API_TOKEN", None) or token
        sf_dir_path = os.path.join(sf_out_dir, 'sf')
        if not Path(sf_out_dir).exists():
            os.makedirs(sf_out_dir)
        if not self.token:
            raise Exception("Must set SUBSTREAMS_API_TOKEN")
        if not Path(spkg_path).exists() or not spkg_path.endswith(".spkg"):
            raise Exception("Must provide a valid .spkg file!")
        if not Path(sf_dir_path).exists() or regenerate:
            # generate sf/ directory
            commands = """
            protogen_py sf/substreams/v1/substreams.proto;
            protogen_py sf/substreams/v1/package.proto;
            protogen_py sf/substreams/v1/modules.proto;
            protogen_py sf/substreams/v1/clock.proto;
            """
            generate_pb2_files(spkg_path, commands, out_path=sf_out_dir)

        sys.path.append(sf_out_dir)

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
        generate_pb2_files(spkg_path, custom_proto_files, out_path=sf_out_dir)

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
        pb2_path: str = raw_module_path.replace('.proto', '_pb2').replace('/', '.')
        return retrieve_class(pb2_path, output_type)

    def _parse_from_string(self, raw: str, key: str, output_class) -> dict:
        decoded: bytes = base64.b64decode(raw)
        obj = {}
        if output_class is None:
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

    def _parse_data_outputs(self, data: dict, module_name: str) -> list[dict]:
        outputs = list()
        for output in data["outputs"]:
            if "mapOutput" not in output or output["name"] != module_name:
                continue
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
            if map_output_type != "":
                output_type = map_output_type

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

    def poll(
        self,
        output_module: str,
        start_block: int,
        end_block: int,
        return_first_result: bool = False,
        return_first_result_function: Optional[callable] = None,
        initial_snapshot: bool = False,
        return_type: str = "df"
    ):

        return_dict_interface = {"data": [], "module_name": output_module, "data_block": str(start_block), "error": None}
        valid_return_types = ["dict", "df", "csv"]
        results = []
        raw_results = defaultdict(lambda: {"data": list(), "snapshots": list()})

        try:
            if isinstance(output_module, str) is False:
                raise Exception("The 'output_module' parameter passed into the poll() function is not a string.")
            return_type = return_type.lower()
            if return_type not in valid_return_types:
                return_type = "df"

            from sf.substreams.v1.substreams_pb2 import STEP_IRREVERSIBLE, Request
            if output_module not in self.output_modules:
                raise Exception(f"module '{output_module}' is not supported for {self.name}")
            if self.output_modules[output_module].get('is_map') is False:
                raise Exception(f"module '{output_module}' is not a map module")
            self._class_from_module(output_module)

            stream = self.service.Blocks(
                Request(
                    start_block_num=start_block,
                    stop_block_num=end_block,
                    fork_steps=[STEP_IRREVERSIBLE],
                    modules=self.pkg.modules,
                    output_modules=[output_module],
                    initial_store_snapshot_for_modules=[output_module]
                    if initial_snapshot
                    else None,
                )
            )

            for response in stream:
                snapshot = MessageToDict(response.snapshot_data)
                data = MessageToDict(response.data)
                session = MessageToDict(response.session)

                if session:
                    continue

                if snapshot:
                    snapshot_deltas = self._parse_snapshot_deltas(snapshot)
                    raw_results[output_module]["snapshots"].extend(snapshot_deltas)

                if data:
                    parsed = self._parse_data_outputs(data, output_module)
                    raw_results[output_module]["data"].extend(parsed)
                    return_dict_interface["data_block"] = data["clock"]["number"]
                    if len(parsed) > 0:
                        parsed = [dict(item, **{'block':data["clock"]["number"]}) for item in parsed]
                        if return_first_result is True:
                            if callable(return_first_result_function):
                                func_result = return_first_result_function(parsed)
                                if func_result is True:
                                    break
                                else:
                                    continue
                            else:
                                break
                    elif int(return_dict_interface["data_block"]) + 1 == end_block:
                        results = return_dict_interface

            if return_first_result is True and parsed:
                if return_type == "dict":
                    return_dict_interface["data"] = parsed
                if return_type == "df":
                    return_dict_interface["data"] = pd.DataFrame(parsed)
                if return_type == "csv":
                    return_dict_interface["data"] = pd.DataFrame(parsed).to_csv(index=False)
                results = return_dict_interface
            if return_first_result is False and raw_results:
                result = SubstreamOutput(module_name=output_module)
                data_dict: dict = raw_results.get(output_module)
                for k, v in data_dict.items():
                    df = pd.DataFrame(v)
                    df["output_module"] = output_module
                    setattr(result, k, df)
                results.append(result)
                if return_type == "dict":
                    return_dict_interface["data"] = results.to_dict()
                    results = return_dict_interface
                if return_type == "csv":
                    return_dict_interface["data"] = results.to_csv(index=False)
                    results = return_dict_interface
        except Exception as err:
            error_to_pass = err
            if isinstance(err, Exception):
                error_to_pass = str(err)
            return_dict_interface["error"] = error_to_pass
            results = return_dict_interface
        return results

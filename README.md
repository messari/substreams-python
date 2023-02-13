# substreams-python
WIP Python Interface for querying via substreams

## Requirements
1. Install the package:

```curl
pip3 install substreams
```
2. Generate a StreamingFast API Token
    - Can do so via this [link](https://substreams.streamingfast.io/reference-and-specs/authentication)
    - Be sure to save the Token as an env var
    ```curl
    export SUBSTREAMS_API_TOKEN="<MY_CUSTON_SPI_TOKEN>"
    ```

3. Retrieve the relevant `.spkg` file
    - Can retrieve officially released `.spkg` files from the instructions [here](https://github.com/streamingfast/substreams-playground/releases)
        - Alternatively, can refer to the official StreamingFast [README](https://github.com/streamingfast/substreams-playground/tree/master/consumers/python) or generate a Messari `.spkg` from these [build instructions](https://github.com/messari/substreams/tree/master/uniswap-v2)


## Instructions

### Instantiation
First import the `Substream` object and pass in the path to your `.spkg` file
```python
from substreams import Substream

sb = Substream("substreams-uniswap-v2-v0.1.0.spkg")
```

If you already have the `sf/substreams` repo generated (as per [step #3 in these instructions](https://github.com/streamingfast/substreams-playground/tree/master/consumers/python)), then the library will import from those files. Otherwise, the files will be generated dynamically.

### Polling
In order to poll the substream, you will need to call the `poll()` function on the `Substream` object. `poll()` requires that you specify a list of output modules, you can inspect what modules are available by calling `supported_output_modules`

```python
# View available modules on .spkg
print(sb.output_modules)

# Poll the module and return a list of SubstreamOutput objects in the order of the specified modules
result = sb.poll("map_swap_events", start_block=10000835, end_block=10000835+20000)
```

With the default inputs, this function outputs Pandas Dataframes after streaming all blocks between the start_block and end_block. However depending on how this function is called, a dict object is returned. The `poll()` function has a number of inputs

- output_modules
    - String of the output module to stream 
- start_block
    - Integer block number to start the polling
- end_block
    - Integer block number to end the polling. In theory, there is no max block number as any block number past chain head will stream the blocks in real time. Its recommended to use an end_block far off into the future if building a data app that will be streaming datain real time as blocks finalize, such as block 20,000,000
- return_first_result
    - Boolean value that if True will return data on the first block after the start block to have an applicable TX/Event.
    - Can be called recursively on the front end while incrementing the start_block to return data as its streamed rather than all data at once after streaming is completed
    - Defaults to False
- initial_snapshot
    - Boolean value, defaults to False
- return_first_result_function
    - Custom filtering function that accepts parsed data passed as an argument and returns as either True or False
    - Gets called when return_first_result is True and a block has applicable events/txs
    - If function resolves to True, the polling function returns the data from the block
    - If function resolves to False, the polling function continues iteration
- return_type
    - Specifies the type of value to return
    - Passing "df" returns the data in a pandas DataFrame
    - Passing "dict" returns in the format {"data": [], "module_name": String, "data_block": int, error: str | None}
    - Passing "csv" returns in the format {"data": String(CSV), "module_name": String, "data_block": int, error: str | None}

The result here is the default `SubstreamOutput` object, you can access both the `data` and `snapshots` dataframes by doing:

```python
# These will return pandas DataFrames
swap_events_result = results[0]
data_df = swap_events_result.data
snapshots_df = swap_events_result.snapshots
```

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
result = sb.poll(["store_swap_events"], start_block=10000835, end_block=10000835+20000)
```

With the default inputs, this function outputs Pandas Dataframes after streaming all blocks between the start_block and end_block. However depending on how this function is called, a dict object is returned. The `poll()` function has a number of inputs

- output_modules
    - List of strings of output modules to stream 
- start_block
    - Integer block number to start the polling
- end_block
    - Integer block number to end the polling. In theory, there is no max block number as any block number past chain head will stream the blocks in real time. Its recommended to use an end_block far off into the future if building a data app that will be streaming datain real time as blocks finalize, such as block 20,000,000
- stream_callback
    - An optional callback function to be passed into the polling function to execute when valid streamed data is received 
- return_first_result
    - Boolean value that if True will return data on the first block after the start block to have an applicable TX/Event.
    - Can be called recursively on the front end while incrementing the start_block to return data as its streamed rather than all data at once after streaming is completed
    - Defaults to False
    - If True, the data is returned in the format {"data": [], "module_name": String, "data_block": int}
- initial_snapshot
    - Boolean value, defaults to False
- highest_processed_block
    - Integer block number that is used in measuring indexing and processing progress, in cases where return_progress is True
    - Defaults to 0
- return_progress: bool = False,
    - Boolean value that if True returns progress in back processing
    - Defaults to False


The result here is the default `SubstreamOutput` object, you can access both the `data` and `snapshots` dataframes by doing:

```python
# These will return pandas DataFrames
swap_events_result = results[0]
data_df = swap_events_result.data
snapshots_df = swap_events_result.snapshots
```

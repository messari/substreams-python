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

# Poll the module and return a list of SubstreamOutput objects in the order of teh specified modules
result = sb.poll(["store_swap_events"], start_block=10000835, end_block=10000835+20000)
```

The result here is a `SubstreamOutput` object, you can access both the `data` and `snapshots` dataframes by doing:

```python
# These will return pandas DataFrames
swap_events_result = results[0]
data_df = swap_events_result.data
snapshots_df = swap_events_result.snapshots
```

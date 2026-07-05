# Test fixture contracts

`TestERC20.sol` is compiled once and its ABI+bytecode checked in at
`integration-tests/src/test/resources/contracts/TestERC20.json` - tests don't
need Foundry installed to run, only to regenerate the artifact after an edit.

Regenerate after editing the contract:

```
cd integration-tests/contracts
docker run --rm -v "$(pwd)":/workspace -w /workspace ghcr.io/foundry-rs/foundry:latest "forge build"
python3 -c "
import json
with open('out/TestERC20.sol/TestERC20.json') as f:
    d = json.load(f)
out = {'abi': d['abi'], 'bytecode': d['bytecode']['object']}
with open('../src/test/resources/contracts/TestERC20.json', 'w') as f:
    json.dump(out, f, indent=2)
"
```

import { loadRegistry } from "./lib/config.mjs";

const DEFAULT_SUI_RPC_URL = "https://fullnode.mainnet.sui.io:443";
const rpcUrl = process.env.SUI_RPC_URL ?? DEFAULT_SUI_RPC_URL;
const registry = await loadRegistry();
const issues = [];

await validateCoinMetadata({
  label: registry.quoteAsset.symbol,
  coinType: registry.quoteAsset.coinType,
  expectedDecimals: registry.quoteAsset.decimals,
  expectedSymbol: registry.quoteAsset.symbol,
});

for (const pair of registry.pairs) {
  await validateCoinMetadata({
    label: pair.id,
    coinType: pair.baseAsset.coinType,
    expectedDecimals: pair.baseAsset.decimals,
  });
  await validatePoolType(pair);
}

if (issues.length > 0) {
  console.error(`registry validation failed (${issues.length} issues)`);
  for (const issue of issues) {
    console.error(`- ${issue}`);
  }
  process.exitCode = 1;
} else {
  console.log(`registry validation ok (${registry.pairs.length} pairs)`);
}

async function validateCoinMetadata(input) {
  const metadata = await rpc("suix_getCoinMetadata", [input.coinType]);
  if (!metadata) {
    issues.push(`${input.label}: missing coin metadata for ${input.coinType}`);
    return;
  }
  if (metadata.decimals !== input.expectedDecimals) {
    issues.push(
      `${input.label}: decimals ${input.expectedDecimals} does not match on-chain ${metadata.decimals}`,
    );
  }
  if (input.expectedSymbol && metadata.symbol !== input.expectedSymbol) {
    issues.push(
      `${input.label}: symbol ${input.expectedSymbol} does not match on-chain ${metadata.symbol}`,
    );
  }
}

async function validatePoolType(pair) {
  const object = await rpc("sui_getObject", [pair.poolId, { showType: true }]);
  const type = object?.data?.type;
  if (!type) {
    issues.push(`${pair.id}: missing pool object type for ${pair.poolId}`);
    return;
  }

  const expectedType = `${registry.eventSources.orderInfoPackageIds[0]}::pool::Pool<${pair.baseAsset.coinType}, ${registry.quoteAsset.coinType}>`;
  if (normalizeType(type) !== normalizeType(expectedType)) {
    issues.push(
      `${pair.id}: pool type mismatch; expected ${expectedType}, got ${type}`,
    );
  }
}

async function rpc(method, params) {
  const response = await fetch(rpcUrl, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: method,
      method,
      params,
    }),
  });
  const body = await response.json().catch(() => null);
  if (!response.ok || body?.error) {
    throw new Error(
      `${method} failed: ${response.status} ${JSON.stringify(body?.error ?? body)}`,
    );
  }
  return body.result;
}

function normalizeType(type) {
  return type.replace(/\s+/g, "").replace(/0x[0-9a-fA-F]+/g, (address) => {
    return `0x${BigInt(address).toString(16)}`;
  });
}

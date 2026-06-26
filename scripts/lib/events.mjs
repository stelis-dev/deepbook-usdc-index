import { DEFAULT_GRAPHQL_URL } from "./config.mjs";

export async function queryGraphql(query, variables = {}) {
  const response = await fetch(
    process.env.SUI_GRAPHQL_URL ?? DEFAULT_GRAPHQL_URL,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query, variables }),
    },
  );
  const body = await response.json().catch(() => undefined);
  if (!response.ok) {
    throw new Error(
      `Sui GraphQL HTTP ${response.status}: ${JSON.stringify(body)}`,
    );
  }
  if (body?.errors?.length) {
    throw new Error(`Sui GraphQL error: ${JSON.stringify(body.errors)}`);
  }
  if (!body?.data) {
    throw new Error("Sui GraphQL returned no data");
  }
  return body.data;
}

export async function availableEventRange(eventType) {
  const result = await queryGraphql(
    `
    query DeepBookUsdcIndexEventRange($filters: [String!]) {
      serviceConfig {
        availableRange(type: "Query", field: "events", filters: $filters) {
          first { sequenceNumber timestamp }
          last { sequenceNumber timestamp }
        }
      }
    }
  `,
    { filters: [`type:${eventType}`] },
  );
  const range = result.serviceConfig?.availableRange;
  if (!range?.first || !range.last) {
    throw new Error(
      "Sui GraphQL event availableRange returned an unexpected shape",
    );
  }
  return {
    first: checkpointFact(range.first),
    last: checkpointFact(range.last),
  };
}

export async function scanOrderFilledEventsForCheckpointRange(input) {
  const maxPages = input.maxPages ?? Infinity;
  const maxRecords = input.maxRecords ?? Infinity;
  const filter = { type: input.eventType };
  const afterCheckpoint = inclusiveLowerBoundToAfterCheckpoint(
    input.fromCheckpoint,
  );
  if (afterCheckpoint !== undefined) {
    filter.afterCheckpoint = afterCheckpoint;
  }
  const beforeCheckpoint = inclusiveUpperBoundToBeforeCheckpoint(
    input.toCheckpoint,
  );
  if (beforeCheckpoint !== undefined) {
    filter.beforeCheckpoint = beforeCheckpoint;
  }
  const records = [];
  let cursor = null;
  let hasNextPage = true;
  let pageCount = 0;
  while (hasNextPage) {
    pageCount += 1;
    if (pageCount > maxPages) {
      return {
        records,
        hasMore: true,
        cursor,
        pageCount,
        stoppedReason: "max_pages",
      };
    }
    const result = await queryGraphql(
      `
      query DeepBookUsdcIndexOrderFilledEvents($first: Int!, $after: String, $filter: EventFilter!) {
        events(first: $first, after: $after, filter: $filter) {
          nodes {
            sequenceNumber
            timestamp
            transaction { digest effects { checkpoint { sequenceNumber timestamp } } }
            contents { type { repr } json }
          }
          pageInfo { hasNextPage endCursor }
        }
      }
    `,
      { first: input.pageSize, after: cursor, filter },
    );
    const connection = result.events;
    if (
      !connection ||
      !Array.isArray(connection.nodes) ||
      !connection.pageInfo
    ) {
      throw new Error(
        "Sui GraphQL OrderFilled event scan returned an unexpected shape",
      );
    }
    for (const node of connection.nodes) {
      const event = orderFilledEventFromEventNode(node);
      if (
        poolIdFromContents(event.contentsJson)?.toLowerCase() ===
        input.poolId.toLowerCase()
      ) {
        records.push(fillRecordFromEvent(event, input.pair));
      }
      if (records.length >= maxRecords) {
        return {
          records,
          hasMore: true,
          cursor: connection.pageInfo.endCursor ?? cursor,
          pageCount,
          stoppedReason: "max_records",
        };
      }
    }
    hasNextPage = connection.pageInfo.hasNextPage === true;
    cursor = connection.pageInfo.endCursor ?? null;
    if (hasNextPage && !cursor) {
      throw new Error(
        "Sui GraphQL OrderFilled event scan reported another page without a cursor",
      );
    }
  }
  return {
    records,
    hasMore: false,
    cursor: null,
    pageCount,
    stoppedReason: null,
  };
}

export async function availableTransactionRange() {
  const result = await queryGraphql(`
    query DeepBookUsdcIndexTransactionRange {
      serviceConfig {
        availableRange(type: "Query", field: "transactions") {
          first { sequenceNumber timestamp }
          last { sequenceNumber timestamp }
        }
      }
    }
  `);
  const range = result.serviceConfig?.availableRange;
  if (!range?.first || !range.last) {
    throw new Error(
      "Sui GraphQL transaction availableRange returned an unexpected shape",
    );
  }
  return {
    first: checkpointFact(range.first),
    last: checkpointFact(range.last),
  };
}

export async function firstPoolTransaction(poolId) {
  const result = await queryGraphql(
    `
    query DeepBookUsdcIndexFirstPoolTransaction($pool: SuiAddress!) {
      transactions(first: 1, filter: { affectedObject: $pool }) {
        nodes { digest effects { timestamp checkpoint { sequenceNumber timestamp } } }
      }
    }
  `,
    { pool: poolId },
  );
  const node = result.transactions?.nodes?.[0];
  if (!node) {
    return null;
  }
  return transactionShellFromNode(node);
}

export async function scanPoolTransactionsForCheckpointRange(input) {
  const filter = { affectedObject: input.poolId };
  const afterCheckpoint = inclusiveLowerBoundToAfterCheckpoint(
    input.fromCheckpoint,
  );
  if (afterCheckpoint !== undefined) {
    filter.afterCheckpoint = afterCheckpoint;
  }
  const beforeCheckpoint = inclusiveUpperBoundToBeforeCheckpoint(
    input.toCheckpoint,
  );
  if (beforeCheckpoint !== undefined) {
    filter.beforeCheckpoint = beforeCheckpoint;
  }
  return scanPoolTransactionsForward({ ...input, filter });
}

export async function scanPoolTransactionsBackward(input) {
  const maxPages = input.maxPages ?? Infinity;
  const maxRecords = input.maxRecords ?? Infinity;
  const filter = { affectedObject: input.poolId };
  if (input.beforeCheckpoint !== undefined) {
    const beforeCheckpoint = inclusiveUpperBoundToBeforeCheckpoint(
      input.beforeCheckpoint,
    );
    if (beforeCheckpoint !== undefined) {
      filter.beforeCheckpoint = beforeCheckpoint;
    }
  }
  const records = [];
  const transactions = [];
  let cursor = input.beforeCursor ?? null;
  let hasPreviousPage = true;
  let pageCount = 0;
  while (hasPreviousPage) {
    pageCount += 1;
    if (pageCount > maxPages) {
      return {
        records,
        transactions,
        hasMore: true,
        cursor,
        pageCount,
        stoppedReason: "max_pages",
      };
    }
    const result = await queryPoolTransactions({
      direction: "backward",
      filter,
      cursor,
      pageSize: input.pageSize,
    });
    records.push(
      ...(
        await eventsFromTransactionNodes(
          result.nodes,
          input.eventTypes,
          input.poolId,
        )
      ).map((event) => fillRecordFromEvent(event, input.pair)),
    );
    transactions.push(...result.nodes.map(transactionShellFromNode));
    if (records.length >= maxRecords) {
      return {
        records,
        transactions,
        hasMore: true,
        cursor: result.cursor ?? cursor,
        pageCount,
        stoppedReason: "max_records",
      };
    }
    hasPreviousPage = result.hasMore;
    cursor = result.cursor;
  }
  return {
    records,
    transactions,
    hasMore: false,
    cursor: null,
    pageCount,
    stoppedReason: null,
  };
}

async function scanPoolTransactionsForward(input) {
  const maxPages = input.maxPages ?? Infinity;
  const maxRecords = input.maxRecords ?? Infinity;
  const records = [];
  const transactions = [];
  let cursor = null;
  let hasNextPage = true;
  let pageCount = 0;
  while (hasNextPage) {
    pageCount += 1;
    if (pageCount > maxPages) {
      return {
        records,
        transactions,
        hasMore: true,
        cursor,
        pageCount,
        stoppedReason: "max_pages",
      };
    }
    const result = await queryPoolTransactions({
      direction: "forward",
      filter: input.filter,
      cursor,
      pageSize: input.pageSize,
    });
    records.push(
      ...(
        await eventsFromTransactionNodes(
          result.nodes,
          input.eventTypes,
          input.poolId,
        )
      ).map((event) => fillRecordFromEvent(event, input.pair)),
    );
    transactions.push(...result.nodes.map(transactionShellFromNode));
    if (records.length >= maxRecords) {
      return {
        records,
        transactions,
        hasMore: true,
        cursor: result.cursor ?? cursor,
        pageCount,
        stoppedReason: "max_records",
      };
    }
    hasNextPage = result.hasMore;
    cursor = result.cursor;
  }
  return {
    records,
    transactions,
    hasMore: false,
    cursor: null,
    pageCount,
    stoppedReason: null,
  };
}

async function queryPoolTransactions(input) {
  const pageVars =
    input.direction === "forward"
      ? { first: input.pageSize, after: input.cursor, last: null, before: null }
      : {
          first: null,
          after: null,
          last: input.pageSize,
          before: input.cursor,
        };
  const result = await queryGraphql(
    `
    query DeepBookUsdcIndexPoolTransactions(
      $first: Int
      $after: String
      $last: Int
      $before: String
      $filter: TransactionFilter!
    ) {
      transactions(first: $first, after: $after, last: $last, before: $before, filter: $filter) {
        nodes {
          digest
          effects {
            timestamp
            checkpoint { sequenceNumber timestamp }
            events(first: 50) {
              nodes { sequenceNumber contents { type { repr } json } }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
        pageInfo { hasNextPage endCursor hasPreviousPage startCursor }
      }
    }
  `,
    { ...pageVars, filter: input.filter },
  );
  const connection = result.transactions;
  if (!connection || !Array.isArray(connection.nodes) || !connection.pageInfo) {
    throw new Error(
      "Sui GraphQL pool transaction scan returned an unexpected shape",
    );
  }
  if (input.direction === "forward") {
    if (
      connection.pageInfo.hasNextPage === true &&
      typeof connection.pageInfo.endCursor !== "string"
    ) {
      throw new Error(
        "Sui GraphQL pool transaction scan reported a next page without a cursor",
      );
    }
    return {
      nodes: connection.nodes,
      hasMore: connection.pageInfo.hasNextPage === true,
      cursor: connection.pageInfo.endCursor ?? null,
    };
  }
  if (
    connection.pageInfo.hasPreviousPage === true &&
    typeof connection.pageInfo.startCursor !== "string"
  ) {
    throw new Error(
      "Sui GraphQL pool transaction scan reported a previous page without a cursor",
    );
  }
  return {
    nodes: connection.nodes,
    hasMore: connection.pageInfo.hasPreviousPage === true,
    cursor: connection.pageInfo.startCursor ?? null,
  };
}

async function eventsFromTransactionNodes(nodes, eventTypes, poolId) {
  const events = [];
  for (const node of nodes) {
    const shell = transactionShellFromNode(node);
    const eventConnection = node.effects?.events;
    if (
      !eventConnection ||
      !Array.isArray(eventConnection.nodes) ||
      !eventConnection.pageInfo
    ) {
      throw new Error(
        "Sui GraphQL transaction effects events returned an unexpected shape",
      );
    }
    const allEvents = [...eventConnection.nodes];
    let cursor = eventConnection.pageInfo.endCursor ?? null;
    let hasNextPage = eventConnection.pageInfo.hasNextPage === true;
    while (hasNextPage) {
      if (typeof cursor !== "string" || cursor.length === 0) {
        throw new Error(
          `Transaction ${shell.digest} reported more events without a cursor`,
        );
      }
      const nextPage = await queryTransactionEventPage(shell.digest, cursor);
      allEvents.push(...nextPage.nodes);
      cursor = nextPage.cursor;
      hasNextPage = nextPage.hasNextPage;
    }
    events.push(
      ...orderFilledEventsFromNodes(allEvents, eventTypes, poolId, shell),
    );
  }
  return events;
}

async function queryTransactionEventPage(digest, cursor) {
  const result = await queryGraphql(
    `
    query DeepBookUsdcIndexTransactionEventPage($digest: String!, $after: String) {
      transaction(digest: $digest) {
        effects {
          events(first: 50, after: $after) {
            nodes { sequenceNumber contents { type { repr } json } }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
  `,
    { digest, after: cursor },
  );
  const connection = result.transaction?.effects?.events;
  if (!connection || !Array.isArray(connection.nodes) || !connection.pageInfo) {
    throw new Error(
      `Sui GraphQL transaction event page returned an unexpected shape for ${digest}`,
    );
  }
  if (
    connection.pageInfo.hasNextPage === true &&
    typeof connection.pageInfo.endCursor !== "string"
  ) {
    throw new Error(
      `Transaction ${digest} reported another event page without a cursor`,
    );
  }
  return {
    nodes: connection.nodes,
    hasNextPage: connection.pageInfo.hasNextPage === true,
    cursor: connection.pageInfo.endCursor ?? null,
  };
}

function orderFilledEventsFromNodes(nodes, eventTypes, poolId, shell) {
  const typeSet = new Set(eventTypes);
  const normalizedPool = poolId.toLowerCase();
  const events = [];
  for (const event of nodes) {
    const eventType = event.contents?.type?.repr;
    if (!typeSet.has(eventType)) {
      continue;
    }
    if (
      poolIdFromContents(event.contents?.json)?.toLowerCase() !== normalizedPool
    ) {
      continue;
    }
    events.push({
      sequenceNumber: String(event.sequenceNumber),
      timestamp: shell.timestamp,
      transactionDigest: shell.digest,
      checkpoint: shell.checkpoint,
      eventType,
      contentsJson: event.contents?.json,
    });
  }
  return events;
}

export function fillRecordFromEvent(event, pair) {
  const amounts = quantitiesFromContents(event.contentsJson);
  const priceDecimal = priceDecimalFromAtomic(
    amounts.baseQuantityAtomic,
    pair.baseAsset.decimals,
    amounts.quoteQuantityAtomic,
    6,
  );
  return {
    schemaVersion: 1,
    pairId: pair.id,
    poolId: pair.poolId,
    eventType: event.eventType,
    transactionDigest: event.transactionDigest,
    eventSequenceNumber: event.sequenceNumber,
    checkpoint: event.checkpoint,
    timestamp: event.timestamp,
    baseQuantityAtomic: amounts.baseQuantityAtomic,
    quoteQuantityAtomic: amounts.quoteQuantityAtomic,
    price: {
      convention: "USDC_PER_BASE",
      decimal: priceDecimal,
      scale: 1_000_000_000,
    },
  };
}

export function sortFillRecords(records) {
  return [...records].sort((a, b) => {
    const time = Date.parse(a.timestamp) - Date.parse(b.timestamp);
    if (time !== 0) return time;
    const checkpoint = BigInt(a.checkpoint) - BigInt(b.checkpoint);
    if (checkpoint !== 0n) return checkpoint < 0n ? -1 : 1;
    const sequence =
      BigInt(a.eventSequenceNumber) - BigInt(b.eventSequenceNumber);
    if (sequence !== 0n) return sequence < 0n ? -1 : 1;
    return a.transactionDigest.localeCompare(b.transactionDigest);
  });
}

function orderFilledEventFromEventNode(node) {
  const checkpoint = node.transaction?.effects?.checkpoint;
  const eventType = node.contents?.type?.repr;
  if (
    node.sequenceNumber === undefined ||
    typeof node.timestamp !== "string" ||
    typeof node.transaction?.digest !== "string" ||
    !checkpoint ||
    typeof eventType !== "string"
  ) {
    throw new Error(
      "Sui GraphQL OrderFilled event returned an unexpected shape",
    );
  }
  return {
    sequenceNumber: String(node.sequenceNumber),
    timestamp: new Date(node.timestamp).toISOString(),
    transactionDigest: node.transaction.digest,
    checkpoint: checkpointFact(checkpoint).sequenceNumber,
    eventType,
    contentsJson: node.contents?.json,
  };
}

function transactionShellFromNode(node) {
  const checkpoint = node.effects?.checkpoint;
  if (
    typeof node.digest !== "string" ||
    typeof node.effects?.timestamp !== "string" ||
    !checkpoint
  ) {
    throw new Error(
      "Sui GraphQL pool transaction returned an unexpected shape",
    );
  }
  return {
    digest: node.digest,
    timestamp: new Date(node.effects.timestamp).toISOString(),
    checkpoint: checkpointFact(checkpoint).sequenceNumber,
  };
}

function checkpointFact(node) {
  if (node.sequenceNumber === undefined || typeof node.timestamp !== "string") {
    throw new Error("Sui GraphQL checkpoint returned an unexpected shape");
  }
  return {
    sequenceNumber: String(node.sequenceNumber),
    timestamp: new Date(node.timestamp).toISOString(),
  };
}

function poolIdFromContents(value) {
  if (!isRecord(value)) return undefined;
  const poolId = value.pool_id ?? value.poolId;
  return typeof poolId === "string" ? poolId : undefined;
}

function quantitiesFromContents(value) {
  if (!isRecord(value)) {
    throw new Error("OrderFilled contents are not an object");
  }
  const baseQuantityAtomic = stringQuantity(
    value.base_quantity ??
      value.baseQuantity ??
      value.base_qty ??
      value.baseQty,
  );
  const quoteQuantityAtomic = stringQuantity(
    value.quote_quantity ??
      value.quoteQuantity ??
      value.quote_qty ??
      value.quoteQty,
  );
  if (baseQuantityAtomic === undefined || quoteQuantityAtomic === undefined) {
    throw new Error(
      `OrderFilled contents do not expose base and quote quantities: ${JSON.stringify(value)}`,
    );
  }
  if (BigInt(baseQuantityAtomic) === 0n) {
    throw new Error("OrderFilled base quantity is zero");
  }
  return { baseQuantityAtomic, quoteQuantityAtomic };
}

function stringQuantity(value) {
  if (typeof value === "string" && /^[0-9]+$/.test(value)) return value;
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
    return String(value);
  return undefined;
}

function priceDecimalFromAtomic(
  baseAtomic,
  baseDecimals,
  quoteAtomic,
  quoteDecimals,
) {
  const decimalPlaces = 12n;
  const numerator =
    BigInt(quoteAtomic) * 10n ** BigInt(baseDecimals) * 10n ** decimalPlaces;
  const denominator = BigInt(baseAtomic) * 10n ** BigInt(quoteDecimals);
  const scaled = numerator / denominator;
  const whole = scaled / 10n ** decimalPlaces;
  const fractional = (scaled % 10n ** decimalPlaces)
    .toString()
    .padStart(Number(decimalPlaces), "0")
    .replace(/0+$/, "");
  return fractional ? `${whole}.${fractional}` : whole.toString();
}

function inclusiveLowerBoundToAfterCheckpoint(value) {
  const checkpoint = BigInt(value);
  return checkpoint === 0n ? undefined : Number(checkpoint - 1n);
}

function inclusiveUpperBoundToBeforeCheckpoint(value) {
  const checkpoint = BigInt(value);
  return checkpoint >= BigInt(Number.MAX_SAFE_INTEGER)
    ? undefined
    : Number(checkpoint + 1n);
}

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

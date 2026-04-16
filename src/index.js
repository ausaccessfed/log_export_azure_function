const { app } = require("@azure/functions");
const { TableClient } = require("@azure/data-tables");
const { DefaultAzureCredential } = require("@azure/identity");

const tableName = process.env.STATE_TABLE_NAME;
const partitionKey = "entityId";
const connectionString = process.env.STATE_TABLE_CONNECTION_STRING;
const apiUrl = process.env.API_HOST;
const apiKey = process.env.API_KEY;
const sentinelDceBaseUrl = process.env.SENTINEL_DCE_BASE_URL;
const sentinelDcrImmutableId = process.env.SENTINEL_DCR_IMMUTABLE_ID;
const sentinelStreamName = process.env.SENTINEL_STREAM_NAME;
const credential = new DefaultAzureCredential();
const SENTINEL_BATCH_SIZE = 200;

function apiURL(idpId, lastEpoch) {
    const nowEpoch = BigInt(Date.now()) * 1000000n;
    return `${apiUrl}/identity_providers/${idpId}/logs?date_start=${lastEpoch}&date_end=${nowEpoch}&type=audit`;
}

function headers() {
    return {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": `Bearer ${apiKey}`,
        "X-Api-Version": "1",
    };
}

function tableClient() {
    if (!connectionString) {
        throw new Error("Missing STATE_TABLE_CONNECTION_STRING app setting");
    }

    return TableClient.fromConnectionString(connectionString, tableName);
}

async function state(context, idpId) {
    const client = tableClient();
    // default to a day ago
    const currentEpoch = BigInt(Date.now() - 24 * 60 * 60 * 1000) * 1000000n

    try {
        const entity = await client.getEntity(partitionKey, idpId);
        return entity.lastEpoch || currentEpoch;
    } catch (error) {
        if (error.statusCode === 404) {
            context.log("no epoch found for idpId:", idpId, "returning current epoch");
            return currentEpoch;
        }

        throw error;
    }
}

async function upsertState(context, idpId, lastEpoch) {
    const client = tableClient();

    context.log("upserting epoch for idpId:", idpId, "epoch:", lastEpoch);

    await client.upsertEntity(
        {
            partitionKey,
            rowKey: idpId,
            lastEpoch,
        },
        "Merge"
    );
}

async function pullLogs(context, idpId, lastEpoch) {
    const result = await performRequest(context, apiURL(idpId, lastEpoch));
    return {
        logs: result.items,
        count: result.count,
        lastEpoch: result.etag,
    };
}

async function performRequest(context, url) {
    const response = await fetch(url, { headers: headers() });
    if (!response.ok) {
        throw new Error(`API request failed with status ${response.status}`);
    }
    return await response.json();
}

function sentinelIngestionUrl() {
    if (!sentinelDceBaseUrl || !sentinelDcrImmutableId || !sentinelStreamName) {
        throw new Error("Missing Sentinel settings. Required: SENTINEL_DCE_BASE_URL, SENTINEL_DCR_IMMUTABLE_ID, SENTINEL_STREAM_NAME");
    }

    const trimmedBase = sentinelDceBaseUrl.replace(/\/$/, "");
    return `${trimmedBase}/dataCollectionRules/${sentinelDcrImmutableId}/streams/${encodeURIComponent(sentinelStreamName)}?api-version=2023-01-01`;
}

function mapLogsToSentinelRecords(idpId, logs) {
    return logs.map((log) => {
        return {
            TimeGenerated: new Date(Number(BigInt(log.etag) / 1000000n)).toISOString(),
            entity_id: idpId,
            epoch_ns: log.etag,
            raw_json: JSON.stringify(log.log_data),
        };
    });
}

function epochNsToIso(epochNs) {
    if (!epochNs) {
        return "unknown";
    }

    try {
        return new Date(Number(BigInt(epochNs) / 1000000n)).toISOString();
    } catch (_error) {
        return "invalid-epoch";
    }
}

async function sendLogsToSentinel(context, idpId, logs) {
    if (!Array.isArray(logs) || logs.length === 0) {
        context.log("No logs to send to Sentinel");
        return;
    }

    const token = await credential.getToken("https://monitor.azure.com/.default");
    if (!token || !token.token) {
        throw new Error("Failed to acquire Azure Monitor token for Sentinel ingestion");
    }

    const records = mapLogsToSentinelRecords(idpId, logs);

    for (let i = 0; i < records.length; i += SENTINEL_BATCH_SIZE) {
        const batch = records.slice(i, i + SENTINEL_BATCH_SIZE);
        const response = await fetch(sentinelIngestionUrl(), {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${token.token}`,
            },
            body: JSON.stringify(batch),
        });

        if (!response.ok) {
            const body = await response.text();
            throw new Error(`Sentinel ingestion failed with status ${response.status}: ${body}`);
        }

        context.log(`Sent Sentinel batch ${Math.floor(i / SENTINEL_BATCH_SIZE) + 1} (${batch.length} records)`);
    }

    context.log(`Sent ${records.length} log records to Sentinel stream ${sentinelStreamName}`);
}

async function run(context) {
    const idpId = process.env.IDP_ID;
    const lastEpoch = await state(context, idpId);

    context.log("Starting search for logs from epoch:", lastEpoch, "datetime:", epochNsToIso(lastEpoch));

    const body = await pullLogs(context, idpId, lastEpoch);

    context.log(`Pulled ${body.count} logs for idpId: ${idpId} (new last epoch datetime: ${epochNsToIso(body.lastEpoch)})`);

    await sendLogsToSentinel(context, idpId, body.logs);

    await upsertState(context, idpId, body.lastEpoch);

    return body;
}

app.timer("sentinelIdpLogExport", {
    schedule: "0 */5 * * * *",
    handler: async (_timer, context) => {
        context.log("Timer fired at:", new Date().toISOString());
        await run(context);
    },
});

app.http("sentinelIdpLogExportRun", {
    methods: ["GET"],
    authLevel: "function",
    route: "sentinel/idp-log-export/run",
    handler: async (request, context) => {
        context.log("action fired at:", new Date().toISOString());

        const result = await run(context);

        return {
            status: 200,
            jsonBody: {
                message: "Run completed",
                ...result,
            },
        };
    },
});

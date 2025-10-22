const express = require('express');
const multer = require('multer');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, GetItemCommand, PutItemCommand, ScanCommand } = require('@aws-sdk/client-dynamodb');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const app = express();
const port = 3000;

// AWS clients
const s3 = new S3Client({ region: 'ap-south-1' });  // set your region
const dynamoDB = new DynamoDBClient({ region: 'ap-south-1' });

const upload = multer({ dest: 'uploads/' });

// Config
const BUCKET_NAME = 'deduplication-bucket-cloudproject'; // set your bucket
const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB fixed-size chunks
const CHUNKS_TABLE = 'chunks'; // unique chunk store
const FILES_TABLE = 'files';   // per-file manifests

// Helpers
const sha256Hex = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

async function ensureChunkStored(chunkHash, chunkBuffer) {
  // Fast path: check if chunk already known
  const existing = await dynamoDB.send(new GetItemCommand({
    TableName: CHUNKS_TABLE,
    Key: { hash: { S: chunkHash } }
  }));
  if (existing.Item) return { existed: true };

  // Upload chunk to S3 (idempotent if concurrent)
  const s3Key = `chunks/${chunkHash}`;
  await s3.send(new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: s3Key,
    Body: chunkBuffer,
  }));

  // Record chunk metadata (ignore if another writer just inserted)
  try {
    await dynamoDB.send(new PutItemCommand({
      TableName: CHUNKS_TABLE,
      Item: {
        hash: { S: chunkHash },
        size: { N: String(chunkBuffer.length) },
        s3Key: { S: s3Key },
        createdAt: { S: new Date().toISOString() }
      },
      ConditionExpression: 'attribute_not_exists(hash)'
    }));
  } catch (_) {
    // ConditionalCheckFailedException => another process wrote it; safe to ignore
  }

  return { existed: false };
}

async function putFileManifest({ fileId, fileName, size, chunkHashes, contentHash }) {
  await dynamoDB.send(new PutItemCommand({
    TableName: FILES_TABLE,
    Item: {
      fileId: { S: fileId },
      fileName: { S: fileName },
      size: { N: String(size) },
      uploadDate: { S: new Date().toISOString() },
      contentHash: { S: contentHash },
      chunkHashes: { L: chunkHashes.map(h => ({ S: h })) }
    }
  }));
}

// Serve static frontend
app.use(express.static(path.join(__dirname, '../frontend')));

// Root
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/index.html'));
});

// Upload with chunk-level deduplication
app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;
  if (!file) return res.status(400).send('No file uploaded.');

  const fileId = crypto.randomUUID();
  const fileName = file.originalname;

  let uploadedNewChunks = 0;
  let reusedChunks = 0;

  try {
    const readStream = fs.createReadStream(file.path);
    let buffered = Buffer.alloc(0);
    const chunkHashes = [];
    const contentHasher = crypto.createHash('sha256');
    let totalBytes = 0;

    await new Promise((resolve, reject) => {
      readStream.on('data', async (data) => {
        readStream.pause();
        try {
          contentHasher.update(data);
          totalBytes += data.length;
          buffered = Buffer.concat([buffered, data]);

          // Process fixed-size chunks
          while (buffered.length >= CHUNK_SIZE) {
            const chunk = buffered.subarray(0, CHUNK_SIZE);
            buffered = buffered.subarray(CHUNK_SIZE);

            const chunkHash = sha256Hex(chunk);
            const { existed } = await ensureChunkStored(chunkHash, chunk);
            if (existed) reusedChunks++; else uploadedNewChunks++;
            chunkHashes.push(chunkHash);
          }
          readStream.resume();
        } catch (e) {
          reject(e);
        }
      });

      readStream.on('end', async () => {
        try {
          // Tail chunk
          if (buffered.length > 0) {
            const chunk = buffered;
            const chunkHash = sha256Hex(chunk);
            const { existed } = await ensureChunkStored(chunkHash, chunk);
            if (existed) reusedChunks++; else uploadedNewChunks++;
            chunkHashes.push(chunkHash);
          }
          resolve();
        } catch (e) {
          reject(e);
        }
      });

      readStream.on('error', reject);
    });

    const contentHash = contentHasher.digest('hex');

    // Always store a file manifest (files are separate, storage is deduped)
    await putFileManifest({
      fileId,
      fileName,
      size: totalBytes,
      chunkHashes,
      contentHash
    });

    // Cleanup temp file
    try { fs.unlinkSync(file.path); } catch (_) {}

    res.status(200).json({
      message: 'File processed with chunk-level deduplication.',
      fileId,
      fileName,
      size: totalBytes,
      chunks: chunkHashes.length,
      uploadedNewChunks,
      reusedChunks
    });
  } catch (err) {
    console.error('Upload error:', err);
    try { if (fs.existsSync(file.path)) fs.unlinkSync(file.path); } catch (_) {}
    res.status(500).send(`Error processing file: ${err.message}`);
  }
});

// List files (from manifests)
app.get('/files', async (req, res) => {
  try {
    const data = await dynamoDB.send(new ScanCommand({ TableName: FILES_TABLE }));
    res.json(data.Items || []);
  } catch (err) {
    console.error('Error fetching files:', err);
    res.status(500).send('Error fetching files.');
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
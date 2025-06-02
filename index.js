const SFTPClient       = require('ssh2-sftp-client');
const { Storage }      = require('@google-cloud/storage');
const fs               = require('fs');
const path             = require('path');
const readline         = require('readline');
const iconv            = require('iconv-lite');
const { Transform }    = require('stream');

const sftp = new SFTPClient();
const storage = new Storage();
const bucket = storage.bucket('evergreen-import-storage');
const remoteDir = '/Test/Export/';

const filesToFetch = [
  'TMZip.txt',
  'SalesRep.txt',
  'CM.txt',
  'PRODUCTS_EVERGREEN.txt',
  'Evergreen_OH_Full.txt',
  'Evergreen_OD_Delta.txt'
];

// SFTP authentication from env or hard-coded
const sftpConfig = {
  host: 'datx.myevergreen.com',
  port: 22,
  username: process.env.USERNAME,
  password: process.env.PASSWORD,
  compress: false,
  keepaliveInterval: 10000000,
  readyTimeout: 20000000
};

// chunk threshold
const MAX_SIZE = 500 * 1024 * 1024; // 500 MB

/**
 * Transcode a local file (UTF-16LE/CRLF) → (UTF-8/LF) on the fly,
 * then upload it to GCS under "uploads/<datedName>".
 */
async function uploadFile(localPath, fileName) {
  // Build the destination name with today’s date
  const dated = `${fileName.replace('.txt','')}____${new Date()
    .toISOString().split('T')[0]}.txt`;

  // 1) Read the raw local file
  const rawStream = fs.createReadStream(localPath);

  // 2) Decode from UTF-16LE → JS string
  const decodeStream = iconv.decodeStream('utf16le');

  // 3) Normalize CRLF → LF
  const nlNormalizer = new Transform({
    transform(chunk, encoding, callback) {
      const asString = chunk.toString('utf8').replace(/\r\n/g, '\n');
      callback(null, Buffer.from(asString, 'utf8'));
    }
  });

  // 4) Encode JS string → UTF-8 bytes
  const encodeStream = iconv.encodeStream('utf8');

  // 5) Create a GCS write stream for "uploads/<dated>"
  const remoteWrite = bucket
    .file(`uploads/${dated}`)
    .createWriteStream({
      resumable: false,
      gzip: true,
      metadata: { contentType: 'text/csv; charset=utf-8' }
    });

  // 6) Pipe: raw → decode → normalize → encode → GCS
  await new Promise((resolve, reject) => {
    rawStream
      .pipe(decodeStream)
      .pipe(nlNormalizer)
      .pipe(encodeStream)
      .pipe(remoteWrite)
      .on('error', reject)
      .on('finish', resolve);
  });

  console.log(`✓ Uploaded (transcoded) ${dated}`);
}

/**
 * Splits a large CSV‐formatted .txt into multiple files,
 * always including the header as the first line in each chunk
 * and never slicing a row in half.
 */
async function splitAndUpload(localPath, fileName) {
  const fileStream = fs.createReadStream(localPath);
  const rl = readline.createInterface({ input: fileStream });

  let headerLine = null;
  let part = 0;
  let currentLines = [];
  let currentBytes = 0;

  // Helper to flush currentLines into a chunk
  async function flushChunk() {
    if (currentLines.length === 0) return;
    part++;
    const chunkName = `${fileName.replace('.txt','')}___part${part}.txt`;
    const chunkPath = path.join('/tmp', chunkName);

    // Write header + buffered lines to a temporary file
    await new Promise((resolve, reject) => {
      const ws = fs.createWriteStream(chunkPath);
      ws.on('error', reject);
      ws.on('finish', resolve);

      ws.write(headerLine + '\n');
      for (const line of currentLines) {
        ws.write(line + '\n');
      }
      ws.end();
    });

    // Use the new uploadFile (which transcodes) instead of bucket.upload
    await uploadFile(chunkPath, chunkName);
    fs.unlinkSync(chunkPath);

    // Reset buffers
    currentLines = [];
    currentBytes = Buffer.byteLength(headerLine + '\n', 'utf8');
  }

  for await (const line of rl) {
    if (headerLine === null) {
      // Capture the very first line as header
      headerLine = line;
      currentBytes = Buffer.byteLength(headerLine + '\n', 'utf8');
      continue;
    }

    // Measure this line’s byte length + newline
    const thisLineBytes = Buffer.byteLength(line + '\n', 'utf8');

    // If adding it would exceed MAX_SIZE, flush what we have so far
    if (currentBytes + thisLineBytes > MAX_SIZE) {
      await flushChunk();
    }

    // Buffer this line for the next chunk
    currentLines.push(line);
    currentBytes += thisLineBytes;
  }

  // Flush any remaining lines as the last chunk
  await flushChunk();
}

async function fetchFiles() {
  await sftp.connect(sftpConfig);
  console.log('✔ Connected to SFTP');

  for (const name of filesToFetch) {
    const remote = path.join(remoteDir, name);
    const local  = path.join('/tmp', name);

    console.log(`→ Downloading ${name}`);
    await sftp.fastGet(remote, local, {
      concurrency: 1,              // Number of concurrent reads
      chunkSize:   1024 * 1024,    // 1MB per chunk
      step: (transferred, chunk, total) => {
        const pct = Math.floor((transferred / total) * 100);
        if (pct % 10 === 0) console.log(`  • ${name}: ${pct}%`);
      }
    });

    const size = fs.statSync(local).size;
    console.log(`  ↓ Downloaded (${(size/1024).toFixed()} KB)`);

    if (size > MAX_SIZE) {
      console.log(`  ↯ Splitting ${name}`);
      await splitAndUpload(local, name);
    } else {
      console.log(`  ↑ Uploading ${name}`);
      await uploadFile(local, name);
    }

    fs.unlinkSync(local);
  }

  await sftp.end();
  console.log('✔ All done');
}

async function main() {
  try {
    // (Optional) log egress IP
    const res = await fetch('https://ipv4.icanhazip.com');
    console.log('Egress IP:', await res.text());

    await fetchFiles();
    process.exit(0);
  } catch (err) {
    console.error('✗ Error:', err);
    process.exit(1);
  }
}

main();

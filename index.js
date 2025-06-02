const SFTPClient    = require('ssh2-sftp-client');
const { Storage }   = require('@google-cloud/storage');
const fs            = require('fs');
const path          = require('path');
const readline      = require('readline');
const iconv         = require('iconv-lite');
const { Transform } = require('stream');

const sftp    = new SFTPClient();
const storage = new Storage();
const bucket  = storage.bucket('evergreen-import-storage');
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
 * Uploads a local file to GCS under "uploads/<datedName>". If
 * skipTranscode=true, it assumes the local file is already UTF-8/LF
 * and does a normal bucket.upload(). Otherwise, it streams:
 *   UTF-16LE/CRLF → (decode → normalize → encode) → GCS as UTF-8/LF.
 */
async function uploadFile(localPath, fileName, skipTranscode = false) {
  const dated = `${fileName.replace('.txt','')}____${new Date()
    .toISOString().split('T')[0]}.txt`;

  if (skipTranscode) {
    // Simply upload as UTF-8 (chunks are already well-formed)
    await bucket.upload(localPath, {
      destination: `uploads/${dated}`,
      gzip: true,
      metadata: { contentType: 'text/csv; charset=utf-8' }
    });
    console.log(`✓ Uploaded (no transcode) ${dated}`);
    return;
  }

  // Otherwise, transcode: raw UTF-16LE/CRLF → UTF-8/LF
  const rawStream = fs.createReadStream(localPath);

  // 1) Decode from UTF-16LE → JS strings
  const decodeStream = iconv.decodeStream('utf16le');

  // 2) Normalize CRLF → LF
  const nlNormalizer = new Transform({
    readableObjectMode: true,
    writableObjectMode: true,
    transform(chunk, encoding, callback) {
      // chunk is a JS string here
      const str = chunk.replace(/\r\n/g, '\n');
      callback(null, str);
    }
  });

  // 3) Encode JS string → UTF-8 bytes
  const encodeStream = iconv.encodeStream('utf8');

  // 4) GCS write stream
  const remoteWrite = bucket
    .file(`uploads/${dated}`)
    .createWriteStream({
      resumable: false,
      gzip: true,
      metadata: { contentType: 'text/csv; charset=utf-8' }
    });

  await new Promise((resolve, reject) => {
    rawStream
      .pipe(decodeStream)   // Buffer (UTF-16LE) → string
      .pipe(nlNormalizer)   // string (CRLF→LF) → string
      .pipe(encodeStream)   // string → Buffer (UTF-8)
      .pipe(remoteWrite)    // Buffer → GCS
      .on('error', reject)
      .on('finish', resolve);
  });

  console.log(`✓ Uploaded (transcoded) ${dated}`);
}

/**
 * Splits a large CSV-formatted .txt into multiple files,
 * always including the header as the first line in each chunk
 * and never slicing a row in half. Then uses uploadFile(..., true)
 * to upload each chunk “as is” (skipTranscode).
 */
async function splitAndUpload(localPath, fileName) {
  const fileStream = fs.createReadStream(localPath);
  const rl = readline.createInterface({ input: fileStream });

  let headerLine = null;
  let part = 0;
  let currentLines = [];
  let currentBytes = 0;

  // Flush currentLines into a chunk file, then upload (skipTranscode)
  async function flushChunk() {
    if (currentLines.length === 0) return;
    part++;
    const chunkName = `${fileName.replace('.txt','')}___part${part}.txt`;
    const chunkPath = path.join('/tmp', chunkName);

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

    // Upload chunk WITHOUT transcoding (it's already UTF-8/LF)
    await uploadFile(chunkPath, chunkName, true);
    fs.unlinkSync(chunkPath);

    currentLines = [];
    currentBytes = Buffer.byteLength(headerLine + '\n', 'utf8');
  }

  for await (const line of rl) {
    if (headerLine === null) {
      headerLine = line;
      currentBytes = Buffer.byteLength(headerLine + '\n', 'utf8');
      continue;
    }
    const thisLineBytes = Buffer.byteLength(line + '\n', 'utf8');
    if (currentBytes + thisLineBytes > MAX_SIZE) {
      await flushChunk();
    }
    currentLines.push(line);
    currentBytes += thisLineBytes;
  }

  // Final flush
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
      concurrency: 1,
      chunkSize:   1024 * 1024,
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
      await uploadFile(local, name); // no skipTranscode => do transcode
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

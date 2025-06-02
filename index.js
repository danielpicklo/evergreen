const SFTPClient = require('ssh2-sftp-client');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');

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

async function uploadFile(localPath, fileName) {
  const dated = `${fileName.replace('.txt','')}____${new Date().toISOString().split('T')[0]}.txt`;
  await bucket.upload(localPath, {
    destination: `uploads/${dated}`,
    gzip: true
  });
  console.log(`✓ Uploaded ${dated}`);
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

    // Upload and clean up
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
      concurrency: 1, // Number of concurrent downloads
      chunkSize: 1024 * 1024, // 1MB per chunk
      step: (transferred, chunk, total) => {
        const pct = Math.floor((transferred/total)*100);
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

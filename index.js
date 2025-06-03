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
 * Splits a large UTF-16LE/CRLF CSV into exactly two equal-row chunks.
 *
 * - First pass: count total data rows (excluding header).
 * - Second pass: write header + first half of rows → part1, 
 *                header + second half → part2.
 * - Upload each part (they’re already UTF-8/LF) with skipTranscode=true.
 */
async function splitByRowCountAndUpload(localPath, fileName) {
  // 1) First pass: count rows
  let totalRows = 0;
  let headerLine = null;

  // Build a decode+normalize stream for counting
  const countStream = fs.createReadStream(localPath)
    .pipe(iconv.decodeStream('utf16le'))
    .pipe(new Transform({
      readableObjectMode: true,
      writableObjectMode: true,
      transform(chunk, encoding, callback) {
        callback(null, chunk.replace(/\r\n/g, '\n'));
      }
    }));

  const rlCount = readline.createInterface({ input: countStream });
  let lineNum = 0;
  for await (const line of rlCount) {
    lineNum++;
    if (lineNum === 1) {
      headerLine = line;       // capture header
    } else {
      totalRows++;
    }
  }
  rlCount.close();

  if (!headerLine) {
    throw new Error(`No header found in ${fileName}`);
  }
  if (totalRows === 0) {
    // no data to split—just upload as a single file
    await uploadFile(localPath, fileName);
    return;
  }

  const half = Math.ceil(totalRows / 2);
  console.log(`  → ${fileName} has ${totalRows} rows; splitting into ${half} / ${totalRows - half}`);

  // 2) Second pass: write two chunk files
  const decodeNormalize2 = fs.createReadStream(localPath)
    .pipe(iconv.decodeStream('utf16le'))
    .pipe(new Transform({
      readableObjectMode: true,
      writableObjectMode: true,
      transform(chunk, encoding, callback) {
        callback(null, chunk.replace(/\r\n/g, '\n'));
      }
    }));

  const rlSplit = readline.createInterface({ input: decodeNormalize2 });

  let partIdx = 0;
  let writtenRows = 0; // tracks how many data-rows have been written to current part
  let writer = null;

  // Helper to open a new chunk file and write its header
  function openNewWriter() {
    partIdx++;
    const chunkName = `${fileName.replace('.txt','')}___part${partIdx}.txt`;
    const chunkPath = path.join('/tmp', chunkName);
    writer = fs.createWriteStream(chunkPath, { encoding: 'utf8' });
    writer.write(headerLine + '\n');
    return { chunkName, chunkPath };
  }

  // Kick off part1
  let { chunkName, chunkPath } = openNewWriter();

  let dataRowIdx = 0;
  for await (const line of rlSplit) {
    if (dataRowIdx === 0) {
      // already wrote header in openNewWriter()
      dataRowIdx++; 
      continue;
    }
    dataRowIdx++;

    // Determine which part this row belongs to
    if (writtenRows >= half) {
      // close previous part, upload it, then start part2
      writer.end();
      await uploadFile(chunkPath, chunkName, true);
      fs.unlinkSync(chunkPath);

      // start new writer for part2
      ({ chunkName, chunkPath } = openNewWriter());
      writtenRows = 0;
    }

    writer.write(line + '\n');
    writtenRows++;
  }

  // Close and upload the last part if it has any rows
  writer.end();
  await uploadFile(chunkPath, chunkName, true);
  fs.unlinkSync(chunkPath);

  console.log(`  ✓ Finished splitting ${fileName} into 2 parts`);
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
      await splitByRowCountAndUpload(local, name);
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

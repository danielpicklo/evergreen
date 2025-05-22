const SFTPClient = require('ssh2-sftp-client');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');

const sftp = new SFTPClient();
const storage = new Storage();
const bucket = storage.bucket('evergreen-import-storage');
const remoteDir = '/Test/Export/';
const filesToFetch = [
  'test0.txt',
  'test1.txt',
  'test2.txt'
];

// SFTP authentication from env or hard-coded
const sftpConfig = {
  host: 'us-east-1.sftpcloud.io',
  port: 22,
  username: 'daniel-evg-dev-test',
  password: 'P5S5f7OI30kcxnFpGVdt8KmEGzeuIuKb',
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

async function splitAndUpload(localPath, fileName) {
  const stream = fs.createReadStream(localPath, { highWaterMark: MAX_SIZE });
  let part = 0;
  let header = null;

  stream.on('data', chunk => {
    part++;
    const chunkName = `${fileName.replace('.txt','')}___part${part}.txt`;
    const chunkPath = path.join('/tmp', chunkName);
    const ws = fs.createWriteStream(chunkPath);

    // capture header once
    if (!header) {
      const firstLine = chunk.toString().split('\n')[0] + '\n';
      header = firstLine;
      ws.write(firstLine);
    }

    ws.write(chunk);
    ws.end();

    ws.on('finish', async () => {
      await uploadFile(chunkPath, chunkName);
      fs.unlinkSync(chunkPath);
    });
  });

  await new Promise((resolve, reject) => {
    stream.on('end', resolve);
    stream.on('error', reject);
  });
}

async function fetchFiles() {
  await sftp.connect(sftpConfig);
  console.log('✔ Connected to SFTP');

  for (const name of filesToFetch) {
    const remote = path.join(remoteDir, name);
    const local  = path.join('/tmp', name);

    console.log(`→ Downloading ${name}`);
    await sftp.fastGet(remote, local, {
      concurrency: 64,
      chunkSize: 1024 * 1024,
      step: (transferred, chunk, total) => {
        const pct = Math.floor((transferred/total)*100);
        if (pct % 5 === 0) console.log(`  • ${name}: ${pct}%`);
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

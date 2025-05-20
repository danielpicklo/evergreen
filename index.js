
const functions = require('@google-cloud/functions-framework');
const SFTPClient = require('ssh2-sftp-client');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');

//functions.http('caller', (req, res) => {

    /*const sftp = new SFTPClient();
    const storage = new Storage();
    const bucket = storage.bucket('evergreen-import-storage');
    const remoteDir = '/Test/Export/';
    const filesToFetch = [
        'TMZip.txt',
        'Products_Evergreen.txt',
        'SalesReps.txt', 
        'CM.txt',
        'Evergreen_OH_Full.txt',
        'Evergreen_OD_Delta.txt'
    ];*/
    
    const sftp = new SFTPClient();
    const storage = new Storage();
    const bucket = storage.bucket('evergreen-import-storage');
    const remoteDir = '/Test/Export/';
    const filesToFetch = [
        'test0.txt',
        'test1.txt',
        'test2.txt'
    ];

    //const { username, password } = process.env

    // SFTP authentication
    const sftpConfig = {
        host: 'us-east-1.sftpcloud.io',
        port: 22,
        username: 'daniel-evg-dev-test',
        password: 'P5S5f7OI30kcxnFpGVdt8KmEGzeuIuKb',
        compress: false,
        keepaliveInterval: 10000000,    // send a packet every 10 seconds
        readyTimeout: 20000000         // wait up to 20 seconds for connection
    };

    async function fetchFiles() {
        try {

            await sftp.connect(sftpConfig);

            console.log('Connected to SFTP');

            // Loop through each file name
            for (const fileName of filesToFetch) {

                console.log('Downloading file: ', fileName);
                //res.write(`Downloading File: ${fileName}`);

                const remoteFilePath = path.join(remoteDir, fileName);
                const localFilePath = path.join('/tmp', fileName); // Temp path for Cloud Run

                // Download the file from SFTP
                await sftp.fastGet(remoteFilePath, localFilePath, {
                    concurrency: 64,    // how many parallel reads
                    chunkSize: 1024 * 1024, // size of each read in bytes (default is 32 KB),
                    step: (total_transferred, chunk, total) => {
                        //console.log('total_transferred', total_transferred)
                        //console.log('chunk', chunk)
                        //console.log('total', total)

                        let percentage = Math.floor((total_transferred/total)*100)

                        percentage % 5 ? console.log('PERCENTAGE', percentage) : ''

                        //console.log('PERCENTAGE', (total_transferred/total)*100 + "%")
                    }
                });
                
                console.log(`Downloaded ${fileName}`);
                //res.write(`Downloaded File: ${fileName}`);

                // Check the file size and split if necessary
                const stats = fs.statSync(localFilePath);
                const fileSize = stats.size;

                console.log('File size: ', fileSize/1024 + " KB");

                if (fileSize > 500 * 1024 * 1024) {
                    console.log(`Splitting ${fileName} (size: ${fileSize} bytes)`);
                    //res.write(`Splitting File: ${fileName}`);
                    await splitAndUploadFile(localFilePath, fileName);
                } else {
                    console.log('File appropriate size');
                    //res.write(`File appropriate size: ${fileName}`);
                    await uploadFile(localFilePath, fileName);
                }

                // Clean up local file
                fs.unlinkSync(localFilePath);
                //res.write(`Removing File from /tmp Storage: ${fileName}`);
            }

            console.log('Closing SFTP connection');

            // Close the SFTP connection
            await sftp.end();
        } catch (err) {
            console.error('Error:', err);
        }
    }

        // Function to upload files to Google Cloud Storage
    async function uploadFile(localFilePath, fileName) {

        let datedFileName = `${fileName.replace('.txt', '')}_${new Date().toISOString().split('T')[0]}.txt`;

        try {
            await bucket.upload(localFilePath, {
                destination: `uploads/${datedFileName}`,
                gzip: true,
            });
            console.log(`Uploaded ${datedFileName} to Cloud Storage`);
            //res.write(`Uploaded File: ${fileName}`);
        } catch (err) {
            console.error(`Error uploading ${datedFileName}:`, err);
        }
    }

    // Function to split files larger than 500MB
    async function splitAndUploadFile(localFilePath, fileName) {
        const readStream = fs.createReadStream(localFilePath, { highWaterMark: 500 * 1024 * 1024 }); // 500MB chunks
        let chunkCount = 0;
        let headerWritten = false;

        // Split the file into chunks
        readStream.on('data', async (chunk) => {
            chunkCount++;
            const chunkFileName = `${fileName.replace('.txt', '')}___part${chunkCount}.txt`;

            // Write the chunk to a temporary file
            const chunkFilePath = path.join('/tmp', chunkFileName);
            const writeStream = fs.createWriteStream(chunkFilePath);

            if (!headerWritten) {
                // Retain the header for the first chunk
                const header = chunk.toString().split('\n')[0]; // Assuming CSV-like structure
                writeStream.write(header + '\n');
                headerWritten = true;
            }

            writeStream.write(chunk);
            writeStream.end();

            // Wait for the chunk file to be written before uploading
            writeStream.on('finish', async () => {
                //res.write(`Chunking File: ${fileName}`);
                await uploadFile(chunkFilePath, chunkFileName);
                fs.unlinkSync(chunkFilePath); // Clean up chunk file after upload
            });
        });

        // Handle any errors during file split
        readStream.on('error', (err) => {
            console.error('Error reading file during split:', err);
        });
    }

    // Trigger the fetch process
    fetchFiles();
    clearInterval(heartbeat);

    console.log('Deployment');

    /*res.send({
        data: {
            message: "Function Triggered"
        }
    });
});*/

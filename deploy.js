const FtpDeploy = require("ftp-deploy");
const ftpDeploy = new FtpDeploy();
const path = require('path');
require('dotenv').config();

// Get target from command line argument (default to 'crm')
const target = process.argv[2] || 'crm';

const targets = {
    crm: {
        remoteRoot: "/",  // FTP user CRM@ is already chrooted to /public_html/officetask/
        localRoot: path.join(__dirname, "public"),
        include: ["*", "**/*"]
    },
    website: {
        remoteRoot: process.env.FTP_REMOTE_ROOT_WEBSITE || "/public_html/web.coralinfratel.com/",
        localRoot: path.join(__dirname, "website"),
        include: ["*", "**/*"]
    },
    root: {
        remoteRoot: process.env.FTP_REMOTE_ROOT_CRM || "/public_html/officetask/",
        localRoot: __dirname,
        include: ["app.js", "package.json", ".env"]
    }
};

// Target selection
const currentTarget = targets[target];

if (!currentTarget) {
    console.error(`❌ Unknown target: ${target}. Use 'crm' or 'website'.`);
    process.exit(1);
}

const config = {
    user: process.env.FTP_USER,
    password: process.env.FTP_PASSWORD,
    host: process.env.FTP_HOST,
    port: 21,
    localRoot: currentTarget.localRoot,
    remoteRoot: currentTarget.remoteRoot,
    include: currentTarget.include,
    deleteRemote: false,
    forcePasv: true,
    sftp: false
};

console.log(`🚀 Starting Deployment to BigRock [Target: ${target.toUpperCase()}]...`);
console.log(`📡 Host: ${config.host}`);
console.log(`📂 Remote Path: ${config.remoteRoot}`);

ftpDeploy
    .deploy(config)
    .then((res) => console.log(`✅ Success! ${target.toUpperCase()} synced successfully.`))
    .catch((err) => {
        console.error("❌ Deployment Error:", err.message);
        console.log("💡 Tip: Check if your FTP credentials and folder path are correct in .env.");
    });

ftpDeploy.on("uploading", function (data) {
    console.log(`📤 [${data.transferredFileCount}/${data.totalFilesCount}] Uploading: ${data.relPath || 'unknown'}`);
});

ftpDeploy.on("uploaded", function (data) {
    console.log(`✅ Uploaded: ${data.relPath}`);
});

ftpDeploy.on("log", function (msg) {
    console.log(`📝 Log: ${msg}`);
});

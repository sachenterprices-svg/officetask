# Alternative Deployment: Bypassing Application Manager

Since cPanel's Application Manager is restricting the deployment, we can bypass it entirely and run the Node.js app directly using the server's backend processes (PM2 or Nohup) and route traffic using an `.htaccess` file.

### Step 1: Start the App Manually via Terminal
1. Open **cPanel**.
2. Go to the **Advanced** section and click on **Terminal**.
3. Run these exact commands, one by one:
   ```bash
   cd public_html/officetask
   npm install --production
   nohup node app.js > output.log 2>&1 &
   ```
   *(This starts your server in the background on port `8080`)*

### Step 2: Route Traffic (The .htaccess File)
We need to tell the server to route web traffic from `crm.coralinfratel.com/officetask` to your Node.js app running on port `8080`.

1. Go to **File Manager** and open `public_html/officetask`.
2. Click **+ File** at the top left to create a new file.
3. Name it exactly **`.htaccess`** (with the dot at the start) and click Create.
4. Right-click the new `.htaccess` file and click **Edit**.
5. Paste the following code:

```apache
RewriteEngine On
RewriteRule ^$ http://127.0.0.1:8080/ [P,L]
RewriteCond %{REQUEST_FILENAME} !-f
RewriteCond %{REQUEST_FILENAME} !-d
RewriteRule ^(.*)$ http://127.0.0.1:8080/$1 [P,L]
```

6. Click **Save Changes**.

---
**What this does:**
It ignores the restrictive Application Manager and manually forces the Apache server to act as a proxy, passing all requests to your running Node.js application.

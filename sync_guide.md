# Connecting Local Folder to BigRock Server (Auto-Sync)

To ensure that your future updates on this laptop are automatically uploaded to the BigRock server, we can use the **Git Version Control** feature in cPanel.

### Step 1: Initialize Git on your Laptop
1. Open terminal in `D:\ANTIGRAVITY\proposals`.
2. Run these commands:
   ```bash
   git init
   git add .
   git commit -m "Initial commit for server sync"
   ```

### Step 2: Create a Private Repository on BigRock
1. Log in to **cPanel**.
2. Search for **"Git™ Version Control"**.
3. Click **"Create"**.
4. Set **Repository Path** to `public_html/officetask`.
5. Set **Repository Name** to `bsnl-proposals`.
6. Click **Create**.

### Step 3: Connect Local to Server
1. cPanel will give you a "Remote URL" (looks like `username@coralinfratel.com:public_html/officetask`).
2. In your laptop terminal, run:
   ```bash
   git remote add origin [YOUR_REMOTE_URL]
   ```

### Step 4: Pushing Updates
Whenever you make a change in the future, just run these 3 commands:
```bash
git add .
git commit -m "Update description"
git push origin master
```
**This will automatically update the files on the BigRock server.**

---

> [!NOTE]
> Since this is a Node.js app, if you add new packages, you might still need to click "Run NPM Install" in the cPanel "Application Manager" once.

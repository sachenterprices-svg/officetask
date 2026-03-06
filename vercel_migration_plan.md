# Migration Plan: Split Deployment (BigRock Frontend + Vercel Backend)

Because BigRock Shared Hosting does not support Node.js, we cannot run our `server.js` (backend) there. However, we can still host our HTML/CSS/JS (frontend) on BigRock, and host our Node.js backend on a free serverless platform like Vercel.

## 1. Hosting the Backend (Node.js API) on Vercel
Vercel is a completely free and highly reliable platform for hosting Node.js functions.
- We will modify `server.js` slightly to work as a Vercel Serverless Function.
- We will create a `vercel.json` configuration file.
- We will deploy the backend to Vercel (e.g., `bsnl-proposals-api.vercel.app`).
- The Vercel backend will connect to your BigRock MySQL database remotely. *(Note: We must ensure Remote MySQL is enabled in cPanel)*.

## 2. Hosting the Frontend (HTML/UI) on BigRock
BigRock is excellent at hosting static files (HTML, CSS, JS).
- We will keep the `public_html/officetask` folder on BigRock.
- We will update the frontend JavaScript (`script.js`, `reports.js`, `auth.js`) to point API calls to the new Vercel backend URL instead of `/api/...`.
  - Old: `fetch('/api/users')`
  - New: `fetch('https://bsnl-proposals-api.vercel.app/api/users')`
- Users will still visit `crm.coralinfratel.com/officetask` in their browsers.

## 3. Remote MySQL Configuration
For Vercel to read/write data, the BigRock MySQL database must allow external connections.
- In cPanel, go to **Remote MySQL**.
- Add the IP wildcard `%` (or specific Vercel IPs) to allow the backend to connect.

## Next Steps
If you approve this plan, I will:
1. Prepare the codebase for Vercel deployment.
2. Guide you through creating a free Vercel account and deploying the backend.
3. Guide you on updating the frontend files on BigRock to point to the new backend.

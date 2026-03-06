# Deploying BSNL Proposal Generator to BigRock Server

Follow these steps to migrate your application and database from `D:\ANTIGRAVITY\proposals` to your BigRock hosting environment.

---

## Part 1: Exporting Local Database (MySQL)

Since your data is now in MySQL, you need to create a SQL dump file to move it.

1.  Open **Command Prompt** (cmd) or **PowerShell**.
2.  Run the following command to export the database (Enter your password when prompted):
    ```bash
    mysqldump -u root -p proposal_db > D:\ANTIGRAVITY\proposals\proposal_db_export.sql
    ```
    *This creates a file named `proposal_db_export.sql` in your project folder.*

---

## Part 2: Setting up BigRock Database

1.  Log in to your **BigRock cPanel**.
2.  Go to **MySQL® Databases**.
3.  Create a new database (e.g., `yourname_proposal_db`).
4.  Create a new Database User and set a strong password.
5.  Add the User to the Database with **ALL PRIVILEGES**.
6.  Go to **phpMyAdmin** from cPanel.
7.  Select your new database and click on the **Import** tab.
8.  Choose the `proposal_db_export.sql` file you created in Part 1 and click **Go**.

---

## Part 3: Uploading Files to BigRock

1.  **Prepare Files**: Go to `D:\ANTIGRAVITY\proposals` and highlight everything **EXCEPT** the `node_modules` folder.
2.  Right-click and **Compress to ZIP** (name it `proposals.zip`).
3.  In BigRock cPanel, go to **File Manager**.
4.  Navigate to your domain's folder (usually `public_html` or a specific subdirectory).
5.  **Upload** the `proposals.zip` file.
6.  Right-click the zip file and select **Extract**.

---

## Part 4: Configuring the Server

1.  In the File Manager, find the `.env` file.
2.  Right-click and **Edit**. Update the database details with the ones you created on BigRock:
    ```env
    DB_HOST=localhost
    DB_USER=your_bigrock_username
    DB_PASSWORD=your_bigrock_password
    DB_NAME=your_bigrock_database_name
    DB_PORT=3306
    PORT=8080
    ```
3.  **Install Dependencies**:
    - If BigRock provides a **Node.js Selector**, select it.
    - Click **Run NPM Install**.
    - Set the **Application Startup File** to `server.js`.
    - Set the **Application URL**.

---

## Part 5: Starting the App

If BigRock uses a managed Node.js environment, the app should start automatically once configured. If you are using a VPS:
1.  Connect via SSH (e.g., using Putty).
2.  Navigate to the folder and run:
    ```bash
    npm install
    npm start
    ```

> [!IMPORTANT]
> Make sure the BigRock Firewall allows traffic on the port specified in your `.env` (default 8080), or use a reverse proxy to point your domain to that port.

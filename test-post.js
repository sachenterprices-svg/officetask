const http = require('http');

const payload = JSON.stringify({
    customer_name: "Test Customer",
    mobile: "1234567890",
    email: "test@example.com",
    issue_type: "Auto-Support",
    description: "Test description",
    std_code: "01262",
    telephone_number: "242000"
});

const options = {
    hostname: 'localhost',
    port: 8080,
    path: '/api/complaints',
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'Content-Length': payload.length
    }
};

const req = http.request(options, (res) => {
    console.log('Status:', res.statusCode);
    let body = '';
    res.on('data', (chunk) => body += chunk);
    res.on('end', () => {
        console.log('Response:', body);
    });
});

req.on('error', (err) => {
    console.error('Error:', err.message);
});

req.write(payload);
req.end();

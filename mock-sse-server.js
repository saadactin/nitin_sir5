/**
 * Mock SSE Server for Testing API Sync
 * Port: 3000
 * Endpoint: /api/crm/stream
 */

const express = require('express');
const app = express();
const PORT = 3000;

// Enable CORS
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', '*');
    next();
});

// SSE endpoint - streams CRM data
app.get('/api/crm/stream', (req, res) => {
    console.log(`[${new Date().toISOString()}] New SSE connection from ${req.ip}`);
    
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    
    // Send connected event
    res.write(`event: connected\n`);
    res.write(`data: {"status": "connected", "message": "Successfully connected to CRM stream"}\n\n`);
    
    // Send initial batch of 10 records
    const initialRecords = [];
    for (let i = 1; i <= 10; i++) {
        initialRecords.push({
            id: i,
            customer_name: `Customer ${i}`,
            email: `customer${i}@example.com`,
            status: i % 2 === 0 ? 'active' : 'pending',
            total_orders: Math.floor(Math.random() * 100),
            total_spent: Math.floor(Math.random() * 10000) / 100,
            created_at: new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000).toISOString()
        });
    }
    
    res.write(`event: initial_data\n`);
    res.write(`data: ${JSON.stringify(initialRecords)}\n\n`);
    console.log(`  → Sent initial_data with ${initialRecords.length} records`);
    
    // Send new records every 5 seconds
    let recordId = 11;
    const interval = setInterval(() => {
        if (res.writableEnded) {
            clearInterval(interval);
            return;
        }
        
        const newRecord = {
            id: recordId++,
            customer_name: `Customer ${recordId}`,
            email: `customer${recordId}@example.com`,
            status: recordId % 2 === 0 ? 'active' : 'pending',
            total_orders: Math.floor(Math.random() * 100),
            total_spent: Math.floor(Math.random() * 10000) / 100,
            created_at: new Date().toISOString()
        };
        
        res.write(`event: new_data\n`);
        res.write(`data: ${JSON.stringify(newRecord)}\n\n`);
        console.log(`  → Sent new_data record #${newRecord.id}`);
    }, 5000);
    
    // Cleanup on disconnect
    req.on('close', () => {
        clearInterval(interval);
        console.log(`[${new Date().toISOString()}] SSE connection closed`);
    });
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        service: 'Mock SSE Server',
        timestamp: new Date().toISOString()
    });
});

// Start server
app.listen(PORT, () => {
    console.log('='.repeat(80));
    console.log('MOCK SSE SERVER STARTED');
    console.log('='.repeat(80));
    console.log(`Port: ${PORT}`);
    console.log(`SSE Endpoint: http://localhost:${PORT}/api/crm/stream`);
    console.log(`Health Check: http://localhost:${PORT}/health`);
    console.log('='.repeat(80));
    console.log('Waiting for connections...\n');
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n' + '='.repeat(80));
    console.log('SHUTTING DOWN MOCK SERVER');
    console.log('='.repeat(80));
    process.exit(0);
});

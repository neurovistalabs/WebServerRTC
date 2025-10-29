// Servidor de señalización WebSocket simple para WebRTC
const WebSocket = require('ws');

const server = new WebSocket.Server({ port: 8080 });
console.log('🚀 Servidor de señalización WebRTC iniciado en puerto 8080');

// Almacenar conexiones de clientes
const clients = new Set();

server.on('connection', function connection(ws, request) {
    const clientIP = request.socket.remoteAddress;
    console.log(`📱 Cliente conectado desde ${clientIP}`);
    
    clients.add(ws);
    
    ws.on('message', function incoming(data) {
        try {
            const message = JSON.parse(data.toString());
            console.log(`📨 Mensaje recibido de ${clientIP}:`, message.type);
            
            // Reenviar el mensaje a todos los otros clientes conectados
            clients.forEach(function each(client) {
                if (client !== ws && client.readyState === WebSocket.OPEN) {
                    client.send(data.toString());
                    console.log(`📤 Mensaje reenviado a otro cliente`);
                }
            });
            
        } catch (error) {
            console.error('❌ Error procesando mensaje:', error);
        }
    });
    
    ws.on('close', function() {
        clients.delete(ws);
        console.log(`📱 Cliente ${clientIP} desconectado`);
    });
    
    ws.on('error', function(error) {
        console.error(`❌ Error con cliente ${clientIP}:`, error);
        clients.delete(ws);
    });
});

console.log('✅ Servidor listo para recibir conexiones WebRTC');
console.log('💡 Los dispositivos pueden conectarse usando ws://[IP_DEL_SERVIDOR]:8080');
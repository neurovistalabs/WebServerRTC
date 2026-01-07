// Servidor de señalización WebRTC con Arquitectura de Islas
const WebSocket = require('ws');
const fs = require('fs');

// Cargar configuración
let config = { port: 8080 };
try {
    const rawConfig = fs.readFileSync('./server_config.json');
    config = JSON.parse(rawConfig);
    console.log('⚙️ Configuración cargada:', config);
} catch (e) {
    console.warn('⚠️ No se encontró server_config.json, usando valores por defecto');
}

const PORT = process.env.PORT || config.port || 8080;
const server = new WebSocket.Server({ port: PORT });
console.log(`🚀 Servidor de Islas WebRTC iniciado en puerto ${PORT}`);

// Estructura de datos:
// islands = {
//   'island_id': {
//      'device_id': { ws: socket, role: 'MOTHER'|'CHILD' }
//   }
// }
const islands = {};

server.on('connection', function connection(ws, request) {
    const clientIP = request.socket.remoteAddress;
    console.log(`📱 Nueva conexión desde ${clientIP}`);

    // Metadatos temporales hasta que haga login
    ws.isLogued = false;
    ws.islandId = null;
    ws.deviceId = null;
    ws.role = null;

    ws.on('message', function incoming(data) {
        try {
            const message = JSON.parse(data.toString());

            // 1. Manejo de LOGIN
            if (message.type === 'login') {
                handleLogin(ws, message);
                return;
            }

            // Si no está logueado, ignorar otros mensajes
            if (!ws.isLogued) {
                console.warn(`⚠️ Mensaje ignorado de ${clientIP}: No ha hecho login`);
                return;
            }

            console.log(`📨 [${ws.islandId}] ${ws.deviceId} (${ws.role}) -> ${message.type}`);

            // 2. Enrutamiento de mensajes
            routeMessage(ws, message);

        } catch (error) {
            console.error('❌ Error procesando mensaje:', error);
        }
    });

    ws.on('close', function () {
        if (ws.isLogued && ws.islandId && ws.deviceId) {
            console.log(`🔌 Desconectado: ${ws.deviceId} de isla ${ws.islandId}`);
            // Eliminar de la lista de islas
            if (islands[ws.islandId] && islands[ws.islandId][ws.deviceId]) {
                delete islands[ws.islandId][ws.deviceId];

                // Si la isla queda vacía, borrarla (opcional)
                if (Object.keys(islands[ws.islandId]).length === 0) {
                    delete islands[ws.islandId];
                }
            }
        } else {
            console.log(`🔌 Desconexión anónima de ${clientIP}`);
        }
    });
});

function handleLogin(ws, message) {
    const { islandId, deviceId, role } = message;

    if (!islandId || !deviceId) {
        console.error('❌ Login fallido: Faltan datos', message);
        return;
    }

    // Inicializar isla si no existe
    if (!islands[islandId]) {
        islands[islandId] = {};
        console.log(`🏝️ Nueva Isla creada: ${islandId}`);
    }

    // Registrar dispositivo
    islands[islandId][deviceId] = {
        ws: ws,
        role: role || 'CHILD'
    };

    // Actualizar metadatos del socket
    ws.isLogued = true;
    ws.islandId = islandId;
    ws.deviceId = deviceId;
    ws.role = role || 'CHILD';

    console.log(`✅ Login exitoso: ${deviceId} (${ws.role}) unido a isla ${islandId}`);

    // Enviar confirmación (opcional)
    ws.send(JSON.stringify({ type: 'login-success', message: 'Conectado a la isla' }));
}

function routeMessage(senderWs, message) {
    const island = islands[senderWs.islandId];
    if (!island) return;

    // CASO A: Llamar a Mamá (target implícito)
    if (message.target === 'MOTHER') {
        console.log(`📞 Buscando MADRE en isla ${senderWs.islandId}...`);
        let motherFound = false;

        Object.values(island).forEach(client => {
            if (client.role === 'MOTHER' && client.ws !== senderWs) {
                if (client.ws.readyState === WebSocket.OPEN) {
                    client.ws.send(JSON.stringify(message));
                    console.log(`📤 Mensaje reenviado a MADRE`);
                    motherFound = true;
                }
            }
        });

        if (!motherFound) {
            console.warn(`⚠️ No se encontró MADRE conectada en isla ${senderWs.islandId}`);
        }
        return;
    }

    // CASO A.2: BROADCAST EXPLÍCITO (Llamar a todos)
    if (message.target === 'BROADCAST') {
        console.log(`📢 BROADCAST solicitado en isla ${senderWs.islandId}`);
        Object.values(island).forEach(client => {
            if (client.ws !== senderWs && client.ws.readyState === WebSocket.OPEN) {
                client.ws.send(JSON.stringify(message));
                console.log(`📤 Broadcast a ${client.role} (${client.deviceId})`);
            }
        });
        return;
    }

    // CASO A.2: Routing por Rol (ej: targetRole = 'CHILD')
    if (message.targetRole) {
        console.log(`📞 Buscando rol ${message.targetRole} en isla ${senderWs.islandId}...`);
        let found = false;

        Object.values(island).forEach(client => {
            if (client.role === message.targetRole && client.ws !== senderWs) {
                if (client.ws.readyState === WebSocket.OPEN) {
                    client.ws.send(JSON.stringify(message));
                    console.log(`📤 Mensaje reenviado a ${client.role} (${client.deviceId})`);
                    found = true;
                }
            }
        });

        if (!found) {
            console.warn(`⚠️ No se encontraron dispositivos con rol ${message.targetRole} en isla ${senderWs.islandId}`);
        }
        return;
    }

    // CASO B: Target específico (si se implementa lógica de llamar a hijo específico)
    if (message.targetDeviceId) {
        const targetClient = island[message.targetDeviceId];
        if (targetClient && targetClient.ws.readyState === WebSocket.OPEN) {
            targetClient.ws.send(JSON.stringify(message));
            console.log(`📤 Mensaje enviado directo a ${message.targetDeviceId}`);
        }
        return;
    }

    // CASO C: Broadcast (comportamiento por defecto para señalización WebRTC simple)
    // Reenvía a TODOS los demás en la isla (útil si hay múltiples madres o hijos escuchando)
    Object.values(island).forEach(client => {
        if (client.ws !== senderWs && client.ws.readyState === WebSocket.OPEN) {
            client.ws.send(JSON.stringify(message));
            console.log(`📤 Broadcast a ${client.role}`);
        }
    });
}
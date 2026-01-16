// Servidor de señalización WebRTC con Arquitectura de Islas y Jerarquía Familiar
const WebSocket = require('ws');
const fs = require('fs');
const crypto = require('crypto');

// --- Configuración ---
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
console.log(`🚀 Servidor WebRTC (Familia/Roles) iniciado en puerto ${PORT}`);

// --- Persistencia (Mini-DB JSON) ---
const DEVICES_FILE = './devices.json';
const USERS_FILE = './users.json';

let devicesDB = {}; // { device_id: { brand, model, os, ... } }
let usersDB = {};   // { user_id: { name, familyId, role, deviceId } }

function loadDB() {
    try {
        if (fs.existsSync(DEVICES_FILE)) devicesDB = JSON.parse(fs.readFileSync(DEVICES_FILE));
        if (fs.existsSync(USERS_FILE)) usersDB = JSON.parse(fs.readFileSync(USERS_FILE));
        console.log(`📚 DB cargada: ${Object.keys(devicesDB).length} dispositivos, ${Object.keys(usersDB).length} usuarios`);
    } catch (e) {
        console.error('❌ Error cargando DB:', e);
    }
}
loadDB();

function saveDevices() {
    fs.writeFileSync(DEVICES_FILE, JSON.stringify(devicesDB, null, 2));
}

function saveUsers() {
    fs.writeFileSync(USERS_FILE, JSON.stringify(usersDB, null, 2));
}

// --- Lógica de Negocio ---
// connections = { socket: ws, deviceId: '...', userId: '...' }
// Se mantiene referencia en el objeto 'ws' directamente para acceso rápido
const clients = new Set();

server.on('connection', function connection(ws, request) {
    const clientIP = request.socket.remoteAddress;
    console.log(`📱 Nueva conexión física desde ${clientIP}`);

    ws.isAlive = true;
    ws.deviceId = null;
    ws.userId = null;
    ws.familyId = null;
    ws.role = null;

    clients.add(ws);

    ws.on('message', function incoming(data) {
        try {
            const message = JSON.parse(data.toString());

            switch (message.type) {
                case 'register_device':
                    handleDeviceRegister(ws, message);
                    break;
                case 'login_user':
                    handleUserLogin(ws, message);
                    break;
                case 'get_online_users':
                    handleGetOnlineUsers(ws);
                    break;
                case 'offer':
                case 'answer':
                case 'ice-candidate':
                case 'call_request':
                    routeSignalingMessage(ws, message);
                    break;
                default:
                    console.warn(`Mensaje desconocido de ${ws.deviceId}: ${message.type}`);
            }

        } catch (error) {
            console.error('❌ Error procesando mensaje:', error);
        }
    });

    ws.on('close', function () {
        console.log(`🔌 Desconectado: ${ws.deviceId || 'Anónimo'} (${ws.userId || '?'})`);
        clients.delete(ws);
        // Podríamos marcar last_seen en DB aquí
    });
});

// 1. Registro del Dispositivo (Hardware)
function handleDeviceRegister(ws, msg) {
    // msg: { type: 'register_device', deviceId: 'uuid', brand: '...', model: '...', os: '...' }
    const { deviceId, brand, model, os, appVersion } = msg;

    if (!deviceId) return;

    if (!devicesDB[deviceId]) {
        console.log(`🆕 Nuevo Dispositivo registrado: ${brand} ${model} (${deviceId})`);
        devicesDB[deviceId] = {
            deviceId,
            brand,
            model,
            os,
            appVersion,
            createdAt: Date.now()
        };
    } else {
        // Actualizar metadatos
        devicesDB[deviceId].brand = brand;
        devicesDB[deviceId].model = model;
        devicesDB[deviceId].os = os;
        devicesDB[deviceId].appVersion = appVersion;
        devicesDB[deviceId].lastSeen = Date.now();
    }
    saveDevices();

    ws.deviceId = deviceId;
    ws.send(JSON.stringify({ type: 'register_success', message: 'Dispositivo reconocido' }));
}

// 2. Login de Usuario (Persona + Rol + Familia)
function handleUserLogin(ws, msg) {
    // msg: { type: 'login_user', deviceId: '...', userId: '...', name: 'Carlos', familyId: 'FamA', role: 'FATHER' }
    const { deviceId, userId, name, familyId, role } = msg;

    if (!deviceId || !ws.deviceId) {
        ws.send(JSON.stringify({ type: 'error', message: 'Primero debe registrar dispositivo' }));
        return;
    }

    // Actualizar o Crear Usuario permanentemente
    usersDB[userId] = {
        userId,
        name,
        familyId,
        role,     // 'GRANDFATHER', 'FATHER', 'CHILD'
        deviceId  // Dispositivo actual
    };
    saveUsers();

    // Actualizar sesión en memoria RAM (Socket)
    ws.userId = userId;
    ws.familyId = familyId;
    ws.role = role.toUpperCase(); // Normalizar
    ws.userName = name;

    console.log(`✅ Login: ${name} (${ws.role}) en Familia ${familyId}`);

    ws.send(JSON.stringify({
        type: 'login_success',
        user: usersDB[userId],
        iceServers: config.ice_servers || []
    }));

    // Notificar a otros de la MISMA familia que alguien entró (opcional, refresca listas)
    broadcastOnlineUpdate(familyId);
}

// 3. Obtener Usuarios Online (Filtrado por Familia y Reglas)
function handleGetOnlineUsers(ws) {
    if (!ws.userId || !ws.familyId) return;

    const availableUsers = [];

    clients.forEach(client => {
        // 1. Descartar uno mismo
        if (client === ws) return;
        // 2. Verificar que esté autenticado
        if (!client.userId) return;

        // 3. REGLA DE ORO: Aislamiento Familiar
        if (client.familyId !== ws.familyId) return;

        // 4. REGLA DE JERARQUIA (Quién puede ver a quién)
        if (canCall(ws.role, client.role)) {
            availableUsers.push({
                userId: client.userId,
                name: client.userName,
                role: client.role,
                status: 'online'
            });
        }
    });

    ws.send(JSON.stringify({
        type: 'online_users',
        users: availableUsers
    }));
}

// 4. Enrutamiento de Señalización (P2P)
function routeSignalingMessage(senderWs, msg) {
    const targetUserId = msg.targetUserId;
    if (!targetUserId) return;

    // Buscar socket destino
    let targetWs = null;
    for (const client of clients) {
        if (client.userId === targetUserId) {
            targetWs = client;
            break;
        }
    }

    if (targetWs) {
        // Verificar reglas de familia nuevamente por seguridad
        if (targetWs.familyId !== senderWs.familyId) {
            console.warn(`🛑 Bloqueado intento llamada entre familias: ${senderWs.familyId} -> ${targetWs.familyId}`);
            return;
        }

        // Reenviar mensaje con senderId y senderName para que sepa quién llama
        msg.senderId = senderWs.userId;
        msg.senderName = senderWs.userName || "Alguien"; // Asegurar que siempre hay un nombre
        targetWs.send(JSON.stringify(msg));
        console.log(`📡 Señal ${msg.type}: ${senderWs.userName} -> ${targetWs.userName}`);
    } else {
        console.warn(`⚠️ Usuario destino no encontrado o offline: ${targetUserId}`);
    }
}

// --- Matriz de Permisos ---
function canCall(myRole, targetRole) {
    const ME = myRole.toUpperCase();
    const TARGET = targetRole.toUpperCase();

    // GRANDFATHER: Habla con todos
    if (ME === 'GRANDFATHER') return true;

    // FATHER / MOTHER: Habla con todos
    if (ME === 'FATHER' || ME === 'MOTHER') return true;

    // CHILD:
    if (ME === 'CHILD') {
        // Puede hablar con Abuelos y Padres
        if (TARGET === 'GRANDFATHER' || TARGET === 'FATHER' || TARGET === 'MOTHER') return true;

        // NO puede hablar con otros niños (según requerimiento original)
        // "Nietos no hablan sino con su abuelos o los otros padres"
        if (TARGET === 'CHILD') return false;
    }

    return false; // Por defecto restrictivo
}

function broadcastOnlineUpdate(familyId) {
    // Avisar a todos los de la familia que actualicen su lista
    clients.forEach(client => {
        if (client.familyId === familyId && client.userId) {
            // Enviamos un trigger para que ellos pidan la lista de nuevo
            client.send(JSON.stringify({ type: 'refresh_users' }));
        }
    });
}
// Servidor de señalización WebRTC con Arquitectura de Islas y Jerarquía Familiar
const WebSocket = require('ws');
const fs = require('fs');
const mongoose = require('mongoose');

// --- Configuración ---
let config = { port: 8080 };
try {
    const rawConfig = fs.readFileSync('./server_config.json');
    config = JSON.parse(rawConfig);
    console.log('⚙️ Configuración cargada:', config);
} catch (e) {
    console.warn('⚠️ No se encontró server_config.json, usando valores por defecto');
}

// --- Conexión a MongoDB Atlas ---
const MONGO_USER = "uservistaai";
const MONGO_PASS = encodeURIComponent("Diablo.2026...");
const MONGO_URI = `mongodb+srv://${MONGO_USER}:${MONGO_PASS}@cluster0.nbqlctb.mongodb.net/vistaai?retryWrites=true&w=majority`;

mongoose.connect(MONGO_URI)
    .then(() => console.log('🍃 Conectado a MongoDB Atlas (DB: vistaai)'))
    .catch(err => console.error('❌ Error conectando a MongoDB:', err));

// --- Esquema de MongoDB Unificado (vistaaicollection) ---
const IdentitySchema = new mongoose.Schema({
    deviceId: { type: String, required: true, unique: true },
    userId: { type: String, unique: true, sparse: true }, // ID único de usuario/persona
    name: String,
    familyId: { type: String, index: true },
    role: String,
    brand: String,
    model: String,
    os: String,
    appVersion: String,
    lastSeen: { type: Date, default: Date.now }
}, { collection: 'vistaaicollection' });

const Identity = mongoose.model('Identity', IdentitySchema);

// Sincronizar índices para asegurar limpieza
Identity.syncIndexes();

const PORT = process.env.PORT || config.port || 8080;
const server = new WebSocket.Server({ port: PORT });
console.log(`🚀 Servidor WebRTC (Jerarquía Unificada) iniciado en puerto ${PORT}`);

// --- Lógica de Negocio ---
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

    ws.on('message', async function incoming(data) {
        try {
            const message = JSON.parse(data.toString());
            console.log(`📥 [MSG] type: ${message.type} | user: ${ws.userName || '?'}`);

            switch (message.type) {
                case 'register_device':
                    await handleDeviceRegister(ws, message);
                    break;
                case 'login_user':
                    await handleUserLogin(ws, message);
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
                    console.warn(`Mensaje desconocido: ${message.type}`);
            }

        } catch (error) {
            console.error('❌ Error procesando mensaje:', error);
        }
    });

    ws.on('close', function () {
        console.log(`🔌 Desconectado: ${ws.userName || 'Anónimo'}`);
        clients.delete(ws);
    });
});

// 1. Registro del Dispositivo (Hardware)
async function handleDeviceRegister(ws, msg) {
    const { deviceId, brand, model, os, appVersion } = msg;
    if (!deviceId) return;
    ws.deviceId = deviceId;

    try {
        const updateData = {
            brand,
            model,
            os,
            appVersion,
            lastSeen: Date.now()
        };

        await Identity.findOneAndUpdate(
            { deviceId },
            { $set: updateData },
            { upsert: true, new: true }
        );

        console.log(`📱 Dispositivo reconocido: ${brand} ${model} (${deviceId})`);
        ws.send(JSON.stringify({ type: 'register_success', message: 'Dispositivo reconocido' }));
    } catch (err) {
        console.error('❌ Error registrando dispositivo:', err);
        ws.send(JSON.stringify({ type: 'error', message: 'Error en BD dispositivo' }));
    }
}

// 2. Login de Usuario (Vínculo con Persona)
async function handleUserLogin(ws, msg) {
    const { deviceId, userId, name, familyId, role } = msg;

    if (!deviceId || !ws.deviceId) {
        ws.send(JSON.stringify({ type: 'error', message: 'Falta registro de hardware' }));
        return;
    }

    try {
        const userData = {
            userId,
            name,
            familyId,
            role: role.toUpperCase(),
            lastSeen: Date.now()
        };

        const doc = await Identity.findOneAndUpdate(
            { deviceId },
            { $set: userData },
            { new: true }
        ).lean();

        // Actualizar sesión en socket
        ws.userId = userId;
        ws.familyId = familyId;
        ws.role = role.toUpperCase();
        ws.userName = name;

        console.log(`✅ Login: ${name} (${ws.role}) en Familia ${familyId}`);

        ws.send(JSON.stringify({
            type: 'login_success',
            user: doc,
            iceServers: config.ice_servers || []
        }));

        broadcastOnlineUpdate(familyId);
    } catch (err) {
        console.error('❌ Error en login:', err);
        ws.send(JSON.stringify({ type: 'error', message: 'Error en BD login' }));
    }
}

// 3. Obtener Usuarios Online (DEDUPLICADO Y FILTRADO)
function handleGetOnlineUsers(ws) {
    if (!ws.userId || !ws.familyId) return;

    // Usar un mapa para deduplicar por userId
    const userMap = new Map();

    clients.forEach(client => {
        // REGLAS:
        // 1. No soy yo mismo (por ID, no por socket)
        if (client.userId === ws.userId) return;
        // 2. Debe estar logueado
        if (!client.userId || !client.familyId) return;
        // 3. Misma familia
        if (client.familyId !== ws.familyId) return;
        // 4. Permisos de llamada
        if (!canCall(ws.role, client.role)) return;

        // Si ya está en el mapa, ignoramos (evita duplicados si hay reconexiones sucias)
        if (!userMap.has(client.userId)) {
            userMap.set(client.userId, {
                userId: client.userId,
                name: client.userName,
                role: client.role,
                status: 'online'
            });
        }
    });

    ws.send(JSON.stringify({
        type: 'online_users',
        users: Array.from(userMap.values())
    }));
}

// 4. Enrutamiento (P2P)
function routeSignalingMessage(senderWs, msg) {
    const targetUserId = msg.targetUserId;
    if (!targetUserId) return;

    // Buscar socket destino (el primero que encontremos del usuario)
    let targetWs = null;
    for (const client of clients) {
        if (client.userId === targetUserId) {
            targetWs = client;
            break;
        }
    }

    if (targetWs) {
        if (targetWs.familyId !== senderWs.familyId) return;

        msg.senderId = senderWs.userId;
        msg.senderName = senderWs.userName || "Alguien";
        targetWs.send(JSON.stringify(msg));
        console.log(`📡 Señal ${msg.type}: ${senderWs.userName} -> ${targetWs.userName}`);
    }
}

// --- Matriz de Permisos ---
function canCall(myRole, targetRole) {
    const ME = myRole.toUpperCase();
    const TARGET = targetRole.toUpperCase();

    if (ME === 'GRANDFATHER') return true;
    if (ME === 'FATHER' || ME === 'MOTHER') return true;
    if (ME === 'CHILD') {
        if (TARGET === 'GRANDFATHER' || TARGET === 'FATHER' || TARGET === 'MOTHER') return true;
        return false;
    }
    return false;
}

function broadcastOnlineUpdate(familyId) {
    clients.forEach(client => {
        if (client.familyId === familyId && client.userId) {
            client.send(JSON.stringify({ type: 'refresh_users' }));
        }
    });
}
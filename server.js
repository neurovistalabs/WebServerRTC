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

// --- Esquemas de MongoDB (Unificados en vistaaicollection) ---
const DeviceSchema = new mongoose.Schema({
    type: { type: String, default: 'device' },
    deviceId: { type: String, required: true },
    brand: String,
    model: String,
    os: String,
    appVersion: String,
    createdAt: { type: Date, default: Date.now },
    lastSeen: { type: Date, default: Date.now }
}, { collection: 'vistaaicollection' });

// Índice único PARCIAL: solo único para dispositivos
DeviceSchema.index({ deviceId: 1 }, { unique: true, partialFilterExpression: { type: 'device' } });

const UserSchema = new mongoose.Schema({
    type: { type: String, default: 'user' },
    userId: { type: String, required: true },
    name: String,
    familyId: { type: String, index: true },
    role: String,
    deviceId: String,
    lastLogin: { type: Date, default: Date.now }
}, { collection: 'vistaaicollection' });

// Índice único PARCIAL: solo único para usuarios
UserSchema.index({ userId: 1 }, { unique: true, partialFilterExpression: { type: 'user' } });

const Device = mongoose.model('Device', DeviceSchema);
const User = mongoose.model('User', UserSchema);

// Limpiar índices antiguos que causan conflicto y sincronizar los nuevos
async function syncAndCleanIndexes() {
    try {
        await Device.collection.dropIndexes();
        console.log('🧹 Índices antiguos eliminados de vistaaicollection');
        await Device.syncIndexes();
        await User.syncIndexes();
        console.log('✅ Nuevos índices parciales configurados');
    } catch (e) {
        console.warn('⚠️ Nota sobre índices:', e.message);
    }
}
syncAndCleanIndexes();

const PORT = process.env.PORT || config.port || 8080;
const server = new WebSocket.Server({ port: PORT });
console.log(`🚀 Servidor WebRTC (Familia/Roles) iniciado en puerto ${PORT}`);

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
            console.log(`📥 [MSG] type: ${message.type} | device: ${ws.deviceId || '?'}`);

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
                    console.warn(`Mensaje desconocido de ${ws.deviceId}: ${message.type}`);
            }

        } catch (error) {
            console.error('❌ Error procesando mensaje:', error);
        }
    });

    ws.on('close', function () {
        console.log(`🔌 Desconectado: ${ws.deviceId || 'Anónimo'} (${ws.userId || '?'})`);
        clients.delete(ws);
    });
});

// 1. Registro del Dispositivo (Hardware)
async function handleDeviceRegister(ws, msg) {
    const { deviceId, brand, model, os, appVersion } = msg;
    if (!deviceId) return;

    // Set deviceId early to avoid race conditions with login_user
    ws.deviceId = deviceId;

    try {
        const deviceData = {
            brand,
            model,
            os,
            appVersion,
            lastSeen: Date.now()
        };

        const device = await Device.findOneAndUpdate(
            { deviceId },
            { $set: deviceData },
            { upsert: true, new: true }
        ).lean(); // Usar lean() para obtener objeto plano

        console.log(`📱 Dispositivo actualizado/registrado: ${brand} ${model} (${deviceId})`);

        const response = { type: 'register_success', message: 'Dispositivo reconocido' };
        console.log(`  -> Enviando a cliente:`, response);
        ws.send(JSON.stringify(response));
    } catch (err) {
        console.error('❌ Error registrando dispositivo:', err);
        ws.send(JSON.stringify({ type: 'error', message: 'Error en base de datos' }));
    }
}

// 2. Login de Usuario (Persona + Rol + Familia)
async function handleUserLogin(ws, msg) {
    const { deviceId, userId, name, familyId, role } = msg;

    if (!deviceId || !ws.deviceId) {
        console.warn(`⚠️ Login fallido: Falta deviceId (msg: ${deviceId}, socket: ${ws.deviceId})`);
        ws.send(JSON.stringify({ type: 'error', message: 'Primero debe registrar dispositivo' }));
        return;
    }

    try {
        const userData = {
            userId,
            name,
            familyId,
            role: role.toUpperCase(),
            deviceId,
            lastLogin: Date.now()
        };

        const user = await User.findOneAndUpdate(
            { userId },
            { $set: userData },
            { upsert: true, new: true }
        ).lean(); // Usar lean() para obtener objeto plano

        // Actualizar sesión en memoria RAM (Socket)
        ws.userId = userId;
        ws.familyId = familyId;
        ws.role = role.toUpperCase();
        ws.userName = name;

        console.log(`✅ Login: ${name} (${ws.role}) en Familia ${familyId}`);

        const response = {
            type: 'login_success',
            user: user,
            iceServers: config.ice_servers || []
        };
        console.log(`  -> Enviando login_success a cliente ${name}`);
        ws.send(JSON.stringify(response));

        broadcastOnlineUpdate(familyId);
    } catch (err) {
        console.error('❌ Error en login de usuario:', err);
        ws.send(JSON.stringify({ type: 'error', message: 'Error en base de datos al autenticar' }));
    }
}

// 3. Obtener Usuarios Online (Filtrado por Familia y Reglas)
function handleGetOnlineUsers(ws) {
    if (!ws.userId || !ws.familyId) return;

    const availableUsers = [];

    clients.forEach(client => {
        if (client === ws) return;
        if (!client.userId) return;
        if (client.familyId !== ws.familyId) return;

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

    let targetWs = null;
    for (const client of clients) {
        if (client.userId === targetUserId) {
            targetWs = client;
            break;
        }
    }

    if (targetWs) {
        if (targetWs.familyId !== senderWs.familyId) {
            console.warn(`🛑 Bloqueado intento llamada entre familias: ${senderWs.familyId} -> ${targetWs.familyId}`);
            return;
        }

        msg.senderId = senderWs.userId;
        msg.senderName = senderWs.userName || "Alguien";
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

    if (ME === 'GRANDFATHER') return true;
    if (ME === 'FATHER' || ME === 'MOTHER') return true;

    if (ME === 'CHILD') {
        if (TARGET === 'GRANDFATHER' || TARGET === 'FATHER' || TARGET === 'MOTHER') return true;
        if (TARGET === 'CHILD') return false;
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
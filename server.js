require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 1e7,
  pingTimeout: 60000,
  pingInterval: 25000
});

app.use(express.json({ limit: '15mb' }));
app.use(express.urlencoded({ extended: true, limit: '15mb' }));

// ==========================================
// MONGODB CONNECTION & SCHEMAS
// ==========================================
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/go2go';
const ADMIN_PASS = process.env.ADMIN_PASS || 'G2FREE';

mongoose.connect(MONGO_URI)
  .then(() => console.log('✅ Connected to MongoDB permanently'))
  .catch(err => console.error('❌ MongoDB connection error:', err));

const riderSchema = new mongoose.Schema({
  name: { type: String, required: true, trim: true },
  mobile: { type: String, required: true, unique: true, trim: true },
  vehicleType: { type: String, required: true },
  vehicleNumber: { type: String, required: true, uppercase: true, trim: true },
  vehicleModel: { type: String, required: true },
  aadhaarImage: { type: String, required: true },
  vehicleImage: { type: String, required: true },
  profileImage: { type: String, default: '' },
  status: { type: String, enum: ['pending', 'approved', 'rejected'], default: 'pending' },
  isOnline: { type: Boolean, default: true },
  joinedAt: { type: Date, default: Date.now }
});

const customerSchema = new mongoose.Schema({
  name: { type: String, required: true, trim: true },
  mobile: { type: String, required: true, unique: true, trim: true },
  aadhaarImage: { type: String, default: '' },
  createdAt: { type: Date, default: Date.now }
});

const rideSchema = new mongoose.Schema({
  customerName: String,
  customerMobile: String,
  pickupAddress: String,
  pickupCoords: { lat: Number, lng: Number },
  dropAddress: String,
  dropCoords: { lat: Number, lng: Number },
  distanceKm: Number,
  fare: Number,
  status: { type: String, enum: ['searching', 'accepted', 'completed', 'cancelled'], default: 'searching' },
  riderId: { type: mongoose.Schema.Types.ObjectId, ref: 'Rider', default: null },
  riderName: String,
  riderMobile: String,
  riderVehicle: String,
  createdAt: { type: Date, default: Date.now },
  completedAt: { type: Date, default: null }
});

const Rider = mongoose.model('Rider', riderSchema);
const Customer = mongoose.model('Customer', customerSchema);
const Ride = mongoose.model('Ride', rideSchema);

async function computeRiderStats(riderId) {
  const now = new Date();
  const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  const dayOfWeek = now.getDay();
  const diffToMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
  const startOfWeek = new Date(now.getFullYear(), now.getMonth(), now.getDate() - diffToMonday, 0, 0, 0, 0);
  const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);

  const completedRides = await Ride.find({
    riderId: new mongoose.Types.ObjectId(riderId),
    status: 'completed'
  }).sort({ completedAt: -1, createdAt: -1 });

  const summarize = (rides) => {
    let totalRides = 0;
    let totalKm = 0;
    let totalEarnings = 0;
    for (const r of rides) {
      totalRides += 1;
      totalKm += Number(r.distanceKm || 0);
      totalEarnings += Number(r.fare || 0);
    }
    return {
      totalRides,
      totalKm: parseFloat(totalKm.toFixed(2)),
      totalEarnings: Math.round(totalEarnings)
    };
  };

  const getRideDate = (r) => new Date(r.completedAt || r.createdAt);

  return {
    daily: summarize(completedRides.filter(r => getRideDate(r) >= startOfDay)),
    weekly: summarize(completedRides.filter(r => getRideDate(r) >= startOfWeek)),
    monthly: summarize(completedRides.filter(r => getRideDate(r) >= startOfMonth))
  };
}

// ==========================================
// REAL-TIME SEQUENTIAL DISPATCH ENGINE
// ==========================================
const activeDispatches = new Map();
const connectedRiders = new Map();

async function dispatchNextRider(rideId) {
  try {
    const ride = await Ride.findById(rideId);
    if (!ride || ride.status !== 'searching') {
      clearDispatch(rideId);
      return;
    }

    const eligibleRiders = await Rider.find({ status: 'approved', isOnline: true }).sort({ joinedAt: 1 });

    if (eligibleRiders.length === 0) {
      const retryTimer = setTimeout(() => dispatchNextRider(rideId), 8000);
      activeDispatches.set(rideId, { timer: retryTimer, currentIndex: 0 });
      return;
    }

    let state = activeDispatches.get(rideId) || { timer: null, currentIndex: 0 };
    if (state.currentIndex >= eligibleRiders.length) {
      state.currentIndex = 0;
    }

    const targetRider = eligibleRiders[state.currentIndex];
    const targetRiderIdStr = targetRider._id.toString();
    const targetSocketId = connectedRiders.get(targetRiderIdStr);

    const ringPayload = {
      rideId: ride._id,
      pickupAddress: ride.pickupAddress,
      pickupCoords: ride.pickupCoords,
      dropAddress: ride.dropAddress,
      dropCoords: ride.dropCoords,
      distanceKm: ride.distanceKm,
      fare: ride.fare,
      timeoutSec: 20
    };

    io.to(`rider_${targetRiderIdStr}`).emit('incoming_ride_ring', ringPayload);
    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_ride_ring', ringPayload);
    }

    const nextIndex = (state.currentIndex + 1) % eligibleRiders.length;
    const timer = setTimeout(() => {
      io.to(`rider_${targetRiderIdStr}`).emit('ring_timeout', { rideId: ride._id });
      if (targetSocketId) {
        io.to(targetSocketId).emit('ring_timeout', { rideId: ride._id });
      }
      activeDispatches.set(rideId, { timer: null, currentIndex: nextIndex });
      dispatchNextRider(rideId);
    }, 20000);

    activeDispatches.set(rideId, { timer, currentIndex: nextIndex });
  } catch (err) {
    console.error('Dispatch error:', err);
  }
}

function clearDispatch(rideId) {
  const state = activeDispatches.get(rideId.toString());
  if (state && state.timer) clearTimeout(state.timer);
  activeDispatches.delete(rideId.toString());
}

io.on('connection', (socket) => {
  socket.on('register_rider_socket', async (riderId) => {
    if (!riderId) return;
    const idStr = riderId.toString();
    connectedRiders.set(idStr, socket.id);
    socket.riderId = idStr;
    socket.join(`rider_${idStr}`);
    await Rider.findByIdAndUpdate(idStr, { isOnline: true });

    const activeRide = await Ride.findOne({ status: 'searching' }).sort({ createdAt: 1 });
    if (activeRide && !activeDispatches.has(activeRide._id.toString())) {
      dispatchNextRider(activeRide._id.toString());
    }
  });

  socket.on('join_ride_room', (rideId) => socket.join(`ride_${rideId}`));
  socket.on('join_admin_room', (pass) => { if (pass === ADMIN_PASS) socket.join('admin_room'); });

  socket.on('accept_ride', async ({ rideId, riderId }) => {
    try {
      const ride = await Ride.findById(rideId);
      if (!ride || ride.status !== 'searching') {
        socket.emit('accept_failed', { message: 'Ride unavailable.' });
        return;
      }
      const rider = await Rider.findById(riderId);
      if (!rider || rider.status !== 'approved') {
        socket.emit('accept_failed', { message: 'Not verified yet.' });
        return;
      }

      clearDispatch(rideId);
      ride.status = 'accepted';
      ride.riderId = rider._id;
      ride.riderName = rider.name;
      ride.riderMobile = rider.mobile;
      ride.riderVehicle = `${rider.vehicleModel} (${rider.vehicleNumber})`;
      await ride.save();

      socket.emit('ride_confirmed_for_rider', {
        rideId: ride._id,
        customerName: ride.customerName,
        customerMobile: ride.customerMobile,
        pickupAddress: ride.pickupAddress,
        pickupCoords: ride.pickupCoords,
        dropAddress: ride.dropAddress,
        dropCoords: ride.dropCoords,
        distanceKm: ride.distanceKm,
        fare: ride.fare
      });
    } catch (err) {
      console.error('Accept ride error:', err);
    }
  });

  socket.on('complete_ride', async ({ rideId, riderId }) => {
    try {
      const ride = await Ride.findById(rideId);
      if (!ride || ride.status === 'completed') return;

      ride.status = 'completed';
      ride.completedAt = new Date();
      if (riderId && !ride.riderId) ride.riderId = riderId;
      await ride.save();

      const targetRiderId = (riderId || ride.riderId || '').toString();
      const stats = targetRiderId ? await computeRiderStats(targetRiderId) : null;

      const completionPayload = {
        rideId: ride._id,
        pickupAddress: ride.pickupAddress,
        dropAddress: ride.dropAddress,
        distanceKm: ride.distanceKm,
        fare: ride.fare,
        stats
      };

      if (targetRiderId) io.to(`rider_${targetRiderId}`).emit('ride_completed_popup', completionPayload);
      io.to(`ride_${rideId}`).emit('ride_completed_popup', completionPayload);
    } catch (err) {
      console.error('Complete ride error:', err);
    }
  });

  socket.on('disconnect', () => {
    if (socket.riderId && connectedRiders.get(socket.riderId) === socket.id) {
      connectedRiders.delete(socket.riderId);
    }
  });
});

// ==========================================
// REST API ROUTES
// ==========================================
app.get('/api/public/stats', async (req, res) => {
  try {
    const totalRiders = await Rider.countDocuments({});
    res.json({ totalRiders });
  } catch (err) {
    res.json({ totalRiders: 0 });
  }
});

app.post('/api/rider/auth', async (req, res) => {
  try {
    const { mode, name, mobile, vehicleType, vehicleNumber, vehicleModel, aadhaarImage, vehicleImage, profileImage } = req.body;
    if (!mobile || !/^\d{10}$/.test(mobile)) {
      return res.status(400).json({ error: 'Enter a valid 10-digit mobile number.' });
    }

    const existing = await Rider.findOne({ mobile });

    if (mode === 'login') {
      if (!existing) return res.status(404).json({ error: 'Number not found. Register first!' });
      const queuePosition = await Rider.countDocuments({ joinedAt: { $lte: existing.joinedAt } });
      const stats = await computeRiderStats(existing._id);
      return res.json({ rider: existing, queuePosition, stats });
    }

    if (existing) {
      return res.status(400).json({ error: 'Already registered! Tap Login.' });
    }

    if (!name || !vehicleType || !vehicleNumber || !vehicleModel || !aadhaarImage || !vehicleImage) {
      return res.status(400).json({ error: 'Fill all fields & upload Aadhaar + Vehicle photo.' });
    }

    const newRider = await Rider.create({
      name,
      mobile,
      vehicleType,
      vehicleNumber,
      vehicleModel,
      aadhaarImage,
      vehicleImage,
      profileImage: profileImage || ''
    });

    const queuePosition = await Rider.countDocuments({ joinedAt: { $lte: newRider.joinedAt } });

    io.to('admin_room').emit('new_rider_verification', {
      id: newRider._id,
      name: newRider.name,
      mobile: newRider.mobile,
      vehicleNumber: newRider.vehicleNumber
    });

    const stats = await computeRiderStats(newRider._id);
    res.json({ rider: newRider, queuePosition, stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/rider/stats/:riderId', async (req, res) => {
  try {
    const stats = await computeRiderStats(req.params.riderId);
    res.json({ stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/rider/toggle-online', async (req, res) => {
  try {
    const { riderId, isOnline } = req.body;
    const rider = await Rider.findByIdAndUpdate(riderId, { isOnline }, { new: true });
    res.json({ rider });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/customer/waitlist', async (req, res) => {
  try {
    const { name, mobile } = req.body;
    if (!name || !mobile || !/^\d{10}$/.test(mobile)) {
      return res.status(400).json({ error: 'Enter Name & 10-digit Mobile.' });
    }
    const existing = await Customer.findOne({ mobile });
    if (existing) return res.json({ message: 'You are already on the VIP list! 🎉' });
    await Customer.create({ name, mobile });
    res.json({ message: 'VIP Spot Locked! We will alert you at launch. 🚀' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/riders', async (req, res) => {
  const { password } = req.body;
  if (password !== ADMIN_PASS) return res.status(401).json({ error: 'Wrong Password' });
  const riders = await Rider.find().sort({ joinedAt: 1 });
  res.json({ riders });
});

app.post('/api/admin/verify-rider', async (req, res) => {
  const { password, riderId, status } = req.body;
  if (password !== ADMIN_PASS) return res.status(401).json({ error: 'Unauthorized' });
  const rider = await Rider.findByIdAndUpdate(riderId, { status }, { new: true });
  io.to(`rider_${riderId}`).emit('verification_status_changed', { status });
  res.json({ rider });
});

// ==========================================
// FRONTEND SINGLE-PAGE APPLICATION (STICKER-DRIVEN VIBE)
// ==========================================
app.get('*', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Go²Go • 0% Commission • ₹7/km</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="/socket.io/socket.io.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@500;700;900&display=swap" rel="stylesheet">
  <style>
    body {
      font-family: 'Outfit', sans-serif;
      background: radial-gradient(circle at 20% 10%, #0f172a, #020617 75%);
      color: #f8fafc;
      min-height: 100vh;
      overflow-x: hidden;
    }

    /* Ambient Neon Orbs */
    .orb-1 {
      position: fixed;
      width: 420px; height: 420px;
      background: radial-gradient(circle, rgba(16,185,129,0.25) 0%, transparent 70%);
      top: -80px; left: -60px; pointer-events: none; z-index: 0;
    }
    .orb-2 {
      position: fixed;
      width: 420px; height: 420px;
      background: radial-gradient(circle, rgba(59,130,246,0.22) 0%, transparent 70%);
      bottom: -80px; right: -60px; pointer-events: none; z-index: 0;
    }

    /* Characterized Sticker Styles (Die-cut white border + 3D float) */
    .sticker {
      filter: drop-shadow(0 12px 18px rgba(0,0,0,0.55));
      user-select: none;
      transition: transform 0.3s ease;
    }
    .sticker:hover {
      transform: scale(1.12) rotate(4deg) !important;
    }

    @keyframes floatSlow {
      0%, 100% { transform: translateY(0px) rotate(-6deg); }
      50% { transform: translateY(-14px) rotate(-2deg); }
    }
    @keyframes floatAlt {
      0%, 100% { transform: translateY(0px) rotate(7deg); }
      50% { transform: translateY(-16px) rotate(12deg); }
    }
    @keyframes floatBounce {
      0%, 100% { transform: translateY(0px) scale(1); }
      50% { transform: translateY(-10px) scale(1.05); }
    }

    .anim-float-1 { animation: floatSlow 4.2s ease-in-out infinite; }
    .anim-float-2 { animation: floatAlt 4.8s ease-in-out infinite; }
    .anim-float-3 { animation: floatBounce 3.6s ease-in-out infinite; }

    /* 4D Interactive Cards */
    .stage-4d { perspective: 1200px; }
    .btn-4d {
      position: relative;
      transform-style: preserve-3d;
      transition: transform 0.2s cubic-bezier(0.23, 1, 0.32, 1), box-shadow 0.3s ease;
      cursor: pointer;
      overflow: hidden;
    }
    .btn-4d::before {
      content: '';
      position: absolute;
      inset: -2px;
      background: linear-gradient(120deg, transparent, rgba(255,255,255,0.45), transparent);
      transform: translateX(-100%);
      transition: transform 0.65s ease;
    }
    .btn-4d:hover::before { transform: translateX(100%); }
    .btn-4d-rider {
      background: linear-gradient(135deg, #10b981 0%, #059669 100%);
      box-shadow: 0 14px 0 #064e3b, 0 25px 45px rgba(16, 185, 129, 0.45);
    }
    .btn-4d-customer {
      background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
      border: 1.5px solid rgba(96, 165, 250, 0.4);
      box-shadow: 0 14px 0 #090d16, 0 25px 40px rgba(30, 58, 138, 0.25);
    }
    .glass {
      background: rgba(15, 23, 42, 0.88);
      backdrop-filter: blur(16px);
      border: 1px solid rgba(255, 255, 255, 0.12);
    }
    @keyframes pulseRing {
      0% { transform: scale(0.98); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.9); }
      70% { transform: scale(1); box-shadow: 0 0 0 25px rgba(16, 185, 129, 0); }
      100% { transform: scale(0.98); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
    .ring-pulse { animation: pulseRing 1.2s infinite; }
  </style>
</head>
<body class="relative flex flex-col min-h-screen">
  <div class="orb-1"></div>
  <div class="orb-2"></div>

  <!-- Minimal Top Header -->
  <nav class="relative z-20 max-w-6xl w-full mx-auto px-4 py-4 flex justify-between items-center">
    <div onclick="showView('landing')" class="flex items-center gap-2.5 cursor-pointer">
      <div class="w-10 h-10 rounded-2xl bg-gradient-to-tr from-emerald-400 to-blue-500 flex items-center justify-center shadow-lg">
        <span class="text-slate-950 font-black text-lg">⚡</span>
      </div>
      <div>
        <span class="text-2xl font-black tracking-tight leading-none block">Go<sup class="text-emerald-400 text-base">2</sup>Go</span>
        <span class="text-[10px] text-blue-300 font-bold tracking-wide">Co-Powered by CareerBoot</span>
      </div>
    </div>
    <button onclick="showView('admin')" class="text-xs text-slate-400 hover:text-white px-3 py-1.5 rounded-xl border border-slate-800 hover:border-slate-600 transition">
      Admin
    </button>
  </nav>

  <!-- =========================================================
       VIEW 1: STICKER-VIBE LANDING PAGE (ZERO CLUTTER)
  ========================================================== -->
  <main id="view-landing" class="relative z-10 max-w-5xl w-full mx-auto px-4 pt-4 pb-16 text-center flex-1 flex flex-col justify-center">

    <!-- Floating Characterized Stickers Stage -->
    <div class="relative max-w-3xl w-full mx-auto py-4">

      <!-- STICKER 1 (Top Left): Turbo Helmet Rider Mascot -->
      <div class="sticker anim-float-1 hidden sm:flex flex-col items-center absolute -left-8 -top-4 bg-white text-slate-950 px-3.5 py-2 rounded-3xl border-4 border-emerald-400 rotate-[-8deg] z-20">
        <span class="text-3xl leading-none">🛵💨</span>
        <span class="text-[11px] font-black uppercase tracking-tight text-emerald-700 mt-0.5">0% CUT</span>
      </div>

      <!-- STICKER 2 (Top Right): Golden Crown #1 Priority -->
      <div class="sticker anim-float-2 hidden sm:flex flex-col items-center absolute -right-6 -top-2 bg-amber-300 text-slate-950 px-3.5 py-2 rounded-3xl border-4 border-white rotate-[9deg] z-20">
        <span class="text-3xl leading-none">👑🔥</span>
        <span class="text-[11px] font-black uppercase tracking-tight mt-0.5">1st IN = 1st RIDE</span>
      </div>

      <!-- STICKER 3 (Bottom Left): Rupee Rocket -->
      <div class="sticker anim-float-3 hidden md:flex items-center gap-1.5 absolute -left-4 bottom-2 bg-slate-900 text-white px-3.5 py-2 rounded-full border-2 border-emerald-400 rotate-[-5deg] z-20">
        <span class="text-xl">💸</span>
        <span class="text-xs font-black text-emerald-400">₹7/KM FLAT</span>
      </div>

      <!-- STICKER 4 (Bottom Right): Verified Shield -->
      <div class="sticker anim-float-1 hidden md:flex items-center gap-1.5 absolute -right-4 bottom-2 bg-slate-900 text-white px-3.5 py-2 rounded-full border-2 border-blue-400 rotate-[6deg] z-20">
        <span class="text-xl">🛡️</span>
        <span class="text-xs font-black text-blue-300">100% YOURS</span>
      </div>

      <!-- Central Hero Logo & Short Tagline -->
      <div class="inline-flex items-center gap-2 px-4 py-1 rounded-full bg-emerald-500/15 border border-emerald-400/30 text-xs font-black text-emerald-300 mb-3">
        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
        RIDER ONBOARDING OPEN
      </div>

      <h1 class="text-6xl sm:text-8xl font-black tracking-tighter leading-none">
        Go<span class="text-emerald-400">²</span>Go
      </h1>

      <p class="text-xl sm:text-3xl font-black bg-gradient-to-r from-emerald-300 via-white to-blue-300 bg-clip-text text-transparent mt-2">
        Zero Commission. Pocket-Friendly.
      </p>

      <!-- Mobile Character Sticker Strip (Visible on phones) -->
      <div class="flex sm:hidden justify-center gap-2.5 mt-4">
        <span class="sticker anim-float-1 bg-white text-slate-950 px-3 py-1 rounded-full border-2 border-emerald-400 text-xs font-black">🛵 0% CUT</span>
        <span class="sticker anim-float-2 bg-amber-300 text-slate-950 px-3 py-1 rounded-full border-2 border-white text-xs font-black">👑 #1 QUEUE</span>
        <span class="sticker anim-float-3 bg-slate-900 text-emerald-400 px-3 py-1 rounded-full border-2 border-emerald-400 text-xs font-black">💸 ₹7/KM</span>
      </div>
    </div>

    <!-- Compact Visual Stat Pills -->
    <div class="flex flex-wrap justify-center gap-3 max-w-xl mx-auto mt-2 mb-8">
      <div class="glass px-4 py-2 rounded-2xl flex items-center gap-2">
        <span class="text-lg">💰</span>
        <span class="text-xs font-black text-emerald-400">0% FEE</span>
      </div>
      <div class="glass px-4 py-2 rounded-2xl flex items-center gap-2">
        <span class="text-lg">⚡</span>
        <span class="text-xs font-black text-white">₹7/KM</span>
      </div>
      <div class="glass px-4 py-2 rounded-2xl flex items-center gap-2">
        <span class="text-lg">🏆</span>
        <span class="text-xs font-black text-amber-300" id="live-rider-counter">JOIN EARLY</span>
      </div>
    </div>

    <!-- Two 4D Animated Action Cards -->
    <div class="stage-4d grid grid-cols-1 md:grid-cols-2 gap-6 max-w-3xl w-full mx-auto px-2">
      <!-- 1. I'm Rider -->
      <div onclick="showView('rider-auth')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d btn-4d-rider rounded-3xl p-7 text-left select-none relative">
        <span class="absolute top-4 right-4 px-3 py-1 rounded-full bg-slate-950/85 text-emerald-300 text-[10px] font-black uppercase">
          🔥 OPEN NOW
        </span>
        <div class="text-4xl mb-3">🏍️</div>
        <h2 class="text-3xl font-black text-white">1. I'm Rider</h2>
        <p class="text-emerald-100 text-xs font-bold mt-1">Lock #1 Queue Spot • Keep 100% Fare</p>
      </div>

      <!-- 2. I'm Customer -->
      <div onclick="showView('customer-soon')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d btn-4d-customer rounded-3xl p-7 text-left select-none relative">
        <span class="absolute top-4 right-4 px-3 py-1 rounded-full bg-amber-300 text-slate-950 text-[10px] font-black uppercase animate-pulse">
          🚀 LIVE SOON
        </span>
        <div class="text-4xl mb-3">📍</div>
        <h2 class="text-3xl font-black text-white">2. I'm Customer</h2>
        <p class="text-blue-300 text-xs font-bold mt-1">Bookings Opening Soon • Tap for VIP Alert</p>
      </div>
    </div>
  </main>

  <!-- =========================================================
       VIEW 2: RIDER REGISTRATION & LOGIN
  ========================================================== -->
  <section id="view-rider-auth" class="hidden relative z-10 max-w-xl w-full mx-auto px-4 py-6 flex-1">
    <div class="glass rounded-3xl p-6 sm:p-8 shadow-2xl border border-emerald-500/30 relative">
      <!-- Floating Sticker Accent -->
      <div class="sticker anim-float-2 hidden sm:flex items-center gap-1 absolute -top-4 -right-3 bg-amber-300 text-slate-950 px-3 py-1 rounded-full border-2 border-white text-xs font-black rotate-6">
        👑 EARLY = TOP RANK
      </div>

      <div class="flex justify-between items-center mb-5">
        <h2 class="text-2xl font-black text-emerald-400">🏍️ Rider Join</h2>
        <div class="flex bg-slate-900 p-1 rounded-xl border border-slate-800">
          <button id="tab-rider-reg" onclick="switchRiderTab('register')" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950">New</button>
          <button id="tab-rider-log" onclick="switchRiderTab('login')" class="px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400">Login</button>
        </div>
      </div>

      <form onsubmit="handleRiderSubmit(event)" class="space-y-3.5">
        <div id="rider-reg-fields">
          <input type="text" id="r-name" placeholder="Full Name *" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:border-emerald-400 outline-none" />
        </div>

        <input type="tel" id="r-mobile" maxlength="10" placeholder="10-Digit Mobile Number *" required class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:border-emerald-400 outline-none" />

        <div id="rider-vehicle-fields" class="space-y-3.5">
          <div class="grid grid-cols-2 gap-3">
            <select id="r-vtype" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-3 py-3 text-sm text-white outline-none">
              <option value="Bike">🏍️ Bike</option>
              <option value="Scooter">🛵 Scooter</option>
              <option value="Auto">🛺 Auto</option>
              <option value="Cab">🚗 Car / Cab</option>
            </select>
            <input type="text" id="r-vnum" placeholder="Vehicle No (UP32...)*" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white uppercase outline-none" />
          </div>

          <input type="text" id="r-vmodel" placeholder="Vehicle Model & Color (e.g. Honda Shine)*" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white outline-none" />

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
              <label class="block text-xs font-bold text-emerald-400 mb-1">🪪 Aadhaar Photo *</label>
              <input type="file" id="r-aadhaar" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1 file:px-2.5 file:rounded-lg file:border-0 file:bg-emerald-500/20 file:text-emerald-300" />
            </div>
            <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
              <label class="block text-xs font-bold text-emerald-400 mb-1">📸 Vehicle Photo *</label>
              <input type="file" id="r-vimg" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1 file:px-2.5 file:rounded-lg file:border-0 file:bg-emerald-500/20 file:text-emerald-300" />
            </div>
          </div>

          <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
            <label class="block text-xs font-bold text-slate-300 mb-1">🤳 Selfie / Profile Photo (Optional)</label>
            <input type="file" id="r-profile" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1 file:px-2.5 file:rounded-lg file:border-0 file:bg-slate-700 file:text-white" />
          </div>
        </div>

        <button type="submit" id="rider-submit-btn" class="w-full py-4 rounded-2xl bg-gradient-to-r from-emerald-500 to-teal-600 font-black text-slate-950 text-base shadow-lg hover:opacity-95 transition">
          🚀 Lock My Spot & Submit
        </button>
      </form>
    </div>
  </section>

  <!-- =========================================================
       VIEW 3: RIDER DASHBOARD
  ========================================================== -->
  <section id="view-rider-dash" class="hidden relative z-10 max-w-3xl w-full mx-auto px-4 py-6 flex-1">
    <div class="glass rounded-3xl p-6 shadow-2xl space-y-5">
      <div class="flex flex-wrap justify-between items-center gap-4 border-b border-slate-800 pb-4">
        <div class="flex items-center gap-3">
          <img id="rd-avatar" src="https://via.placeholder.com/60/10b981/ffffff?text=R" class="w-14 h-14 rounded-2xl object-cover border-2 border-emerald-400" />
          <div>
            <h2 id="rd-name" class="text-xl font-black text-white">Rider</h2>
            <p id="rd-vehicle" class="text-xs text-slate-400">Vehicle</p>
          </div>
        </div>
        <div class="flex items-center gap-2">
          <span class="px-3 py-1 rounded-full text-xs font-black bg-amber-400 text-slate-950">
            👑 Rank #<span id="rd-seq-top">1</span>
          </span>
          <div id="rd-badge" class="px-3 py-1 rounded-full text-xs font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30">
            ⏳ Pending
          </div>
        </div>
      </div>

      <!-- Daily / Weekly / Monthly Earnings Grid -->
      <div class="grid grid-cols-3 gap-3">
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-3.5 text-center">
          <div class="text-[11px] font-black uppercase text-emerald-400">Daily</div>
          <div class="text-xl sm:text-2xl font-black text-white mt-0.5">₹<span id="stat-daily-earn">0</span></div>
          <div class="text-[11px] text-slate-400 mt-0.5"><span id="stat-daily-rides">0</span> Rides • <span id="stat-daily-km">0</span>km</div>
        </div>
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-3.5 text-center">
          <div class="text-[11px] font-black uppercase text-blue-400">Weekly</div>
          <div class="text-xl sm:text-2xl font-black text-white mt-0.5">₹<span id="stat-weekly-earn">0</span></div>
          <div class="text-[11px] text-slate-400 mt-0.5"><span id="stat-weekly-rides">0</span> Rides • <span id="stat-weekly-km">0</span>km</div>
        </div>
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-3.5 text-center">
          <div class="text-[11px] font-black uppercase text-purple-400">Monthly</div>
          <div class="text-xl sm:text-2xl font-black text-white mt-0.5">₹<span id="stat-monthly-earn">0</span></div>
          <div class="text-[11px] text-slate-400 mt-0.5"><span id="stat-monthly-rides">0</span> Rides • <span id="stat-monthly-km">0</span>km</div>
        </div>
      </div>

      <div id="rd-pending-box" class="bg-amber-500/10 border border-amber-500/30 rounded-2xl p-4 text-center">
        <div class="text-2xl mb-1">🛡️</div>
        <h3 class="text-base font-black text-amber-300">Docs Under Review</h3>
        <p class="text-xs text-slate-300 mt-0.5">Queue rank reserved! Admin approval unlocks ride alerts.</p>
      </div>

      <div id="rd-active-box" class="hidden space-y-4">
        <div class="flex flex-wrap items-center justify-between gap-2 bg-slate-900/90 p-3.5 rounded-2xl border border-slate-800">
          <div class="flex items-center gap-2 text-xs font-bold text-emerald-400">
            <span id="socket-status-dot" class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
            <span id="socket-status-text">Ready for Launch</span>
          </div>
          <div class="flex items-center gap-2">
            <button onclick="unlockAndTestSound()" class="px-3 py-1.5 rounded-xl font-bold text-xs bg-amber-500/20 text-amber-300 border border-amber-500/30">
              🔊 Test Siren
            </button>
            <button id="rd-online-btn" onclick="toggleRiderOnline()" class="px-3.5 py-1.5 rounded-xl font-bold text-xs bg-emerald-500 text-slate-950">
              ● ONLINE
            </button>
          </div>
        </div>

        <!-- Active Ride Card -->
        <div id="rd-accepted-card" class="hidden bg-emerald-950/60 border-2 border-emerald-400 rounded-2xl p-5 space-y-3">
          <div class="flex justify-between items-center">
            <span class="text-xs font-black uppercase text-emerald-400">🚀 Ride Active</span>
            <span id="rd-ride-fare" class="text-2xl font-black text-white">₹0</span>
          </div>
          <div class="bg-slate-950/70 p-3.5 rounded-xl">
            <p class="text-base font-black text-white" id="rd-cust-name">-</p>
            <a id="rd-cust-phone" href="#" class="text-emerald-400 font-bold text-sm underline">📞 -</a>
          </div>
          <div class="text-xs space-y-1 text-slate-300">
            <p><strong class="text-emerald-400">From:</strong> <span id="rd-cust-pickup"></span></p>
            <p><strong class="text-blue-400">To:</strong> <span id="rd-cust-drop"></span></p>
            <p id="rd-gps-drop-dist" class="text-amber-300 font-bold pt-1">📍 GPS Tracking...</p>
          </div>
          <div class="grid grid-cols-2 gap-3">
            <a id="rd-gmaps-nav" target="_blank" href="#" class="text-center py-3 rounded-xl bg-blue-600 font-bold text-xs text-white">
              🗺️ Navigate
            </a>
            <button onclick="triggerRideCompletion()" class="py-3 rounded-xl bg-emerald-500 text-slate-950 font-black text-xs">
              ✓ Complete Ride
            </button>
          </div>
        </div>
      </div>
    </div>
  </section>

  <!-- =========================================================
       VIEW 4: CUSTOMER — LIVE SOON (VISUAL & PUNCHY)
  ========================================================== -->
  <section id="view-customer-soon" class="hidden relative z-10 max-w-md w-full mx-auto px-4 py-8 flex-1 flex flex-col justify-center">
    <div class="glass rounded-3xl p-7 shadow-2xl border border-blue-500/30 text-center space-y-5 relative">
      <!-- Floating Character Sticker -->
      <div class="sticker anim-float-2 inline-block bg-amber-300 text-slate-950 px-4 py-1.5 rounded-full border-2 border-white text-xs font-black rotate-[-4deg]">
        🚀 LAUNCHING SOON
      </div>

      <div class="text-6xl">🛵💨</div>

      <h2 class="text-3xl font-black text-white leading-tight">
        Rides Opening <span class="text-blue-400">Very Soon!</span>
      </h2>

      <div class="flex justify-center gap-2 text-xs font-black">
        <span class="px-3 py-1 rounded-full bg-white/10 text-emerald-300">₹7/KM FLAT</span>
        <span class="px-3 py-1 rounded-full bg-white/10 text-blue-300">ZERO SURGE</span>
      </div>

      <form onsubmit="handleCustomerWaitlist(event)" class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 text-left space-y-3">
        <div class="text-xs font-black text-blue-300 uppercase">🔔 VIP Launch Alert:</div>
        <input type="text" id="cw-name" placeholder="Your Name" required class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white outline-none focus:border-blue-400" />
        <input type="tel" id="cw-mobile" maxlength="10" placeholder="10-Digit Mobile" required class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white outline-none focus:border-blue-400" />
        <button type="submit" id="cw-btn" class="w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-500 font-black text-sm text-white transition">
          Notify Me First
        </button>
      </form>

      <button onclick="showView('rider-auth')" class="w-full py-3.5 rounded-2xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-xs shadow-lg">
        🏍️ Own a Vehicle? Join as Rider Now →
      </button>
    </div>
  </section>

  <!-- LOUD RIDER RING MODAL -->
  <div id="ring-modal" class="hidden fixed inset-0 z-50 bg-slate-950/95 backdrop-blur-md flex items-center justify-center p-4">
    <div class="glass border-2 border-emerald-400 rounded-3xl max-w-md w-full p-6 text-center ring-pulse">
      <div class="text-xs font-black uppercase text-emerald-400">🚨 New Ride!</div>
      <div class="text-4xl font-black text-white my-2" id="ring-fare">₹0</div>
      <div class="text-xs font-bold text-slate-300 mb-4" id="ring-dist">0 km • 0% Commission</div>
      <div class="bg-slate-900/90 rounded-2xl p-4 text-left text-xs space-y-2 mb-4 border border-slate-800">
        <div><span class="text-emerald-400 font-bold">PICKUP:</span> <span id="ring-pickup" class="text-white"></span></div>
        <div class="border-t border-slate-800 pt-2"><span class="text-blue-400 font-bold">DROP:</span> <span id="ring-drop" class="text-white"></span></div>
      </div>
      <div class="text-sm font-black text-amber-400 mb-4">Next rider in <span id="ring-timer">20</span>s</div>
      <button onclick="acceptIncomingRide()" class="w-full py-4 rounded-2xl bg-emerald-500 text-slate-950 font-black text-lg">
        ACCEPT RIDE
      </button>
    </div>
  </div>

  <!-- RIDE COMPLETION POP-UP -->
  <div id="complete-modal" class="hidden fixed inset-0 z-50 bg-slate-950/90 backdrop-blur-md flex items-center justify-center p-4">
    <div class="glass border-2 border-emerald-400 rounded-3xl max-w-md w-full p-6 text-center space-y-4">
      <div class="text-5xl">🎉</div>
      <h3 class="text-2xl font-black text-white">Ride Completed!</h3>
      <div class="bg-slate-900/90 rounded-2xl p-4 text-left text-xs space-y-2 border border-slate-800">
        <div class="flex justify-between"><span class="text-slate-400">Distance:</span><strong class="text-white" id="cmp-dist">0 km</strong></div>
        <div class="flex justify-between"><span class="text-slate-400">Earned (₹7/km):</span><strong class="text-emerald-400 text-base" id="cmp-fare">₹0/-</strong></div>
        <div class="text-slate-400 truncate"><span class="text-emerald-400 font-bold">From:</span> <span id="cmp-pickup" class="text-white"></span></div>
        <div class="text-slate-400 truncate"><span class="text-blue-400 font-bold">To:</span> <span id="cmp-drop" class="text-white"></span></div>
      </div>
      <button onclick="closeCompletionModal()" class="w-full py-3.5 rounded-2xl bg-emerald-500 text-slate-950 font-black text-base">
        Done
      </button>
    </div>
  </div>

  <!-- VIEW 6: ADMIN -->
  <section id="view-admin" class="hidden relative z-10 max-w-5xl w-full mx-auto px-4 py-8 flex-1">
    <div class="glass rounded-3xl p-6">
      <div class="flex flex-wrap justify-between items-center gap-4 mb-6">
        <h2 class="text-xl font-black text-white">🛡️ Admin Verification</h2>
        <div class="flex gap-2">
          <input type="password" id="admin-pass-input" placeholder="Password" class="bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs text-white" />
          <button onclick="loadAdminData()" class="px-4 py-2 rounded-xl bg-emerald-500 text-slate-950 font-bold text-xs">Unlock</button>
        </div>
      </div>
      <div id="admin-riders-list" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
    </div>
  </section>

  <!-- FOOTER -->
  <footer class="relative z-10 max-w-6xl w-full mx-auto px-4 py-4 text-center border-t border-slate-800/80 text-xs text-slate-400">
    Go<sup class="text-emerald-400">2</sup>Go • <strong class="text-white">Co-Powered by CareerBoot</strong>
  </footer>

  <script>
    const socket = io({ reconnection: true, reconnectionAttempts: Infinity, reconnectionDelay: 1000 });
    let currentRider = null;
    let currentAcceptedRide = null;
    let incomingRingRideId = null;
    let ringCountdownInterval = null;
    let ringAudioInterval = null;
    let riderPollInterval = null;
    let gpsWatchId = null;
    let riderAuthMode = 'register';
    let sharedAudioCtx = null;

    fetch('/api/public/stats')
      .then(r => r.json())
      .then(d => {
        if (d.totalRiders > 0) document.getElementById('live-rider-counter').innerText = d.totalRiders + '+ RIDERS';
      }).catch(() => {});

    socket.on('connect', () => {
      const dot = document.getElementById('socket-status-dot');
      const txt = document.getElementById('socket-status-text');
      if (dot) dot.className = 'w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping';
      if (txt) txt.innerText = 'Connected';
      if (currentRider && currentRider._id) socket.emit('register_rider_socket', currentRider._id);
    });

    socket.on('disconnect', () => {
      const dot = document.getElementById('socket-status-dot');
      const txt = document.getElementById('socket-status-text');
      if (dot) dot.className = 'w-2.5 h-2.5 rounded-full bg-red-500';
      if (txt) txt.innerText = 'Reconnecting...';
    });

    function tilt4D(e, el) {
      const rect = el.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      const rotateX = ((y - rect.height / 2) / (rect.height / 2)) * -12;
      const rotateY = ((x - rect.width / 2) / (rect.width / 2)) * 12;
      el.style.transform = \`rotateX(\${rotateX}deg) rotateY(\${rotateY}deg) scale(1.03)\`;
    }
    function reset4D(el) { el.style.transform = 'rotateX(0deg) rotateY(0deg) scale(1)'; }

    function showView(viewName) {
      ['landing', 'rider-auth', 'rider-dash', 'customer-soon', 'admin'].forEach(v => {
        document.getElementById('view-' + v).classList.add('hidden');
      });
      document.getElementById('view-' + viewName).classList.remove('hidden');
    }

    function fileToBase64(file) {
      return new Promise((resolve, reject) => {
        if (!file) return resolve('');
        const reader = new FileReader();
        reader.onload = (e) => {
          const img = new Image();
          img.onload = () => {
            const canvas = document.createElement('canvas');
            const maxW = 800;
            const scale = Math.min(1, maxW / img.width);
            canvas.width = img.width * scale;
            canvas.height = img.height * scale;
            const ctx = canvas.getContext('2d');
            ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
            resolve(canvas.toDataURL('image/jpeg', 0.7));
          };
          img.src = e.target.result;
        };
        reader.onerror = reject;
        reader.readAsDataURL(file);
      });
    }

    function unlockAndTestSound() {
      if ('Notification' in window && Notification.permission !== 'granted') Notification.requestPermission();
      playLoudSiren();
    }

    function playLoudSiren() {
      try {
        if (navigator.vibrate) navigator.vibrate([500, 200, 500, 200, 500]);
        if (!sharedAudioCtx) sharedAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
        if (sharedAudioCtx.state === 'suspended') sharedAudioCtx.resume();
        const now = sharedAudioCtx.currentTime;
        const osc1 = sharedAudioCtx.createOscillator();
        const osc2 = sharedAudioCtx.createOscillator();
        const gain = sharedAudioCtx.createGain();

        osc1.type = 'sawtooth';
        osc2.type = 'square';
        osc1.frequency.setValueAtTime(880, now);
        osc1.frequency.linearRampToValueAtTime(1320, now + 0.45);
        osc2.frequency.setValueAtTime(660, now);
        osc2.frequency.linearRampToValueAtTime(990, now + 0.45);

        gain.gain.setValueAtTime(0.8, now);
        gain.gain.exponentialRampToValueAtTime(0.01, now + 0.9);

        osc1.connect(gain);
        osc2.connect(gain);
        gain.connect(sharedAudioCtx.destination);
        osc1.start(now); osc2.start(now);
        osc1.stop(now + 0.9); osc2.stop(now + 0.9);
      } catch (e) {}
    }

    function getDistanceMeters(lat1, lon1, lat2, lon2) {
      const R = 6371000;
      const toRad = d => (d * Math.PI) / 180;
      const dLat = toRad(lat2 - lat1);
      const dLon = toRad(lon2 - lon1);
      const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
      return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    function switchRiderTab(mode) {
      riderAuthMode = mode;
      const isReg = mode === 'register';
      document.getElementById('rider-reg-fields').classList.toggle('hidden', !isReg);
      document.getElementById('rider-vehicle-fields').classList.toggle('hidden', !isReg);
      document.getElementById('rider-submit-btn').innerText = isReg ? '🚀 Lock My Spot & Submit' : 'Login';
      document.getElementById('tab-rider-reg').className = isReg ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950' : 'px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400';
      document.getElementById('tab-rider-log').className = !isReg ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950' : 'px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400';
    }

    async function handleRiderSubmit(e) {
      e.preventDefault();
      try {
        if (!sharedAudioCtx) sharedAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
        if (sharedAudioCtx.state === 'suspended') sharedAudioCtx.resume();
        if ('Notification' in window && Notification.permission !== 'granted') Notification.requestPermission();
      } catch (err) {}

      const btn = document.getElementById('rider-submit-btn');
      btn.disabled = true; btn.innerText = 'Processing...';
      try {
        const payload = { mode: riderAuthMode, mobile: document.getElementById('r-mobile').value.trim() };
        if (riderAuthMode === 'register') {
          payload.name = document.getElementById('r-name').value.trim();
          payload.vehicleType = document.getElementById('r-vtype').value;
          payload.vehicleNumber = document.getElementById('r-vnum').value.trim();
          payload.vehicleModel = document.getElementById('r-vmodel').value.trim();
          payload.aadhaarImage = await fileToBase64(document.getElementById('r-aadhaar').files[0]);
          payload.vehicleImage = await fileToBase64(document.getElementById('r-vimg').files[0]);
          payload.profileImage = await fileToBase64(document.getElementById('r-profile').files[0]);
        }
        const res = await fetch('/api/rider/auth', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error);
        currentRider = data.rider;
        if (data.stats) applyStatsToUI(data.stats);
        renderRiderDashboard(data.queuePosition);
        showView('rider-dash');
      } catch (err) { alert(err.message); }
      finally { btn.disabled = false; btn.innerText = riderAuthMode === 'register' ? '🚀 Lock My Spot & Submit' : 'Login'; }
    }

    function applyStatsToUI(stats) {
      if (!stats) return;
      document.getElementById('stat-daily-earn').innerText = stats.daily.totalEarnings;
      document.getElementById('stat-daily-rides').innerText = stats.daily.totalRides;
      document.getElementById('stat-daily-km').innerText = stats.daily.totalKm;

      document.getElementById('stat-weekly-earn').innerText = stats.weekly.totalEarnings;
      document.getElementById('stat-weekly-rides').innerText = stats.weekly.totalRides;
      document.getElementById('stat-weekly-km').innerText = stats.weekly.totalKm;

      document.getElementById('stat-monthly-earn').innerText = stats.monthly.totalEarnings;
      document.getElementById('stat-monthly-rides').innerText = stats.monthly.totalRides;
      document.getElementById('stat-monthly-km').innerText = stats.monthly.totalKm;
    }

    async function refreshRiderStats() {
      if (!currentRider || !currentRider._id) return;
      const res = await fetch('/api/rider/stats/' + currentRider._id);
      const data = await res.json();
      if (data.stats) applyStatsToUI(data.stats);
    }

    function renderRiderDashboard(queuePos) {
      document.getElementById('rd-name').innerText = currentRider.name;
      document.getElementById('rd-vehicle').innerText = \`\${currentRider.vehicleType} • \${currentRider.vehicleNumber}\`;
      document.getElementById('rd-seq-top').innerText = queuePos || 1;
      if (currentRider.profileImage) document.getElementById('rd-avatar').src = currentRider.profileImage;
      socket.emit('register_rider_socket', currentRider._id);
      refreshRiderStats();

      const badge = document.getElementById('rd-badge');
      if (currentRider.status === 'approved') {
        badge.innerText = '✓ Verified';
        badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30';
        document.getElementById('rd-pending-box').classList.add('hidden');
        document.getElementById('rd-active-box').classList.remove('hidden');
        startRiderHeartbeat();
      } else if (currentRider.status === 'rejected') {
        badge.innerText = '✕ Rejected';
        badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-red-500/20 text-red-300 border border-red-500/30';
      }
    }

    function startRiderHeartbeat() {
      clearInterval(riderPollInterval);
      riderPollInterval = setInterval(() => {
        if (currentRider && currentRider._id && currentRider.isOnline) socket.emit('register_rider_socket', currentRider._id);
      }, 10000);
    }

    async function toggleRiderOnline() {
      const nextState = !currentRider.isOnline;
      const res = await fetch('/api/rider/toggle-online', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ riderId: currentRider._id, isOnline: nextState }) });
      const data = await res.json();
      currentRider = data.rider;
      if (currentRider.isOnline) socket.emit('register_rider_socket', currentRider._id);
      const btn = document.getElementById('rd-online-btn');
      btn.innerText = currentRider.isOnline ? '● ONLINE' : '○ OFFLINE';
      btn.className = currentRider.isOnline ? 'px-3.5 py-1.5 rounded-xl font-bold text-xs bg-emerald-500 text-slate-950' : 'px-3.5 py-1.5 rounded-xl font-bold text-xs bg-slate-700 text-slate-300';
    }

    async function handleCustomerWaitlist(e) {
      e.preventDefault();
      const btn = document.getElementById('cw-btn');
      btn.disabled = true;
      try {
        const res = await fetch('/api/customer/waitlist', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: document.getElementById('cw-name').value.trim(), mobile: document.getElementById('cw-mobile').value.trim() })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error);
        alert(data.message);
        document.getElementById('cw-name').value = '';
        document.getElementById('cw-mobile').value = '';
      } catch (err) { alert(err.message); }
      finally { btn.disabled = false; }
    }

    socket.on('verification_status_changed', ({ status }) => {
      if (currentRider) { currentRider.status = status; renderRiderDashboard(document.getElementById('rd-seq-top').innerText); }
    });

    socket.on('incoming_ride_ring', (ride) => {
      if (incomingRingRideId === ride.rideId) return;
      incomingRingRideId = ride.rideId;
      document.getElementById('ring-fare').innerText = '₹' + ride.fare;
      document.getElementById('ring-dist').innerText = \`\${ride.distanceKm} km • 0% Commission\`;
      document.getElementById('ring-pickup').innerText = ride.pickupAddress;
      document.getElementById('ring-drop').innerText = ride.dropAddress;
      document.getElementById('ring-modal').classList.remove('hidden');

      let sec = ride.timeoutSec || 20;
      document.getElementById('ring-timer').innerText = sec;

      playLoudSiren();
      clearInterval(ringAudioInterval);
      ringAudioInterval = setInterval(playLoudSiren, 1800);

      clearInterval(ringCountdownInterval);
      ringCountdownInterval = setInterval(() => {
        sec--;
        document.getElementById('ring-timer').innerText = Math.max(0, sec);
        if (sec <= 0) stopRiderRinging();
      }, 1000);
    });

    socket.on('ring_timeout', ({ rideId }) => { if (incomingRingRideId === rideId) stopRiderRinging(); });

    function stopRiderRinging() {
      clearInterval(ringCountdownInterval);
      clearInterval(ringAudioInterval);
      document.getElementById('ring-modal').classList.add('hidden');
      incomingRingRideId = null;
    }

    function acceptIncomingRide() {
      if (!incomingRingRideId || !currentRider) return;
      const acceptedId = incomingRingRideId;
      stopRiderRinging();
      socket.emit('accept_ride', { rideId: acceptedId, riderId: currentRider._id });
    }

    function startGPSCompletionMonitor(rideDetails) {
      if (gpsWatchId) navigator.geolocation.clearWatch(gpsWatchId);
      if (!navigator.geolocation || !rideDetails.dropCoords) return;

      gpsWatchId = navigator.geolocation.watchPosition((pos) => {
        const distMeters = getDistanceMeters(pos.coords.latitude, pos.coords.longitude, rideDetails.dropCoords.lat, rideDetails.dropCoords.lng);
        const distEl = document.getElementById('rd-gps-drop-dist');
        if (distEl) distEl.innerText = \`📍 Drop Distance: \${Math.round(distMeters)}m\`;
        if (distMeters <= 150 && currentAcceptedRide) {
          navigator.geolocation.clearWatch(gpsWatchId);
          gpsWatchId = null;
          triggerRideCompletion();
        }
      }, null, { enableHighAccuracy: true, maximumAge: 5000 });
    }

    function triggerRideCompletion() {
      if (!currentAcceptedRide) return;
      if (gpsWatchId) { navigator.geolocation.clearWatch(gpsWatchId); gpsWatchId = null; }
      socket.emit('complete_ride', { rideId: currentAcceptedRide.rideId, riderId: currentRider ? currentRider._id : null });
    }

    socket.on('ride_confirmed_for_rider', (details) => {
      currentAcceptedRide = details;
      const card = document.getElementById('rd-accepted-card');
      card.classList.remove('hidden');
      document.getElementById('rd-ride-fare').innerText = '₹' + details.fare;
      document.getElementById('rd-cust-name').innerText = details.customerName;
      const phoneEl = document.getElementById('rd-cust-phone');
      phoneEl.innerText = '📞 ' + details.customerMobile;
      phoneEl.href = 'tel:' + details.customerMobile;
      document.getElementById('rd-cust-pickup').innerText = details.pickupAddress;
      document.getElementById('rd-cust-drop').innerText = details.dropAddress;
      document.getElementById('rd-gmaps-nav').href = \`https://www.google.com/maps/dir/?api=1&origin=\${details.pickupCoords.lat},\${details.pickupCoords.lng}&destination=\${details.dropCoords.lat},\${details.dropCoords.lng}\`;
      startGPSCompletionMonitor(details);
    });

    socket.on('ride_completed_popup', (info) => {
      document.getElementById('cmp-dist').innerText = info.distanceKm + ' km';
      document.getElementById('cmp-fare').innerText = '₹' + info.fare + '/-';
      document.getElementById('cmp-pickup').innerText = info.pickupAddress || '-';
      document.getElementById('cmp-drop').innerText = info.dropAddress || '-';
      document.getElementById('complete-modal').classList.remove('hidden');
      if (info.stats) applyStatsToUI(info.stats);
      document.getElementById('rd-accepted-card').classList.add('hidden');
      currentAcceptedRide = null;
    });

    function closeCompletionModal() { document.getElementById('complete-modal').classList.add('hidden'); }
    socket.on('accept_failed', ({ message }) => alert(message));

    // ADMIN
    async function loadAdminData() {
      const password = document.getElementById('admin-pass-input').value;
      socket.emit('join_admin_room', password);
      const res = await fetch('/api/admin/riders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password }) });
      const data = await res.json();
      if (!res.ok) return alert(data.error);

      const container = document.getElementById('admin-riders-list');
      if (data.riders.length === 0) { container.innerHTML = '<p class="text-slate-400 text-sm">No riders yet.</p>'; return; }

      container.innerHTML = data.riders.map((r, idx) => \`
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 space-y-3">
          <div class="flex justify-between items-start">
            <div>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-slate-800 text-slate-300">#\${idx + 1}</span>
              <h3 class="text-lg font-black text-white mt-1">\${r.name}</h3>
              <p class="text-xs text-slate-400">📞 \${r.mobile} • \${r.vehicleType} (\${r.vehicleNumber})</p>
            </div>
            <span class="text-xs font-bold px-2.5 py-1 rounded-full \${r.status === 'approved' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-amber-500/20 text-amber-300'}">
              \${r.status.toUpperCase()}
            </span>
          </div>
          <div class="grid grid-cols-2 gap-2">
            <div><p class="text-[10px] text-slate-400 mb-1">Aadhaar</p><img src="\${r.aadhaarImage}" class="w-full h-24 object-cover rounded-lg border border-slate-700" /></div>
            <div><p class="text-[10px] text-slate-400 mb-1">Vehicle</p><img src="\${r.vehicleImage}" class="w-full h-24 object-cover rounded-lg border border-slate-700" /></div>
          </div>
          <div class="flex gap-2 pt-2">
            <button onclick="verifyRider('\${r._id}', 'approved')" class="flex-1 py-2 rounded-xl bg-emerald-500 text-slate-950 font-bold text-xs">Approve</button>
            <button onclick="verifyRider('\${r._id}', 'rejected')" class="flex-1 py-2 rounded-xl bg-red-500/20 text-red-400 border border-red-500/30 font-bold text-xs">Reject</button>
          </div>
        </div>
      \`).join('');
    }

    async function verifyRider(riderId, status) {
      const password = document.getElementById('admin-pass-input').value;
      await fetch('/api/admin/verify-rider', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password, riderId, status }) });
      loadAdminData();
    }

    socket.on('new_rider_verification', (rider) => {
      alert(\`New Rider: \${rider.name} (\${rider.vehicleNumber})\`);
      if (document.getElementById('admin-pass-input').value) loadAdminData();
    });
  </script>
</body>
</html>`);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Go²Go Server running on port ${PORT}`);
});

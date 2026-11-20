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
      if (!existing) return res.status(404).json({ error: 'Number not found. Join squad first!' });
      const queuePosition = await Rider.countDocuments({ joinedAt: { $lte: existing.joinedAt } });
      const stats = await computeRiderStats(existing._id);
      return res.json({ rider: existing, queuePosition, stats });
    }

    if (existing) {
      return res.status(400).json({ error: 'Already in squad! Tap Login.' });
    }

    if (!name || !vehicleType || !vehicleNumber || !vehicleModel || !aadhaarImage || !vehicleImage) {
      return res.status(400).json({ error: 'Fill all details & upload Aadhaar + Vehicle photo.' });
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
    if (existing) {
      existing.name = name;
      await existing.save();
      io.to('admin_room').emit('new_customer_waitlist', { name: existing.name, mobile: existing.mobile });
      return res.json({ message: 'VIP Pass Active! See you at launch.' });
    }
    const created = await Customer.create({ name, mobile });
    io.to('admin_room').emit('new_customer_waitlist', { name: created.name, mobile: created.mobile });
    res.json({ message: 'VIP Pass Unlocked! Launch alert incoming.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/riders', async (req, res) => {
  const { password } = req.body;
  if (password !== ADMIN_PASS) return res.status(401).json({ error: 'Wrong Password' });
  const riders = await Rider.find().sort({ joinedAt: 1 });
  const customers = await Customer.find().sort({ createdAt: 1 });
  res.json({ riders, customers });
});

app.post('/api/admin/verify-rider', async (req, res) => {
  const { password, riderId, status } = req.body;
  if (password !== ADMIN_PASS) return res.status(401).json({ error: 'Unauthorized' });
  const rider = await Rider.findByIdAndUpdate(riderId, { status }, { new: true });
  io.to(`rider_${riderId}`).emit('verification_status_changed', { status });
  res.json({ rider });
});

// ==========================================
// FRONTEND SINGLE-PAGE APPLICATION
// ==========================================
app.get('*', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Go²Go • 0% Commission Rides • ₹7/km</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="/socket.io/socket.io.js"></script>
  <script src="https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@500;700;900&display=swap" rel="stylesheet">
  <style>
    body {
      font-family: 'Outfit', sans-serif;
      background:
        radial-gradient(circle at 15% 15%, rgba(16,185,129,0.16), transparent 40%),
        radial-gradient(circle at 85% 20%, rgba(59,130,246,0.16), transparent 40%),
        linear-gradient(180deg, #091224 0%, #020617 100%);
      color: #f8fafc;
      min-height: 100vh;
      overflow-x: hidden;
    }

    .road-grid {
      position: fixed;
      inset: 0;
      background-image:
        linear-gradient(rgba(148,163,184,0.04) 1px, transparent 1px),
        linear-gradient(90deg, rgba(148,163,184,0.04) 1px, transparent 1px);
      background-size: 48px 48px;
      pointer-events: none;
      z-index: 0;
    }

    .marquee-wrap {
      overflow: hidden;
      white-space: nowrap;
      background: linear-gradient(90deg, #10b981, #38bdf8, #fbbf24, #10b981);
      color: #020617;
      font-weight: 900;
      font-size: 11px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      padding: 6px 0;
    }
    .marquee-track {
      display: inline-block;
      animation: marqueeScroll 18s linear infinite;
    }
    @keyframes marqueeScroll {
      0% { transform: translateX(0%); }
      100% { transform: translateX(-50%); }
    }

    .sticker {
      filter: drop-shadow(0 14px 20px rgba(0,0,0,0.65));
      user-select: none;
      transition: transform 0.25s ease;
    }
    .sticker:hover {
      transform: scale(1.12) rotate(3deg) !important;
    }

    @keyframes floatA {
      0%, 100% { transform: translateY(0px) rotate(-6deg); }
      50% { transform: translateY(-13px) rotate(-2deg); }
    }
    @keyframes floatB {
      0%, 100% { transform: translateY(0px) rotate(7deg); }
      50% { transform: translateY(-15px) rotate(11deg); }
    }
    @keyframes floatC {
      0%, 100% { transform: translateY(0px) scale(1); }
      50% { transform: translateY(-10px) scale(1.05); }
    }

    .anim-a { animation: floatA 4s ease-in-out infinite; }
    .anim-b { animation: floatB 4.6s ease-in-out infinite; }
    .anim-c { animation: floatC 3.5s ease-in-out infinite; }

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
    .card-rider {
      background: linear-gradient(135deg, #10b981 0%, #059669 100%);
      box-shadow: 0 14px 0 #064e3b, 0 25px 45px rgba(16, 185, 129, 0.42);
    }
    .card-customer {
      background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
      border: 2px solid rgba(56, 189, 248, 0.45);
      box-shadow: 0 14px 0 #090d16, 0 25px 40px rgba(14, 165, 233, 0.2);
    }
    .glass {
      background: rgba(15, 23, 42, 0.9);
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
  <div class="road-grid"></div>

  <div class="marquee-wrap relative z-30">
    <div class="marquee-track">
      &nbsp;&nbsp;ZERO COMMISSION FOREVER • 100% RIDER EARNINGS • ₹7/KM FLAT • FIRST JOINED = #1 PRIORITY QUEUE • CO-POWERED BY CAREERBOOT • CUSTOMER RIDES DROPPING SOON &nbsp;&nbsp;&nbsp;&nbsp;
      ZERO COMMISSION FOREVER • 100% RIDER EARNINGS • ₹7/KM FLAT • FIRST JOINED = #1 PRIORITY QUEUE • CO-POWERED BY CAREERBOOT • CUSTOMER RIDES DROPPING SOON &nbsp;&nbsp;&nbsp;&nbsp;
    </div>
  </div>

  <nav class="relative z-20 max-w-6xl w-full mx-auto px-4 py-3.5 flex justify-between items-center">
    <div onclick="showView('landing')" class="flex items-center gap-2.5 cursor-pointer">
      <svg class="w-10 h-10 shrink-0" viewBox="0 0 48 48" fill="none">
        <rect width="48" height="48" rx="14" fill="url(#brandGrad)"/>
        <path d="M26.5 10L15 26H24L21.5 38L33 22H24L26.5 10Z" fill="#020617"/>
        <defs>
          <linearGradient id="brandGrad" x1="0" y1="0" x2="48" y2="48" gradientUnits="userSpaceOnUse">
            <stop stop-color="#34D399"/>
            <stop offset="1" stop-color="#38BDF8"/>
          </linearGradient>
        </defs>
      </svg>
      <div>
        <span class="text-2xl font-black tracking-tight leading-none block">Go<sup class="text-emerald-400 text-base">2</sup>Go</span>
        <span class="text-[10px] text-amber-300 font-extrabold tracking-wide">Co-Powered by CareerBoot</span>
      </div>
    </div>
    <button onclick="showView('admin')" class="text-xs text-slate-400 hover:text-white px-3 py-1.5 rounded-xl border border-slate-800 hover:border-slate-600 transition">
      Admin
    </button>
  </nav>

  <!-- VIEW 1: CONCEPT STICKER LANDING PAGE -->
  <main id="view-landing" class="relative z-10 max-w-5xl w-full mx-auto px-4 pt-3 pb-14 text-center flex-1 flex flex-col justify-center">
    <div class="relative max-w-3xl w-full mx-auto py-6">
      <div class="sticker anim-a hidden sm:flex items-center gap-2 absolute -left-8 -top-3 bg-white text-slate-950 px-3.5 py-2 rounded-3xl border-4 border-emerald-400 rotate-[-7deg] z-20">
        <svg class="w-9 h-9" viewBox="0 0 48 48" fill="none">
          <circle cx="14" cy="34" r="7" stroke="#0F172A" stroke-width="3.5" fill="#E2E8F0"/>
          <circle cx="35" cy="34" r="7" stroke="#0F172A" stroke-width="3.5" fill="#E2E8F0"/>
          <path d="M14 34H24L30 22H37" stroke="#059669" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M22 22H17L14 15H10" stroke="#0F172A" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"/>
          <circle cx="32" cy="11" r="5" fill="#10B981"/>
        </svg>
        <div class="text-left leading-tight">
          <div class="text-[11px] font-black text-emerald-700">0% CUT</div>
          <div class="text-[9px] font-extrabold text-slate-600">FULL FARE</div>
        </div>
      </div>

      <div class="sticker anim-b hidden sm:flex items-center gap-2 absolute -right-7 -top-2 bg-amber-300 text-slate-950 px-3.5 py-2 rounded-3xl border-4 border-white rotate-[8deg] z-20">
        <svg class="w-9 h-9" viewBox="0 0 48 48" fill="none">
          <path d="M9 35L7 16L18 24L24 12L30 24L41 16L39 35H9Z" fill="#F59E0B" stroke="#0F172A" stroke-width="3" stroke-linejoin="round"/>
          <rect x="9" y="36" width="30" height="5" rx="2" fill="#0F172A"/>
        </svg>
        <div class="text-left leading-tight">
          <div class="text-[11px] font-black">#1 QUEUE</div>
          <div class="text-[9px] font-extrabold text-slate-800">EARLY JOIN</div>
        </div>
      </div>

      <div class="sticker anim-c hidden md:flex items-center gap-2 absolute -left-5 bottom-1 bg-slate-900 text-white px-3.5 py-2 rounded-full border-2 border-emerald-400 rotate-[-5deg] z-20">
        <svg class="w-6 h-6" viewBox="0 0 32 32" fill="none">
          <circle cx="16" cy="16" r="13" fill="#10B981" fill-opacity="0.2" stroke="#34D399" stroke-width="2.5"/>
          <path d="M11 10H21M11 15H21M17 10C19.5 10 21 11.5 21 13.5C21 15.5 19.5 17 17 17H11L20 24" stroke="#34D399" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <span class="text-xs font-black text-emerald-300">₹7 / KM</span>
      </div>

      <div class="sticker anim-a hidden md:flex items-center gap-2 absolute -right-5 bottom-1 bg-slate-900 text-white px-3.5 py-2 rounded-full border-2 border-sky-400 rotate-[6deg] z-20">
        <svg class="w-6 h-6" viewBox="0 0 32 32" fill="none">
          <path d="M16 4C11.03 4 7 8.03 7 13C7 19.75 16 28 16 28C16 28 25 19.75 25 13C25 8.03 20.97 4 16 4Z" fill="#38BDF8" fill-opacity="0.25" stroke="#38BDF8" stroke-width="2.5"/>
          <circle cx="16" cy="13" r="4" fill="#38BDF8"/>
        </svg>
        <span class="text-xs font-black text-sky-300" id="live-rider-counter">RIDER DRIVE</span>
      </div>

      <div class="inline-flex items-center gap-2 px-4 py-1 rounded-full bg-emerald-500/15 border border-emerald-400/30 text-xs font-black text-emerald-300 mb-2">
        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
        RIDER ONBOARDING • ZERO COMMISSION
      </div>

      <h1 class="text-6xl sm:text-8xl font-black tracking-tighter leading-none">
        Go<span class="text-emerald-400">²</span>Go
      </h1>

      <p class="text-xl sm:text-3xl font-black bg-gradient-to-r from-emerald-300 via-white to-sky-300 bg-clip-text text-transparent mt-2">
        Zero Commission. Pocket-Friendly.
      </p>

      <div class="flex sm:hidden justify-center gap-2 mt-4">
        <span class="sticker anim-a bg-white text-slate-950 px-3 py-1 rounded-full border-2 border-emerald-400 text-xs font-black">0% CUT</span>
        <span class="sticker anim-b bg-amber-300 text-slate-950 px-3 py-1 rounded-full border-2 border-white text-xs font-black">#1 QUEUE</span>
        <span class="sticker anim-c bg-slate-900 text-emerald-300 px-3 py-1 rounded-full border-2 border-emerald-400 text-xs font-black">₹7/KM</span>
      </div>
    </div>

    <div class="stage-4d grid grid-cols-1 md:grid-cols-2 gap-6 max-w-3xl w-full mx-auto mt-6 px-2">
      <div onclick="showView('rider-auth')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d card-rider rounded-3xl p-7 text-left select-none relative">
        <span class="absolute top-4 right-4 px-3 py-1 rounded-full bg-slate-950 text-emerald-300 text-[10px] font-black uppercase tracking-wider">
          JOIN NOW
        </span>
        <svg class="w-12 h-12 mb-3" viewBox="0 0 56 56" fill="none">
          <rect width="56" height="56" rx="16" fill="rgba(255,255,255,0.2)"/>
          <path d="M14 31C14 22.7157 20.7157 16 29 16C37.2843 16 44 22.7157 44 31V35H14V31Z" fill="#FFFFFF"/>
          <path d="M31 25H44V30H31V25Z" fill="#064E3B"/>
          <path d="M8 38H44" stroke="#FFFFFF" stroke-width="3.5" stroke-linecap="round"/>
          <path d="M11 43H36" stroke="#A7F3D0" stroke-width="3" stroke-linecap="round"/>
        </svg>
        <h2 class="text-3xl font-black text-white tracking-tight">I'm Rider</h2>
        <p class="text-slate-950 font-black text-xs mt-1.5 inline-block bg-emerald-200/95 px-2.5 py-0.5 rounded-lg">
          100% EARNINGS • ₹7/KM • PRIORITY QUEUE
        </p>
      </div>

      <div onclick="showView('customer-soon')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d card-customer rounded-3xl p-7 text-left select-none relative">
        <span class="absolute top-4 right-4 px-3 py-1 rounded-full bg-amber-300 text-slate-950 text-[10px] font-black uppercase animate-pulse">
          LIVE SOON
        </span>
        <svg class="w-12 h-12 mb-3" viewBox="0 0 56 56" fill="none">
          <rect width="56" height="56" rx="16" fill="rgba(56,189,248,0.16)"/>
          <path d="M28 11C20.8203 11 15 16.8203 15 24C15 33.75 28 45 28 45C28 45 41 33.75 41 24C41 16.8203 35.1797 11 28 11Z" fill="#38BDF8" fill-opacity="0.3" stroke="#38BDF8" stroke-width="3"/>
          <circle cx="28" cy="24" r="5.5" fill="#FBBF24"/>
        </svg>
        <h2 class="text-3xl font-black text-white tracking-tight">I'm Customer</h2>
        <p class="text-sky-300 font-extrabold text-xs mt-1.5">
          LAUNCHING SOON • ₹7/KM FLAT • VIP ALERT
        </p>
      </div>
    </div>
  </main>

  <!-- VIEW 2: RIDER REGISTRATION & LOGIN -->
  <section id="view-rider-auth" class="hidden relative z-10 max-w-xl w-full mx-auto px-4 py-6 flex-1">
    <div class="glass rounded-3xl p-6 sm:p-8 shadow-2xl border border-emerald-500/30 relative">
      <div class="sticker anim-b hidden sm:flex items-center gap-1.5 absolute -top-4 -right-3 bg-amber-300 text-slate-950 px-3 py-1 rounded-full border-2 border-white text-xs font-black rotate-6">
        <span>👑</span> EARLY = TOP RANK
      </div>

      <div class="flex justify-between items-center mb-5">
        <h2 class="text-2xl font-black text-emerald-400">Rider Access</h2>
        <div class="flex bg-slate-900 p-1 rounded-xl border border-slate-800">
          <button id="tab-rider-reg" onclick="switchRiderTab('register')" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950">New Rider</button>
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
              <option value="Bike">Bike</option>
              <option value="Scooter">Scooter</option>
              <option value="Auto">Auto</option>
              <option value="Cab">Car / Cab</option>
            </select>
            <input type="text" id="r-vnum" placeholder="Vehicle No (UP32...)*" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white uppercase outline-none" />
          </div>

          <input type="text" id="r-vmodel" placeholder="Vehicle Model & Color *" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white outline-none" />

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
              <label class="block text-xs font-bold text-emerald-400 mb-1">Aadhaar Photo *</label>
              <input type="file" id="r-aadhaar" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1 file:px-2.5 file:rounded-lg file:border-0 file:bg-emerald-500/20 file:text-emerald-300" />
            </div>
            <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
              <label class="block text-xs font-bold text-emerald-400 mb-1">Vehicle Photo *</label>
              <input type="file" id="r-vimg" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1 file:px-2.5 file:rounded-lg file:border-0 file:bg-emerald-500/20 file:text-emerald-300" />
            </div>
          </div>

          <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
            <label class="block text-xs font-bold text-slate-300 mb-1">Profile Photo (Optional)</label>
            <input type="file" id="r-profile" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1 file:px-2.5 file:rounded-lg file:border-0 file:bg-slate-700 file:text-white" />
          </div>
        </div>

        <button type="submit" id="rider-submit-btn" class="w-full py-4 rounded-2xl bg-gradient-to-r from-emerald-400 to-teal-500 font-black text-slate-950 text-base shadow-lg hover:opacity-95 transition">
          Lock Queue Spot & Submit
        </button>
      </form>
    </div>
  </section>

  <!-- VIEW 3: RIDER DASHBOARD -->
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
          <span class="px-3 py-1 rounded-full text-xs font-black bg-amber-300 text-slate-950">
            Queue #<span id="rd-seq-top">1</span>
          </span>
          <div id="rd-badge" class="px-3 py-1 rounded-full text-xs font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30">
            Pending
          </div>
        </div>
      </div>

      <div class="grid grid-cols-3 gap-3">
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-3.5 text-center">
          <div class="text-[11px] font-black uppercase text-emerald-400">Daily</div>
          <div class="text-xl sm:text-2xl font-black text-white mt-0.5">₹<span id="stat-daily-earn">0</span></div>
          <div class="text-[11px] text-slate-400 mt-0.5"><span id="stat-daily-rides">0</span> Rides • <span id="stat-daily-km">0</span>km</div>
        </div>
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-3.5 text-center">
          <div class="text-[11px] font-black uppercase text-sky-400">Weekly</div>
          <div class="text-xl sm:text-2xl font-black text-white mt-0.5">₹<span id="stat-weekly-earn">0</span></div>
          <div class="text-[11px] text-slate-400 mt-0.5"><span id="stat-weekly-rides">0</span> Rides • <span id="stat-weekly-km">0</span>km</div>
        </div>
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-3.5 text-center">
          <div class="text-[11px] font-black uppercase text-amber-300">Monthly</div>
          <div class="text-xl sm:text-2xl font-black text-white mt-0.5">₹<span id="stat-monthly-earn">0</span></div>
          <div class="text-[11px] text-slate-400 mt-0.5"><span id="stat-monthly-rides">0</span> Rides • <span id="stat-monthly-km">0</span>km</div>
        </div>
      </div>

      <div id="rd-pending-box" class="bg-amber-500/10 border border-amber-500/30 rounded-2xl p-4 text-center">
        <h3 class="text-base font-black text-amber-300">Queue Position Reserved</h3>
        <p class="text-xs text-slate-300 mt-0.5">Admin verification activates live ride alerts for launch.</p>
      </div>

      <div id="rd-active-box" class="hidden space-y-4">
        <div class="flex flex-wrap items-center justify-between gap-2 bg-slate-900/90 p-3.5 rounded-2xl border border-slate-800">
          <div class="flex items-center gap-2 text-xs font-bold text-emerald-400">
            <span id="socket-status-dot" class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
            <span id="socket-status-text">Dispatch Ready</span>
          </div>
          <div class="flex items-center gap-2">
            <button onclick="unlockAndTestSound()" class="px-3 py-1.5 rounded-xl font-bold text-xs bg-amber-500/20 text-amber-300 border border-amber-500/30">
              Test Ringtone
            </button>
            <button id="rd-online-btn" onclick="toggleRiderOnline()" class="px-3.5 py-1.5 rounded-xl font-bold text-xs bg-emerald-500 text-slate-950">
              ● ONLINE
            </button>
          </div>
        </div>

        <div id="rd-accepted-card" class="hidden bg-emerald-950/60 border-2 border-emerald-400 rounded-2xl p-5 space-y-3">
          <div class="flex justify-between items-center">
            <span class="text-xs font-black uppercase text-emerald-400">Active Ride</span>
            <span id="rd-ride-fare" class="text-2xl font-black text-white">₹0</span>
          </div>
          <div class="bg-slate-950/70 p-3.5 rounded-xl">
            <p class="text-base font-black text-white" id="rd-cust-name">-</p>
            <a id="rd-cust-phone" href="#" class="text-emerald-400 font-bold text-sm underline">-</a>
          </div>
          <div class="text-xs space-y-1 text-slate-300">
            <p><strong class="text-emerald-400">From:</strong> <span id="rd-cust-pickup"></span></p>
            <p><strong class="text-sky-400">To:</strong> <span id="rd-cust-drop"></span></p>
            <p id="rd-gps-drop-dist" class="text-amber-300 font-bold pt-1">GPS Drop Monitor Active</p>
          </div>
          <div class="grid grid-cols-2 gap-3">
            <a id="rd-gmaps-nav" target="_blank" href="#" class="text-center py-3 rounded-xl bg-sky-600 font-bold text-xs text-white">
              Open Map Route
            </a>
            <button onclick="triggerRideCompletion()" class="py-3 rounded-xl bg-emerald-500 text-slate-950 font-black text-xs">
              Complete Ride
            </button>
          </div>
        </div>
      </div>
    </div>
  </section>

  <!-- VIEW 4: CUSTOMER — LIVE SOON VIP ALERT -->
  <section id="view-customer-soon" class="hidden relative z-10 max-w-md w-full mx-auto px-4 py-8 flex-1 flex flex-col justify-center">
    <div class="glass rounded-3xl p-7 shadow-2xl border border-sky-400/40 text-center space-y-5 relative">
      <div class="sticker anim-b inline-block bg-amber-300 text-slate-950 px-4 py-1.5 rounded-full border-2 border-white text-xs font-black rotate-[-4deg]">
        LIVE SOON • ₹7/KM
      </div>

      <svg class="w-20 h-20 mx-auto" viewBox="0 0 80 80" fill="none">
        <circle cx="40" cy="40" r="34" stroke="#38BDF8" stroke-opacity="0.25" stroke-width="3" stroke-dasharray="6 6"/>
        <circle cx="40" cy="40" r="21" stroke="#34D399" stroke-opacity="0.45" stroke-width="3"/>
        <path d="M40 16C31.7157 16 25 22.7157 25 31C25 42.25 40 55 40 55C40 55 55 42.25 55 31C55 22.7157 48.2843 16 40 16Z" fill="#38BDF8" fill-opacity="0.25" stroke="#38BDF8" stroke-width="3"/>
        <circle cx="40" cy="31" r="6" fill="#FBBF24"/>
      </svg>

      <h2 class="text-3xl font-black text-white leading-tight">
        Customer Rides <span class="text-sky-400">Live Soon</span>
      </h2>

      <form onsubmit="handleCustomerWaitlist(event)" class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 text-left space-y-3">
        <div class="text-xs font-black text-sky-300 uppercase">Get Launch Notification:</div>
        <input type="text" id="cw-name" placeholder="Your Name" required class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white outline-none focus:border-sky-400" />
        <input type="tel" id="cw-mobile" maxlength="10" placeholder="10-Digit Mobile" required class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white outline-none focus:border-sky-400" />
        <button type="submit" id="cw-btn" class="w-full py-3 rounded-xl bg-sky-500 hover:bg-sky-400 text-slate-950 font-black text-sm transition">
          Notify Me When Live
        </button>
      </form>

      <button onclick="showView('rider-auth')" class="w-full py-3.5 rounded-2xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-xs shadow-lg">
        Join as Rider Now (0% Commission)
      </button>
    </div>
  </section>

  <!-- LOUD RIDER RING MODAL -->
  <div id="ring-modal" class="hidden fixed inset-0 z-50 bg-slate-950/95 backdrop-blur-md flex items-center justify-center p-4">
    <div class="glass border-2 border-emerald-400 rounded-3xl max-w-md w-full p-6 text-center ring-pulse">
      <div class="text-xs font-black uppercase text-emerald-400">Incoming Ride Request</div>
      <div class="text-4xl font-black text-white my-2" id="ring-fare">₹0</div>
      <div class="text-xs font-bold text-slate-300 mb-4" id="ring-dist">0 km • 0% Commission</div>
      <div class="bg-slate-900/90 rounded-2xl p-4 text-left text-xs space-y-2 mb-4 border border-slate-800">
        <div><span class="text-emerald-400 font-bold">PICKUP:</span> <span id="ring-pickup" class="text-white"></span></div>
        <div class="border-t border-slate-800 pt-2"><span class="text-sky-400 font-bold">DROP:</span> <span id="ring-drop" class="text-white"></span></div>
      </div>
      <div class="text-sm font-black text-amber-400 mb-4">Passing to next rider in <span id="ring-timer">20</span>s</div>
      <button onclick="acceptIncomingRide()" class="w-full py-4 rounded-2xl bg-emerald-500 text-slate-950 font-black text-lg">
        ACCEPT RIDE
      </button>
    </div>
  </div>

  <!-- RIDE COMPLETION POP-UP -->
  <div id="complete-modal" class="hidden fixed inset-0 z-50 bg-slate-950/90 backdrop-blur-md flex items-center justify-center p-4">
    <div class="glass border-2 border-emerald-400 rounded-3xl max-w-md w-full p-6 text-center space-y-4">
      <svg class="w-16 h-16 mx-auto" viewBox="0 0 64 64" fill="none">
        <circle cx="32" cy="32" r="28" fill="#10B981" fill-opacity="0.2" stroke="#34D399" stroke-width="3.5"/>
        <path d="M20 33L28 41L44 24" stroke="#34D399" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
      <h3 class="text-2xl font-black text-white">Ride Completed</h3>
      <div class="bg-slate-900/90 rounded-2xl p-4 text-left text-xs space-y-2 border border-slate-800">
        <div class="flex justify-between"><span class="text-slate-400">Distance:</span><strong class="text-white" id="cmp-dist">0 km</strong></div>
        <div class="flex justify-between"><span class="text-slate-400">Total Fare (₹7/km):</span><strong class="text-emerald-400 text-base" id="cmp-fare">₹0/-</strong></div>
        <div class="text-slate-400 truncate"><span class="text-emerald-400 font-bold">From:</span> <span id="cmp-pickup" class="text-white"></span></div>
        <div class="text-slate-400 truncate"><span class="text-sky-400 font-bold">To:</span> <span id="cmp-drop" class="text-white"></span></div>
      </div>
      <button onclick="closeCompletionModal()" class="w-full py-3.5 rounded-2xl bg-emerald-500 text-slate-950 font-black text-base">
        Done
      </button>
    </div>
  </div>

  <!-- VIEW 6: ADMIN (CUSTOMERS XLSX EXPORT + RIDERS XLSX EXPORT) -->
  <section id="view-admin" class="hidden relative z-10 max-w-5xl w-full mx-auto px-4 py-8 flex-1">
    <div class="glass rounded-3xl p-6 space-y-8">
      <div class="flex flex-wrap justify-between items-center gap-4 border-b border-slate-800 pb-4">
        <h2 class="text-xl font-black text-white">Admin Control Center</h2>
        <div class="flex gap-2">
          <input type="password" id="admin-pass-input" placeholder="Password" class="bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs text-white" />
          <button onclick="loadAdminData()" class="px-4 py-2 rounded-xl bg-emerald-500 text-slate-950 font-bold text-xs">Unlock</button>
        </div>
      </div>

      <!-- Customer Notify Me Waitlist Section -->
      <div class="space-y-3">
        <div class="flex flex-wrap justify-between items-center gap-3">
          <div>
            <h3 class="text-lg font-black text-sky-400">Customer "Notify Me" Waitlist (<span id="admin-cust-count">0</span>)</h3>
            <p class="text-xs text-slate-400">Customers who filled Name & Mobile Number</p>
          </div>
          <button id="export-cust-btn" onclick="exportCustomersToExcel()" class="hidden px-4 py-2.5 rounded-xl bg-sky-500 hover:bg-sky-400 text-slate-950 font-black text-xs shadow-lg transition">
            📥 Export Customers (.xlsx)
          </button>
        </div>

        <div class="overflow-x-auto bg-slate-900/90 border border-slate-800 rounded-2xl">
          <table class="w-full text-left text-xs">
            <thead class="bg-slate-950 text-slate-400 uppercase border-b border-slate-800">
              <tr>
                <th class="px-4 py-3">#</th>
                <th class="px-4 py-3">Customer Name</th>
                <th class="px-4 py-3">Mobile Number</th>
                <th class="px-4 py-3">Submitted On</th>
              </tr>
            </thead>
            <tbody id="admin-customers-list" class="divide-y divide-slate-800 text-slate-200">
              <tr><td colspan="4" class="px-4 py-4 text-slate-500 text-center">Unlock Admin to view customer waitlist.</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Rider Verification Section -->
      <div class="space-y-3">
        <div class="flex flex-wrap justify-between items-center gap-3">
          <h3 class="text-lg font-black text-emerald-400">Rider Verification Queue (<span id="admin-rider-count">0</span>)</h3>
          <button id="export-rider-btn" onclick="exportRidersToExcel()" class="hidden px-4 py-2.5 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-xs shadow-lg transition">
            📥 Export Riders (.xlsx)
          </button>
        </div>
        <div id="admin-riders-list" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
      </div>
    </div>
  </section>

  <footer class="relative z-10 max-w-6xl w-full mx-auto px-4 py-4 text-center border-t border-slate-800/80 text-xs text-slate-400">
    Go<sup class="text-emerald-400">2</sup>Go • <strong class="text-amber-300">Co-Powered by CareerBoot</strong>
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
    let cachedAdminCustomers = [];
    let cachedAdminRiders = [];

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
      document.getElementById('rider-submit-btn').innerText = isReg ? 'Lock Queue Spot & Submit' : 'Login';
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
      finally { btn.disabled = false; btn.innerText = riderAuthMode === 'register' ? 'Lock Queue Spot & Submit' : 'Login'; }
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
        badge.innerText = 'Verified';
        badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30';
        document.getElementById('rd-pending-box').classList.add('hidden');
        document.getElementById('rd-active-box').classList.remove('hidden');
        startRiderHeartbeat();
      } else if (currentRider.status === 'rejected') {
        badge.innerText = 'Rejected';
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
        if (distEl) distEl.innerText = \`Drop Distance: \${Math.round(distMeters)}m\`;
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
      phoneEl.innerText = details.customerMobile;
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

      // Render Customer Waitlist
      cachedAdminCustomers = data.customers || [];
      document.getElementById('admin-cust-count').innerText = cachedAdminCustomers.length;
      const exportCustBtn = document.getElementById('export-cust-btn');
      const custBody = document.getElementById('admin-customers-list');

      if (cachedAdminCustomers.length === 0) {
        exportCustBtn.classList.add('hidden');
        custBody.innerHTML = '<tr><td colspan="4" class="px-4 py-4 text-slate-400 text-center">No customer signups yet.</td></tr>';
      } else {
        exportCustBtn.classList.remove('hidden');
        custBody.innerHTML = cachedAdminCustomers.map((c, idx) => {
          const dt = c.createdAt ? new Date(c.createdAt).toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' }) : '-';
          return \`
            <tr class="hover:bg-slate-800/50">
              <td class="px-4 py-3 font-bold text-slate-400">\${idx + 1}</td>
              <td class="px-4 py-3 font-bold text-white">\${c.name}</td>
              <td class="px-4 py-3 font-black text-sky-400">\${c.mobile}</td>
              <td class="px-4 py-3 text-slate-400">\${dt}</td>
            </tr>
          \`;
        }).join('');
      }

      // Render Riders
      cachedAdminRiders = data.riders || [];
      document.getElementById('admin-rider-count').innerText = cachedAdminRiders.length;
      const exportRiderBtn = document.getElementById('export-rider-btn');
      const container = document.getElementById('admin-riders-list');

      if (cachedAdminRiders.length === 0) {
        exportRiderBtn.classList.add('hidden');
        container.innerHTML = '<p class="text-slate-400 text-sm">No riders yet.</p>';
        return;
      }

      exportRiderBtn.classList.remove('hidden');
      container.innerHTML = cachedAdminRiders.map((r, idx) => \`
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 space-y-3">
          <div class="flex justify-between items-start">
            <div>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-slate-800 text-slate-300">Queue #\${idx + 1}</span>
              <h3 class="text-lg font-black text-white mt-1">\${r.name}</h3>
              <p class="text-xs text-slate-400">\${r.mobile} • \${r.vehicleType} (\${r.vehicleNumber})</p>
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

    function exportCustomersToExcel() {
      if (!cachedAdminCustomers || cachedAdminCustomers.length === 0) {
        return alert('No customer data to export.');
      }
      const rows = cachedAdminCustomers.map((c, idx) => ({
        'S.No': idx + 1,
        'Customer Name': String(c.name || ''),
        'Mobile Number': String(c.mobile || ''),
        'Registered At': c.createdAt ? new Date(c.createdAt).toLocaleString('en-IN') : ''
      }));

      const today = new Date().toISOString().slice(0, 10);
      if (typeof XLSX !== 'undefined') {
        const ws = XLSX.utils.json_to_sheet(rows);
        ws['!cols'] = [{ wch: 8 }, { wch: 26 }, { wch: 18 }, { wch: 24 }];
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, 'Customer Waitlist');
        XLSX.writeFile(wb, \`Go2Go_Customers_Waitlist_\${today}.xlsx\`);
      } else {
        const headers = ['S.No', 'Customer Name', 'Mobile Number', 'Registered At'];
        const csvRows = rows.map(r => [
          r['S.No'],
          '"' + r['Customer Name'].replace(/"/g, '""') + '"',
          '="' + r['Mobile Number'] + '"',
          '"' + r['Registered At'] + '"'
        ].join(','));
        const csvContent = '\\uFEFF' + [headers.join(','), ...csvRows].join('\\r\\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = \`Go2Go_Customers_Waitlist_\${today}.csv\`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
      }
    }

    function exportRidersToExcel() {
      if (!cachedAdminRiders || cachedAdminRiders.length === 0) {
        return alert('No rider data to export.');
      }
      const rows = cachedAdminRiders.map((r, idx) => ({
        'Queue Rank': idx + 1,
        'Rider Name': String(r.name || ''),
        'Mobile Number': String(r.mobile || ''),
        'Vehicle Type': String(r.vehicleType || ''),
        'Vehicle Number': String(r.vehicleNumber || ''),
        'Vehicle Model': String(r.vehicleModel || ''),
        'Status': String(r.status || '').toUpperCase(),
        'Joined At': r.joinedAt ? new Date(r.joinedAt).toLocaleString('en-IN') : ''
      }));

      const today = new Date().toISOString().slice(0, 10);
      if (typeof XLSX !== 'undefined') {
        const ws = XLSX.utils.json_to_sheet(rows);
        ws['!cols'] = [{ wch: 12 }, { wch: 24 }, { wch: 16 }, { wch: 14 }, { wch: 18 }, { wch: 22 }, { wch: 14 }, { wch: 24 }];
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, 'Rider Queue');
        XLSX.writeFile(wb, \`Go2Go_Riders_Queue_\${today}.xlsx\`);
      }
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

    socket.on('new_customer_waitlist', () => {
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

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

// Helper: Calculate Daily, Weekly, Monthly Stats for a Rider
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
      io.to(`ride_${rideId}`).emit('ride_status_update', {
        status: 'searching',
        message: 'No verified riders online right now. Retrying in 8 seconds...'
      });
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

    io.to(`ride_${rideId}`).emit('ride_status_update', {
      status: 'searching',
      message: `Ringing Rider #${state.currentIndex + 1} (${targetRider.name}) in queue...`
    });

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
  if (state && state.timer) {
    clearTimeout(state.timer);
  }
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

  socket.on('join_ride_room', (rideId) => {
    socket.join(`ride_${rideId}`);
  });

  socket.on('join_admin_room', (pass) => {
    if (pass === ADMIN_PASS) {
      socket.join('admin_room');
    }
  });

  socket.on('accept_ride', async ({ rideId, riderId }) => {
    try {
      const ride = await Ride.findById(rideId);
      if (!ride || ride.status !== 'searching') {
        socket.emit('accept_failed', { message: 'Ride already taken or cancelled.' });
        return;
      }

      const rider = await Rider.findById(riderId);
      if (!rider || rider.status !== 'approved') {
        socket.emit('accept_failed', { message: 'Rider account not verified.' });
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

      io.to(`ride_${rideId}`).emit('ride_accepted_by_rider', {
        rideId: ride._id,
        riderName: rider.name,
        riderMobile: rider.mobile,
        riderVehicle: ride.riderVehicle,
        riderPhoto: rider.profileImage,
        fare: ride.fare,
        distanceKm: ride.distanceKm
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
        customerName: ride.customerName,
        riderName: ride.riderName,
        pickupAddress: ride.pickupAddress,
        dropAddress: ride.dropAddress,
        distanceKm: ride.distanceKm,
        fare: ride.fare,
        stats
      };

      if (targetRiderId) {
        io.to(`rider_${targetRiderId}`).emit('ride_completed_popup', completionPayload);
      }
      io.to(`ride_${rideId}`).emit('ride_completed_popup', completionPayload);
    } catch (err) {
      console.error('Complete ride error:', err);
    }
  });

  socket.on('cancel_ride', async (rideId) => {
    clearDispatch(rideId);
    await Ride.findByIdAndUpdate(rideId, { status: 'cancelled' });
    io.emit('ring_timeout', { rideId });
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

// Public live rider count to create FOMO & social proof for bulk rider registration
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
      return res.status(400).json({ error: 'Please enter a valid 10-digit mobile number.' });
    }

    const existing = await Rider.findOne({ mobile });

    if (mode === 'login') {
      if (!existing) return res.status(404).json({ error: 'No rider account found with this mobile number. Please register first.' });
      const queuePosition = await Rider.countDocuments({ joinedAt: { $lte: existing.joinedAt } });
      const stats = await computeRiderStats(existing._id);
      return res.json({ rider: existing, queuePosition, stats });
    }

    if (existing) {
      return res.status(400).json({ error: 'This mobile number is already registered. Please switch to Login.' });
    }

    if (!name || !vehicleType || !vehicleNumber || !vehicleModel || !aadhaarImage || !vehicleImage) {
      return res.status(400).json({ error: 'All fields, Aadhaar Card, and Vehicle Image are required.' });
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

// Customer Early Waitlist Registration (While Customer Section is "Live Soon")
app.post('/api/customer/waitlist', async (req, res) => {
  try {
    const { name, mobile } = req.body;
    if (!name || !mobile || !/^\d{10}$/.test(mobile)) {
      return res.status(400).json({ error: 'Please enter your Name and a valid 10-digit Mobile Number.' });
    }
    const existing = await Customer.findOne({ mobile });
    if (existing) {
      return res.json({ message: 'You are already on the priority launch list! We will notify you first.' });
    }
    await Customer.create({ name, mobile });
    res.json({ message: 'You are on the VIP Launch List! We will notify you as soon as rides go live.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/rides/book', async (req, res) => {
  return res.status(403).json({
    error: 'Customer ride bookings are launching soon! We are currently onboarding verified riders.'
  });
});

app.post('/api/admin/riders', async (req, res) => {
  const { password } = req.body;
  if (password !== ADMIN_PASS) return res.status(401).json({ error: 'Invalid Admin Password' });

  const riders = await Rider.find().sort({ joinedAt: 1 });
  const rides = await Ride.find().sort({ createdAt: -1 }).limit(20);
  res.json({ riders, rides });
});

app.post('/api/admin/verify-rider', async (req, res) => {
  const { password, riderId, status } = req.body;
  if (password !== ADMIN_PASS) return res.status(401).json({ error: 'Unauthorized' });

  const rider = await Rider.findByIdAndUpdate(riderId, { status }, { new: true });
  io.to(`rider_${riderId}`).emit('verification_status_changed', { status });

  res.json({ rider });
});

// ==========================================
// FRONTEND SINGLE-PAGE APPLICATION (HTML/CSS/JS)
// ==========================================
app.get('*', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Go²Go | Join Early as a Rider • 0% Commission • ₹7/km</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="/socket.io/socket.io.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;800;900&display=swap" rel="stylesheet">
  <style>
    body {
      font-family: 'Outfit', sans-serif;
      background: radial-gradient(circle at top left, #0f172a, #020617 70%);
      color: #f8fafc;
      min-height: 100vh;
      overflow-x: hidden;
    }
    .hero-glow {
      position: fixed;
      width: 550px;
      height: 550px;
      background: radial-gradient(circle, rgba(16,185,129,0.22) 0%, rgba(59,130,246,0.08) 50%, transparent 70%);
      top: -120px;
      left: 50%;
      transform: translateX(-50%);
      pointer-events: none;
      z-index: 0;
    }
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
      border: 1px solid rgba(96, 165, 250, 0.35);
      box-shadow: 0 14px 0 #090d16, 0 25px 40px rgba(30, 58, 138, 0.25);
    }
    .glass {
      background: rgba(15, 23, 42, 0.88);
      backdrop-filter: blur(16px);
      border: 1px solid rgba(255, 255, 255, 0.1);
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
  <div class="hero-glow"></div>

  <nav class="relative z-20 max-w-6xl w-full mx-auto px-4 py-4 flex justify-between items-center">
    <div onclick="showView('landing')" class="flex items-center gap-3 cursor-pointer">
      <div class="w-11 h-11 rounded-2xl bg-gradient-to-tr from-emerald-400 to-blue-500 flex items-center justify-center shadow-lg shadow-emerald-500/20">
        <svg class="w-6 h-6 text-slate-950" fill="none" stroke="currentColor" stroke-width="2.8" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z"/>
        </svg>
      </div>
      <div>
        <span class="text-2xl font-black tracking-tight leading-none block">Go<sup class="text-emerald-400 text-lg">2</sup>Go</span>
        <span class="text-[10px] text-slate-400 font-semibold">Co-Powered by CareerBoot</span>
      </div>
    </div>
    <div class="flex items-center gap-3">
      <span class="hidden sm:inline-block text-xs font-semibold px-3 py-1.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
        ₹7/km • 0% Commission Forever
      </span>
      <button onclick="showView('admin')" class="text-xs text-slate-400 hover:text-white px-3 py-1.5 rounded-lg border border-slate-800 hover:border-slate-600 transition">
        Admin Panel
      </button>
    </div>
  </nav>

  <!-- VIEW 1: LANDING (BULK RIDER ONBOARDING FOCUS + CUSTOMER LIVE SOON) -->
  <main id="view-landing" class="relative z-10 max-w-5xl w-full mx-auto px-4 pt-6 pb-16 text-center flex-1">
    <!-- Early Rider Priority Banner -->
    <div class="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-emerald-500/15 border border-emerald-400/30 text-xs sm:text-sm text-emerald-300 font-bold mb-5">
      <span class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
      🚨 Mega Rider Onboarding Drive Open • Join First = Get Rides First!
    </div>

    <div class="flex flex-col items-center justify-center mb-6">
      <h1 class="text-5xl sm:text-7xl font-black tracking-tight mb-3">
        Go<span class="text-emerald-400">²</span>Go
      </h1>
      <p class="text-xl sm:text-3xl font-extrabold bg-gradient-to-r from-emerald-300 via-white to-blue-300 bg-clip-text text-transparent max-w-2xl">
        Zero Commission for Riders. Pocket-Friendly for You.
      </p>
      <p class="text-slate-300 mt-3 max-w-xl text-sm sm:text-base">
        Riders who join early get <strong class="text-emerald-400">Top Sequence Priority (#1, #2, #3...)</strong> when customer rides launch! Keep <strong class="text-white">100% of ₹7/km</strong> fare with zero platform cuts.
      </p>
      <div class="mt-3 inline-block px-4 py-1.5 rounded-full bg-blue-500/10 border border-blue-400/20 text-xs font-extrabold tracking-wide text-blue-300">
        ⚡ Co-Powered by CareerBoot
      </div>
    </div>

    <!-- High-Conversion Rider Value Props -->
    <div class="grid grid-cols-3 gap-3 max-w-2xl mx-auto mb-8">
      <div class="glass rounded-2xl p-3 sm:p-4">
        <div class="text-xl sm:text-2xl font-black text-emerald-400">0%</div>
        <div class="text-[11px] sm:text-xs text-slate-300 font-semibold">Commission Cut</div>
      </div>
      <div class="glass rounded-2xl p-3 sm:p-4">
        <div class="text-xl sm:text-2xl font-black text-white">1st</div>
        <div class="text-[11px] sm:text-xs text-slate-300 font-semibold">Joined = 1st Ride</div>
      </div>
      <div class="glass rounded-2xl p-3 sm:p-4">
        <div class="text-xl sm:text-2xl font-black text-amber-400" id="live-rider-counter">Active</div>
        <div class="text-[11px] sm:text-xs text-slate-300 font-semibold">Rider Queue Open</div>
      </div>
    </div>

    <!-- Two 4D Role Selection Buttons -->
    <div class="stage-4d grid grid-cols-1 md:grid-cols-2 gap-8 max-w-3xl mx-auto px-2">
      <!-- Button 1: I'm Rider (Primary Active CTA) -->
      <div onclick="showView('rider-auth')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d btn-4d-rider rounded-3xl p-8 text-left select-none relative">
        <span class="absolute top-5 right-5 px-3 py-1 rounded-full bg-slate-950/80 text-emerald-300 text-[10px] font-black uppercase tracking-wider">
          OPEN NOW • JOIN FAST
        </span>
        <div class="w-14 h-14 rounded-2xl bg-white/20 flex items-center justify-center mb-5 backdrop-blur-sm">
          <svg class="w-8 h-8 text-white" fill="none" stroke="currentColor" stroke-width="2.2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4"/>
          </svg>
        </div>
        <div class="text-xs uppercase tracking-widest font-bold text-emerald-100 mb-1">Lock Your Top Queue Spot</div>
        <h2 class="text-3xl font-black text-white">1. I'm Rider</h2>
        <p class="text-emerald-100/95 text-sm mt-2">Register Bike, Scooter, Auto or Cab now. Get verified & keep 100% earnings!</p>
      </div>

      <!-- Button 2: I'm Customer (Live Soon Badge) -->
      <div onclick="showView('customer-soon')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d btn-4d-customer rounded-3xl p-8 text-left select-none relative">
        <span class="absolute top-5 right-5 px-3 py-1 rounded-full bg-amber-400 text-slate-950 text-[10px] font-black uppercase tracking-wider animate-pulse">
          🚀 LIVE SOON
        </span>
        <div class="w-14 h-14 rounded-2xl bg-blue-500/20 flex items-center justify-center mb-5 backdrop-blur-sm">
          <svg class="w-8 h-8 text-blue-300" fill="none" stroke="currentColor" stroke-width="2.2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"/>
            <path stroke-linecap="round" stroke-linejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"/>
          </svg>
        </div>
        <div class="text-xs uppercase tracking-widest font-bold text-blue-300 mb-1">Launching Shortly • ₹7/km</div>
        <h2 class="text-3xl font-black text-white">2. I'm Customer</h2>
        <p class="text-slate-300 text-sm mt-2">Customer ride bookings will be live soon! Tap to join the VIP launch alert list.</p>
      </div>
    </div>
  </main>

  <!-- VIEW 2: RIDER AUTH (BULK ONBOARDING) -->
  <section id="view-rider-auth" class="hidden relative z-10 max-w-xl w-full mx-auto px-4 py-8 flex-1">
    <div class="glass rounded-3xl p-6 sm:p-8 shadow-2xl border border-emerald-500/30">
      <div class="bg-emerald-500/10 border border-emerald-500/30 rounded-2xl p-3 mb-5 text-center">
        <p class="text-xs font-extrabold text-emerald-300">
          🔥 Early Bird Advantage: Riders are aligned strictly in registration order! Register now to lock your top spot.
        </p>
      </div>

      <div class="flex justify-between items-center mb-6">
        <h2 class="text-2xl font-black text-emerald-400">Rider Registration</h2>
        <div class="flex bg-slate-900 p-1 rounded-xl border border-slate-800">
          <button id="tab-rider-reg" onclick="switchRiderTab('register')" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950">New Rider</button>
          <button id="tab-rider-log" onclick="switchRiderTab('login')" class="px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400">Login</button>
        </div>
      </div>

      <form onsubmit="handleRiderSubmit(event)" class="space-y-4">
        <div id="rider-reg-fields" class="space-y-4">
          <div>
            <label class="block text-xs font-bold text-slate-400 uppercase mb-1">Full Name *</label>
            <input type="text" id="r-name" placeholder="Enter your full name" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-white focus:border-emerald-400 outline-none" />
          </div>
        </div>
        <div>
          <label class="block text-xs font-bold text-slate-400 uppercase mb-1">Mobile Number (1 Number = 1 Account) *</label>
          <input type="tel" id="r-mobile" maxlength="10" placeholder="10-digit mobile number" required class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-white focus:border-emerald-400 outline-none" />
        </div>
        <div id="rider-vehicle-fields" class="space-y-4">
          <div class="grid grid-cols-2 gap-3">
            <div>
              <label class="block text-xs font-bold text-slate-400 uppercase mb-1">Vehicle Type *</label>
              <select id="r-vtype" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-3 py-3 text-white outline-none">
                <option value="Bike">Bike / Motorcycle</option>
                <option value="Scooter">Scooter / Activa</option>
                <option value="Auto">Auto Rickshaw</option>
                <option value="Cab">Car / Cab</option>
              </select>
            </div>
            <div>
              <label class="block text-xs font-bold text-slate-400 uppercase mb-1">Vehicle Number *</label>
              <input type="text" id="r-vnum" placeholder="UP32 AB 1234" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-white uppercase outline-none" />
            </div>
          </div>
          <div>
            <label class="block text-xs font-bold text-slate-400 uppercase mb-1">Vehicle Model & Color *</label>
            <input type="text" id="r-vmodel" placeholder="e.g., Honda Shine Black" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-white outline-none" />
          </div>
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
              <label class="block text-xs font-bold text-emerald-400 mb-1">Upload Aadhaar Card *</label>
              <input type="file" id="r-aadhaar" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1.5 file:px-3 file:rounded-lg file:border-0 file:bg-emerald-500/20 file:text-emerald-300" />
            </div>
            <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
              <label class="block text-xs font-bold text-emerald-400 mb-1">Upload Vehicle Image *</label>
              <input type="file" id="r-vimg" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1.5 file:px-3 file:rounded-lg file:border-0 file:bg-emerald-500/20 file:text-emerald-300" />
            </div>
          </div>
          <div class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
            <label class="block text-xs font-bold text-slate-300 mb-1">Profile Picture (Optional)</label>
            <input type="file" id="r-profile" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1.5 file:px-3 file:rounded-lg file:border-0 file:bg-slate-700 file:text-white" />
          </div>
        </div>
        <button type="submit" id="rider-submit-btn" class="w-full py-4 rounded-2xl bg-gradient-to-r from-emerald-500 to-teal-600 font-black text-slate-950 text-lg shadow-lg hover:opacity-95 transition">
          Lock My Spot & Submit for Verification
        </button>
      </form>
    </div>
  </section>

  <!-- VIEW 3: RIDER DASHBOARD -->
  <section id="view-rider-dash" class="hidden relative z-10 max-w-3xl w-full mx-auto px-4 py-8 flex-1">
    <div class="glass rounded-3xl p-6 sm:p-8 shadow-2xl space-y-6">
      <div class="flex flex-wrap justify-between items-center gap-4 border-b border-slate-800 pb-5">
        <div class="flex items-center gap-3">
          <img id="rd-avatar" src="https://via.placeholder.com/60/10b981/ffffff?text=R" class="w-14 h-14 rounded-2xl object-cover border-2 border-emerald-400" />
          <div>
            <h2 id="rd-name" class="text-xl font-black text-white">Rider Name</h2>
            <p id="rd-vehicle" class="text-xs text-slate-400">Vehicle Info</p>
          </div>
        </div>
        <div id="rd-badge" class="px-3 py-1 rounded-full text-xs font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30">
          Pending Verification
        </div>
      </div>

      <!-- Queue Position Highlight -->
      <div class="bg-gradient-to-r from-emerald-500/15 to-blue-500/15 border border-emerald-400/30 rounded-2xl p-4 flex flex-wrap justify-between items-center gap-3">
        <div>
          <span class="text-xs font-bold uppercase text-emerald-300">Your Locked Queue Position</span>
          <div class="text-2xl font-black text-white">Sequence #<span id="rd-seq-top">1</span></div>
        </div>
        <p class="text-xs text-slate-300 max-w-xs">
          Share Go²Go with fellow riders! Customer bookings will go live shortly and ring riders starting from #1.
        </p>
      </div>

      <!-- DETAILED BOOKING & EARNINGS DASHBOARD (DAILY, WEEKLY, MONTHLY) -->
      <div>
        <div class="flex justify-between items-center mb-3">
          <h3 class="text-sm font-black uppercase tracking-wider text-emerald-400">📊 Bookings & Earnings Summary (0% Commission)</h3>
          <button onclick="refreshRiderStats()" class="text-xs text-slate-400 hover:text-white underline">Refresh</button>
        </div>
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4">
            <div class="text-xs font-extrabold uppercase text-emerald-400 mb-2">Today (Daily)</div>
            <div class="text-2xl font-black text-white">₹<span id="stat-daily-earn">0</span>/-</div>
            <div class="text-xs text-slate-400 mt-1">
              Rides: <strong class="text-white" id="stat-daily-rides">0</strong> • Distance: <strong class="text-white" id="stat-daily-km">0</strong> km
            </div>
          </div>
          <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4">
            <div class="text-xs font-extrabold uppercase text-blue-400 mb-2">This Week</div>
            <div class="text-2xl font-black text-white">₹<span id="stat-weekly-earn">0</span>/-</div>
            <div class="text-xs text-slate-400 mt-1">
              Rides: <strong class="text-white" id="stat-weekly-rides">0</strong> • Distance: <strong class="text-white" id="stat-weekly-km">0</strong> km
            </div>
          </div>
          <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4">
            <div class="text-xs font-extrabold uppercase text-purple-400 mb-2">This Month</div>
            <div class="text-2xl font-black text-white">₹<span id="stat-monthly-earn">0</span>/-</div>
            <div class="text-xs text-slate-400 mt-1">
              Rides: <strong class="text-white" id="stat-monthly-rides">0</strong> • Distance: <strong class="text-white" id="stat-monthly-km">0</strong> km
            </div>
          </div>
        </div>
      </div>

      <div id="rd-pending-box" class="bg-amber-500/10 border border-amber-500/30 rounded-2xl p-5 text-center">
        <h3 class="text-lg font-bold text-amber-300">Verification Request Submitted</h3>
        <p class="text-sm text-slate-300 mt-1">Admin has been notified to verify your Aadhaar & Vehicle details. Your early queue spot is safely reserved!</p>
      </div>

      <div id="rd-active-box" class="hidden space-y-5">
        <div class="flex flex-wrap items-center justify-between gap-2 bg-slate-900/90 p-4 rounded-2xl border border-slate-800">
          <div>
            <p class="text-xs text-slate-400 uppercase font-bold">Verification Status</p>
            <p class="text-base font-black text-emerald-400">Approved & Ready for Launch</p>
          </div>
          <div class="flex items-center gap-2">
            <button onclick="unlockAndTestSound()" class="px-3 py-2 rounded-xl font-bold text-xs bg-amber-500/20 text-amber-300 border border-amber-500/30 hover:bg-amber-500/30">
              🔊 Test Ringtone
            </button>
            <button id="rd-online-btn" onclick="toggleRiderOnline()" class="px-4 py-2 rounded-xl font-bold text-xs bg-emerald-500 text-slate-950">
              ● ONLINE
            </button>
          </div>
        </div>

        <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 text-center">
          <div class="flex justify-center items-center gap-2 text-xs text-emerald-400 font-bold mb-1">
            <span id="socket-status-dot" class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
            <span id="socket-status-text">Live Dispatch Connected</span>
          </div>
          <p class="font-bold text-white">You are verified and locked in the queue!</p>
          <p class="text-xs text-slate-400 mt-1">Customer rides will be live soon. You will receive instant loud ring alerts as soon as bookings start.</p>
        </div>

        <!-- ACTIVE RIDE & GPS AUTO-COMPLETION CARD -->
        <div id="rd-accepted-card" class="hidden bg-emerald-950/60 border-2 border-emerald-400 rounded-2xl p-5 space-y-4">
          <div class="flex justify-between items-center">
            <span class="text-xs font-black uppercase tracking-wider text-emerald-400">Active Ride In Progress</span>
            <span id="rd-ride-fare" class="text-2xl font-black text-white">₹0</span>
          </div>
          <div class="bg-slate-950/70 p-4 rounded-xl space-y-1">
            <p class="text-xs text-slate-400">Customer Details:</p>
            <p class="text-lg font-black text-white" id="rd-cust-name">-</p>
            <a id="rd-cust-phone" href="#" class="inline-block text-emerald-400 font-bold text-base underline">📞 -</a>
          </div>
          <div class="text-xs space-y-1 text-slate-300">
            <p><strong class="text-emerald-400">Pickup:</strong> <span id="rd-cust-pickup"></span></p>
            <p><strong class="text-blue-400">Drop:</strong> <span id="rd-cust-drop"></span></p>
            <p id="rd-gps-drop-dist" class="text-amber-300 font-bold pt-1">📍 Monitoring GPS to drop point...</p>
          </div>
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <a id="rd-gmaps-nav" target="_blank" href="#" class="text-center py-3 rounded-xl bg-blue-600 hover:bg-blue-500 font-bold text-sm text-white">
              🗺️ Open Map Navigation
            </a>
            <button onclick="triggerRideCompletion()" class="py-3 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-sm">
              ✓ Complete Ride Now
            </button>
          </div>
        </div>
      </div>
    </div>
  </section>

  <!-- VIEW 4: CUSTOMER SECTION — LIVE SOON + VIP WAITLIST -->
  <section id="view-customer-soon" class="hidden relative z-10 max-w-lg w-full mx-auto px-4 py-10 flex-1">
    <div class="glass rounded-3xl p-7 sm:p-9 shadow-2xl border border-blue-500/30 text-center space-y-5">
      <div class="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-amber-400/15 border border-amber-400/30 text-amber-300 text-xs font-black uppercase tracking-wider">
        🚀 Launching Very Soon
      </div>

      <h2 class="text-3xl sm:text-4xl font-black text-white">
        Customer Rides Will Be <span class="text-blue-400">Live Soon!</span>
      </h2>

      <p class="text-sm text-slate-300 leading-relaxed">
        We are currently verifying and onboarding our fleet of trusted local riders so you get <strong class="text-white">instant pickups at just ₹7/km</strong> with zero surge pricing or hidden charges.
      </p>

      <!-- Optional VIP Launch Notification Form -->
      <form onsubmit="handleCustomerWaitlist(event)" class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 text-left space-y-3">
        <div class="text-xs font-extrabold text-blue-300 uppercase">Get Notified First When Bookings Open:</div>
        <input type="text" id="cw-name" placeholder="Your Full Name" required class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white outline-none focus:border-blue-400" />
        <input type="tel" id="cw-mobile" maxlength="10" placeholder="10-Digit Mobile Number" required class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white outline-none focus:border-blue-400" />
        <button type="submit" id="cw-btn" class="w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-500 font-black text-sm text-white transition">
          🔔 Notify Me on Launch
        </button>
      </form>

      <div class="pt-3 border-t border-slate-800">
        <p class="text-xs text-slate-400 mb-2">Have a Bike, Scooter, Auto, or Car?</p>
        <button onclick="showView('rider-auth')" class="w-full py-3.5 rounded-2xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-sm shadow-lg">
          Join Now as a Rider (0% Commission) →
        </button>
      </div>
    </div>
  </section>

  <!-- LOUD RIDER RING MODAL -->
  <div id="ring-modal" class="hidden fixed inset-0 z-50 bg-slate-950/95 backdrop-blur-md flex items-center justify-center p-4">
    <div class="glass border-2 border-emerald-400 rounded-3xl max-w-md w-full p-6 text-center ring-pulse">
      <div class="text-xs font-black uppercase tracking-widest text-emerald-400 mb-1">🚨 New Ride Incoming!</div>
      <div class="text-4xl font-black text-white my-2" id="ring-fare">₹0</div>
      <div class="text-xs font-bold text-slate-300 mb-4" id="ring-dist">0 km • 100% Yours (0% Commission)</div>
      <div class="bg-slate-900/90 rounded-2xl p-4 text-left text-xs space-y-2 mb-5 border border-slate-800">
        <div>
          <span class="text-emerald-400 font-bold block">PICKUP:</span>
          <span id="ring-pickup" class="text-white font-semibold"></span>
        </div>
        <div class="border-t border-slate-800 pt-2">
          <span class="text-blue-400 font-bold block">DROP:</span>
          <span id="ring-drop" class="text-white font-semibold"></span>
        </div>
      </div>
      <div class="mb-4">
        <div class="text-sm font-black text-amber-400">Auto-passing to next rider in <span id="ring-timer">20</span>s</div>
      </div>
      <button onclick="acceptIncomingRide()" class="w-full py-4 rounded-2xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-lg shadow-xl">
        ACCEPT RIDE NOW
      </button>
    </div>
  </div>

  <!-- RIDE COMPLETION POP-UP MODAL -->
  <div id="complete-modal" class="hidden fixed inset-0 z-50 bg-slate-950/90 backdrop-blur-md flex items-center justify-center p-4">
    <div class="glass border-2 border-emerald-400 rounded-3xl max-w-md w-full p-6 text-center space-y-4">
      <div class="w-16 h-16 rounded-full bg-emerald-500/20 text-emerald-400 flex items-center justify-center mx-auto text-3xl font-black">
        ✓
      </div>
      <h3 class="text-2xl font-black text-white">Ride Completed!</h3>
      <p class="text-xs text-slate-300">Destination reached on Map GPS. Trip summary recorded permanently.</p>
      <div class="bg-slate-900/90 rounded-2xl p-4 text-left text-xs space-y-2 border border-slate-800">
        <div class="flex justify-between"><span class="text-slate-400">Distance:</span><strong class="text-white" id="cmp-dist">0 km</strong></div>
        <div class="flex justify-between"><span class="text-slate-400">Total Fare (₹7/km):</span><strong class="text-emerald-400 text-base" id="cmp-fare">₹0/-</strong></div>
        <div class="text-slate-400 truncate"><span class="text-emerald-400 font-bold">From:</span> <span id="cmp-pickup" class="text-white"></span></div>
        <div class="text-slate-400 truncate"><span class="text-blue-400 font-bold">To:</span> <span id="cmp-drop" class="text-white"></span></div>
      </div>
      <button onclick="closeCompletionModal()" class="w-full py-3.5 rounded-2xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-base">
        Done
      </button>
    </div>
  </div>

  <!-- VIEW 6: ADMIN -->
  <section id="view-admin" class="hidden relative z-10 max-w-5xl w-full mx-auto px-4 py-8 flex-1">
    <div class="glass rounded-3xl p-6 sm:p-8">
      <div class="flex flex-wrap justify-between items-center gap-4 mb-6">
        <div>
          <h2 class="text-2xl font-black text-white">Admin Verification & Queue Monitor</h2>
          <p class="text-xs text-slate-400">Approve onboarded riders in sequence order</p>
        </div>
        <div class="flex gap-2">
          <input type="password" id="admin-pass-input" placeholder="Enter Admin Password" class="bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs text-white" />
          <button onclick="loadAdminData()" class="px-4 py-2 rounded-xl bg-emerald-500 text-slate-950 font-bold text-xs">Unlock / Refresh</button>
        </div>
      </div>
      <div id="admin-riders-list" class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <p class="text-sm text-slate-400">Enter admin password and click Unlock to view pending rider requests.</p>
      </div>
    </div>
  </section>

  <!-- FOOTER: CO-POWERED BY CAREERBOOT -->
  <footer class="relative z-10 max-w-6xl w-full mx-auto px-4 py-5 text-center border-t border-slate-800/80 text-xs text-slate-400">
    Go<sup class="text-emerald-400">2</sup>Go • ₹7/km Zero Commission • <strong class="text-white">Co-Powered by CareerBoot</strong>
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

    // Load live rider count on landing page
    fetch('/api/public/stats')
      .then(r => r.json())
      .then(d => {
        if (d.totalRiders > 0) {
          document.getElementById('live-rider-counter').innerText = d.totalRiders + '+';
        }
      })
      .catch(() => {});

    socket.on('connect', () => {
      const dot = document.getElementById('socket-status-dot');
      const txt = document.getElementById('socket-status-text');
      if (dot) dot.className = 'w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping';
      if (txt) txt.innerText = 'Live Dispatch Connected';
      if (currentRider && currentRider._id) {
        socket.emit('register_rider_socket', currentRider._id);
      }
    });

    socket.on('disconnect', () => {
      const dot = document.getElementById('socket-status-dot');
      const txt = document.getElementById('socket-status-text');
      if (dot) dot.className = 'w-2.5 h-2.5 rounded-full bg-red-500';
      if (txt) txt.innerText = 'Reconnecting to Dispatch...';
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
      if ('Notification' in window && Notification.permission !== 'granted') {
        Notification.requestPermission();
      }
      playLoudSiren();
    }

    function playLoudSiren() {
      try {
        if (navigator.vibrate) {
          navigator.vibrate([500, 200, 500, 200, 500]);
        }
        if (!sharedAudioCtx) {
          sharedAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
        }
        if (sharedAudioCtx.state === 'suspended') {
          sharedAudioCtx.resume();
        }
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

        osc1.start(now);
        osc2.start(now);
        osc1.stop(now + 0.9);
        osc2.stop(now + 0.9);
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

    // RIDER AUTH & STATS
    function switchRiderTab(mode) {
      riderAuthMode = mode;
      const isReg = mode === 'register';
      document.getElementById('rider-reg-fields').classList.toggle('hidden', !isReg);
      document.getElementById('rider-vehicle-fields').classList.toggle('hidden', !isReg);
      document.getElementById('rider-submit-btn').innerText = isReg ? 'Lock My Spot & Submit for Verification' : 'Login to Rider Dashboard';
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
      btn.disabled = true;
      btn.innerText = 'Processing...';
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
      finally { btn.disabled = false; btn.innerText = riderAuthMode === 'register' ? 'Lock My Spot & Submit for Verification' : 'Login to Rider Dashboard'; }
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
      document.getElementById('rd-vehicle').innerText = \`\${currentRider.vehicleType} • \${currentRider.vehicleModel} (\${currentRider.vehicleNumber})\`;
      document.getElementById('rd-seq-top').innerText = queuePos || 1;
      if (currentRider.profileImage) document.getElementById('rd-avatar').src = currentRider.profileImage;
      socket.emit('register_rider_socket', currentRider._id);
      refreshRiderStats();

      const badge = document.getElementById('rd-badge');
      if (currentRider.status === 'approved') {
        badge.innerText = '✓ Verified Rider';
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
        if (currentRider && currentRider._id && currentRider.isOnline) {
          socket.emit('register_rider_socket', currentRider._id);
        }
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
      btn.className = currentRider.isOnline ? 'px-4 py-2 rounded-xl font-bold text-xs bg-emerald-500 text-slate-950' : 'px-4 py-2 rounded-xl font-bold text-xs bg-slate-700 text-slate-300';
    }

    // CUSTOMER WAITLIST SUBMIT
    async function handleCustomerWaitlist(e) {
      e.preventDefault();
      const btn = document.getElementById('cw-btn');
      btn.disabled = true;
      try {
        const res = await fetch('/api/customer/waitlist', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: document.getElementById('cw-name').value.trim(),
            mobile: document.getElementById('cw-mobile').value.trim()
          })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error);
        alert(data.message);
        document.getElementById('cw-name').value = '';
        document.getElementById('cw-mobile').value = '';
      } catch (err) {
        alert(err.message);
      } finally {
        btn.disabled = false;
      }
    }

    socket.on('verification_status_changed', ({ status }) => {
      if (currentRider) { currentRider.status = status; renderRiderDashboard(document.getElementById('rd-seq-top').innerText); }
    });

    socket.on('incoming_ride_ring', (ride) => {
      if (incomingRingRideId === ride.rideId) return;
      incomingRingRideId = ride.rideId;
      document.getElementById('ring-fare').innerText = '₹' + ride.fare;
      document.getElementById('ring-dist').innerText = \`\${ride.distanceKm} km • 100% Yours (0% Commission)\`;
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

    socket.on('ring_timeout', ({ rideId }) => {
      if (incomingRingRideId === rideId) stopRiderRinging();
    });

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
        const distMeters = getDistanceMeters(
          pos.coords.latitude,
          pos.coords.longitude,
          rideDetails.dropCoords.lat,
          rideDetails.dropCoords.lng
        );
        const distEl = document.getElementById('rd-gps-drop-dist');
        if (distEl) {
          distEl.innerText = \`📍 GPS Distance to Drop: \${Math.round(distMeters)}m (Auto-completes within 150m)\`;
        }
        if (distMeters <= 150 && currentAcceptedRide) {
          navigator.geolocation.clearWatch(gpsWatchId);
          gpsWatchId = null;
          triggerRideCompletion();
        }
      }, null, { enableHighAccuracy: true, maximumAge: 5000 });
    }

    function triggerRideCompletion() {
      if (!currentAcceptedRide) return;
      if (gpsWatchId) {
        navigator.geolocation.clearWatch(gpsWatchId);
        gpsWatchId = null;
      }
      socket.emit('complete_ride', {
        rideId: currentAcceptedRide.rideId,
        riderId: currentRider ? currentRider._id : null
      });
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

    function closeCompletionModal() {
      document.getElementById('complete-modal').classList.add('hidden');
    }

    socket.on('accept_failed', ({ message }) => alert(message));

    // ADMIN
    async function loadAdminData() {
      const password = document.getElementById('admin-pass-input').value;
      socket.emit('join_admin_room', password);
      const res = await fetch('/api/admin/riders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password }) });
      const data = await res.json();
      if (!res.ok) return alert(data.error);

      const container = document.getElementById('admin-riders-list');
      if (data.riders.length === 0) { container.innerHTML = '<p class="text-slate-400 text-sm">No riders registered yet.</p>'; return; }

      container.innerHTML = data.riders.map((r, idx) => \`
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 space-y-3">
          <div class="flex justify-between items-start">
            <div>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-slate-800 text-slate-300">Queue #\${idx + 1}</span>
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
      alert(\`New Rider Verification Request: \${rider.name} (\${rider.vehicleNumber})\`);
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

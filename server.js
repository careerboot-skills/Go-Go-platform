require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 1e7 // 10MB limit for base64 document uploads
});

app.use(express.json({ limit: '15mb' }));
app.use(express.urlencoded({ extended: true, limit: '15mb' }));

// ==========================================
// MONGODB CONNECTION & SCHEMAS
// ==========================================
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/go2go';
const ADMIN_PASS = process.env.ADMIN_PASS || 'admin123';

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
  joinedAt: { type: Date, default: Date.now } // Used for First-Joined-First-Served sequence
});

const customerSchema = new mongoose.Schema({
  name: { type: String, required: true, trim: true },
  mobile: { type: String, required: true, unique: true, trim: true },
  aadhaarImage: { type: String, required: true },
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
  createdAt: { type: Date, default: Date.now }
});

const Rider = mongoose.model('Rider', riderSchema);
const Customer = mongoose.model('Customer', customerSchema);
const Ride = mongoose.model('Ride', rideSchema);

// ==========================================
// REAL-TIME SEQUENTIAL DISPATCH ENGINE
// ==========================================
const activeDispatches = new Map(); // rideId -> { timer, currentIndex }
const connectedRiders = new Map();  // riderId -> socketId

async function dispatchNextRider(rideId) {
  try {
    const ride = await Ride.findById(rideId);
    if (!ride || ride.status !== 'searching') {
      clearDispatch(rideId);
      return;
    }

    // Fetch ALL approved & online riders strictly ordered by registration time (First joined = First served)
    const eligibleRiders = await Rider.find({ status: 'approved', isOnline: true }).sort({ joinedAt: 1 });

    if (eligibleRiders.length === 0) {
      io.to(`ride_${rideId}`).emit('ride_status_update', {
        status: 'searching',
        message: 'No verified riders online right now. Retrying in 10 seconds...'
      });
      const retryTimer = setTimeout(() => dispatchNextRider(rideId), 10000);
      activeDispatches.set(rideId, { timer: retryTimer, currentIndex: 0 });
      return;
    }

    let state = activeDispatches.get(rideId) || { timer: null, currentIndex: 0 };
    if (state.currentIndex >= eligibleRiders.length) {
      state.currentIndex = 0; // Loop back to the first rider in sequence
    }

    const targetRider = eligibleRiders[state.currentIndex];
    const targetSocketId = connectedRiders.get(targetRider._id.toString());

    io.to(`ride_${rideId}`).emit('ride_status_update', {
      status: 'searching',
      message: `Ringing Rider #${state.currentIndex + 1} (${targetRider.name}) in queue...`
    });

    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_ride_ring', {
        rideId: ride._id,
        pickupAddress: ride.pickupAddress,
        pickupCoords: ride.pickupCoords,
        dropAddress: ride.dropAddress,
        dropCoords: ride.dropCoords,
        distanceKm: ride.distanceKm,
        fare: ride.fare,
        timeoutSec: 20
      });
    }

    // Advance pointer and schedule 20-second ring timeout
    const nextIndex = (state.currentIndex + 1) % eligibleRiders.length;
    const timer = setTimeout(() => {
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
  // Rider comes online on dashboard
  socket.on('register_rider_socket', async (riderId) => {
    if (!riderId) return;
    connectedRiders.set(riderId, socket.id);
    socket.riderId = riderId;
    await Rider.findByIdAndUpdate(riderId, { isOnline: true });
  });

  // Customer subscribes to live ride updates
  socket.on('join_ride_room', (rideId) => {
    socket.join(`ride_${rideId}`);
  });

  // Admin room for live verification notifications
  socket.on('join_admin_room', (pass) => {
    if (pass === ADMIN_PASS) {
      socket.join('admin_room');
    }
  });

  // Rider accepts ride
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

      // Stop sequential loop
      clearDispatch(rideId);

      // Update Ride in MongoDB
      ride.status = 'accepted';
      ride.riderId = rider._id;
      ride.riderName = rider.name;
      ride.riderMobile = rider.mobile;
      ride.riderVehicle = `${rider.vehicleModel} (${rider.vehicleNumber})`;
      await ride.save();

      // Send Customer details ONLY now to the accepting Rider
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

      // Notify Customer live
      io.to(`ride_${rideId}`).emit('ride_accepted_by_rider', {
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

  // Customer cancels ride
  socket.on('cancel_ride', async (rideId) => {
    clearDispatch(rideId);
    await Ride.findByIdAndUpdate(rideId, { status: 'cancelled' });
    io.emit('ring_timeout', { rideId });
  });

  socket.on('disconnect', () => {
    if (socket.riderId) {
      connectedRiders.delete(socket.riderId);
    }
  });
});

// ==========================================
// REST API ROUTES
// ==========================================

// Rider Registration or Login
app.post('/api/rider/auth', async (req, res) => {
  try {
    const { mode, name, mobile, vehicleType, vehicleNumber, vehicleModel, aadhaarImage, vehicleImage, profileImage } = req.body;
    if (!mobile || !/^\d{10}$/.test(mobile)) {
      return res.status(400).json({ error: 'Please enter a valid 10-digit mobile number.' });
    }

    const existing = await Rider.findOne({ mobile });

    if (mode === 'login') {
      if (!existing) return res.status(404).json({ error: 'No rider account found with this mobile number. Please register first.' });
      const queuePosition = await Rider.countDocuments({ status: 'approved', joinedAt: { $lte: existing.joinedAt } });
      return res.json({ rider: existing, queuePosition });
    }

    // Registration mode (One Number = One Account)
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

    // Notify Admin in real-time
    io.to('admin_room').emit('new_rider_verification', {
      id: newRider._id,
      name: newRider.name,
      mobile: newRider.mobile,
      vehicleNumber: newRider.vehicleNumber
    });

    res.json({ rider: newRider, queuePosition: 0 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Toggle Rider Online/Offline
app.post('/api/rider/toggle-online', async (req, res) => {
  try {
    const { riderId, isOnline } = req.body;
    const rider = await Rider.findByIdAndUpdate(riderId, { isOnline }, { new: true });
    res.json({ rider });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Customer Registration or Login
app.post('/api/customer/auth', async (req, res) => {
  try {
    const { mode, name, mobile, aadhaarImage } = req.body;
    if (!mobile || !/^\d{10}$/.test(mobile)) {
      return res.status(400).json({ error: 'Please enter a valid 10-digit mobile number.' });
    }

    const existing = await Customer.findOne({ mobile });

    if (mode === 'login') {
      if (!existing) return res.status(404).json({ error: 'Account not found. Please register with your Aadhaar Card.' });
      return res.json({ customer: existing });
    }

    if (existing) {
      return res.status(400).json({ error: 'Mobile number already registered. Please login instead.' });
    }

    if (!name || !aadhaarImage) {
      return res.status(400).json({ error: 'Name and Aadhaar Card image are mandatory.' });
    }

    const customer = await Customer.create({ name, mobile, aadhaarImage });
    res.json({ customer });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Book a Ride & Start 20s Sequential Dispatch
app.post('/api/rides/book', async (req, res) => {
  try {
    const { customerName, customerMobile, pickupAddress, pickupCoords, dropAddress, dropCoords, distanceKm, fare } = req.body;
    if (!pickupCoords || !dropCoords || !distanceKm) {
      return res.status(400).json({ error: 'Please select valid Pickup and Drop locations on the map.' });
    }

    const ride = await Ride.create({
      customerName,
      customerMobile,
      pickupAddress,
      pickupCoords,
      dropAddress,
      dropCoords,
      distanceKm,
      fare,
      status: 'searching'
    });

    // Kick off sequential rider queue
    dispatchNextRider(ride._id.toString());

    res.json({ ride });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin Endpoints
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

  // Notify rider live if they are waiting on the page
  const sockId = connectedRiders.get(riderId);
  if (sockId) {
    io.to(sockId).emit('verification_status_changed', { status });
  }

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
  <title>Go²Go | Zero Commission Rides • ₹12/km</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
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

    /* Animated Mesh Glow */
    .hero-glow {
      position: fixed;
      width: 500px;
      height: 500px;
      background: radial-gradient(circle, rgba(16,185,129,0.18) 0%, rgba(59,130,246,0.08) 50%, transparent 70%);
      top: -100px;
      left: 50%;
      transform: translateX(-50%);
      pointer-events: none;
      z-index: 0;
    }

    /* 4D Interactive Perspective Buttons */
    .stage-4d {
      perspective: 1200px;
    }
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
    .btn-4d:hover::before {
      transform: translateX(100%);
    }
    .btn-4d-rider {
      background: linear-gradient(135deg, #10b981 0%, #059669 100%);
      box-shadow: 0 14px 0 #064e3b, 0 25px 40px rgba(16, 185, 129, 0.35);
    }
    .btn-4d-rider:active {
      transform: translateY(8px) scale(0.98) !important;
      box-shadow: 0 4px 0 #064e3b, 0 10px 20px rgba(16, 185, 129, 0.3);
    }
    .btn-4d-customer {
      background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
      box-shadow: 0 14px 0 #1e3a8a, 0 25px 40px rgba(59, 130, 246, 0.35);
    }
    .btn-4d-customer:active {
      transform: translateY(8px) scale(0.98) !important;
      box-shadow: 0 4px 0 #1e3a8a, 0 10px 20px rgba(59, 130, 246, 0.3);
    }
    .btn-4d-inner {
      transform: translateZ(35px);
    }

    /* Glassmorphism Cards */
    .glass {
      background: rgba(15, 23, 42, 0.78);
      backdrop-filter: blur(16px);
      border: 1px solid rgba(255, 255, 255, 0.1);
    }

    /* Map Container */
    #map {
      height: 360px;
      width: 100%;
      border-radius: 1rem;
      z-index: 10;
    }

    @keyframes pulseRing {
      0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
      70% { transform: scale(1); box-shadow: 0 0 0 20px rgba(16, 185, 129, 0); }
      100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
    .ring-pulse {
      animation: pulseRing 1.5s infinite;
    }
  </style>
</head>
<body class="relative">
  <div class="hero-glow"></div>

  <!-- Top Navigation Bar -->
  <nav class="relative z-20 max-w-6xl mx-auto px-4 py-4 flex justify-between items-center">
    <div onclick="showView('landing')" class="flex items-center gap-3 cursor-pointer">
      <div class="w-11 h-11 rounded-2xl bg-gradient-to-tr from-emerald-400 to-blue-500 flex items-center justify-center shadow-lg shadow-emerald-500/20">
        <svg class="w-6 h-6 text-slate-950" fill="none" stroke="currentColor" stroke-width="2.8" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z"/>
        </svg>
      </div>
      <span class="text-2xl font-black tracking-tight">Go<sup class="text-emerald-400 text-lg">2</sup>Go</span>
    </div>
    <div class="flex items-center gap-3">
      <span class="hidden sm:inline-block text-xs font-semibold px-3 py-1.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
        ₹12/km Flat • 0% Commission
      </span>
      <button onclick="showView('admin')" class="text-xs text-slate-400 hover:text-white px-3 py-1.5 rounded-lg border border-slate-800 hover:border-slate-600 transition">
        Admin Panel
      </button>
    </div>
  </nav>

  <!-- =========================================================
       VIEW 1: LANDING PAGE (BRANDING + 4D ANIMATED BUTTONS)
  ========================================================== -->
  <main id="view-landing" class="relative z-10 max-w-5xl mx-auto px-4 pt-8 pb-20 text-center">
    <!-- Custom Animated Logo Badge -->
    <div class="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-white/5 border border-white/10 text-sm text-emerald-300 font-semibold mb-6">
      <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
      Direct Peer-to-Peer Pickup & Drop Network
    </div>

    <!-- Big Exciting Branding -->
    <div class="flex flex-col items-center justify-center mb-6">
      <div class="relative mb-4">
        <div class="w-24 h-24 sm:w-28 sm:h-28 rounded-3xl bg-gradient-to-br from-emerald-400 via-teal-500 to-blue-600 p-1 shadow-2xl shadow-emerald-500/30 rotate-3 hover:rotate-0 transition duration-300">
          <div class="w-full h-full bg-slate-950 rounded-[20px] flex flex-col items-center justify-center">
            <span class="text-3xl sm:text-4xl font-black bg-gradient-to-r from-emerald-400 to-blue-400 bg-clip-text text-transparent">
              Go²Go
            </span>
            <span class="text-[10px] tracking-widest uppercase text-slate-400 font-bold mt-0.5">EXPRESS</span>
          </div>
        </div>
      </div>

      <h1 class="text-5xl sm:text-7xl font-black tracking-tight mb-4">
        Go<span class="text-emerald-400">²</span>Go
      </h1>

      <!-- Shortened Rewritten Title -->
      <p class="text-xl sm:text-3xl font-extrabold bg-gradient-to-r from-emerald-300 via-white to-blue-300 bg-clip-text text-transparent max-w-2xl">
        Zero Commission for Riders. Pocket-Friendly for You.
      </p>
      <p class="text-slate-400 mt-3 max-w-xl text-sm sm:text-base">
        Transparent <strong class="text-white">₹12/km</strong> distance pricing. 100% of the fare goes straight to the Rider with zero hidden charges.
      </p>
    </div>

    <!-- Two 4D Animated Role Selection Buttons -->
    <div class="stage-4d grid grid-cols-1 md:grid-cols-2 gap-8 max-w-3xl mx-auto mt-12 px-2">
      <!-- Button 1: I'm Rider -->
      <div onclick="showView('rider-auth')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d btn-4d-rider rounded-3xl p-8 text-left select-none">
        <div class="btn-4d-inner">
          <div class="w-14 h-14 rounded-2xl bg-white/20 flex items-center justify-center mb-5 backdrop-blur-sm">
            <svg class="w-8 h-8 text-white" fill="none" stroke="currentColor" stroke-width="2.2" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4"/>
            </svg>
          </div>
          <div class="text-xs uppercase tracking-widest font-bold text-emerald-100 mb-1">Keep 100% Earnings</div>
          <h2 class="text-3xl font-black text-white">1. I'm Rider</h2>
          <p class="text-emerald-100/90 text-sm mt-2">Register vehicle, get verified & receive direct ride rings in queue.</p>
        </div>
      </div>

      <!-- Button 2: I'm Customer -->
      <div onclick="showView('customer-auth')" onmousemove="tilt4D(event, this)" onmouseleave="reset4D(this)"
           class="btn-4d btn-4d-customer rounded-3xl p-8 text-left select-none">
        <div class="btn-4d-inner">
          <div class="w-14 h-14 rounded-2xl bg-white/20 flex items-center justify-center mb-5 backdrop-blur-sm">
            <svg class="w-8 h-8 text-white" fill="none" stroke="currentColor" stroke-width="2.2" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"/>
              <path stroke-linecap="round" stroke-linejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"/>
            </svg>
          </div>
          <div class="text-xs uppercase tracking-widest font-bold text-blue-100 mb-1">Flat ₹12/km • No Surges</div>
          <h2 class="text-3xl font-black text-white">2. I'm Customer</h2>
          <p class="text-blue-100/90 text-sm mt-2">Pin pickup & drop on live map and get matched with verified riders.</p>
        </div>
      </div>
    </div>
  </main>

  <!-- =========================================================
       VIEW 2: RIDER REGISTRATION & LOGIN
  ========================================================== -->
  <section id="view-rider-auth" class="hidden relative z-10 max-w-xl mx-auto px-4 py-8">
    <div class="glass rounded-3xl p-6 sm:p-8 shadow-2xl">
      <div class="flex justify-between items-center mb-6">
        <h2 class="text-2xl font-black text-emerald-400">Rider Portal</h2>
        <div class="flex bg-slate-900 p-1 rounded-xl border border-slate-800">
          <button id="tab-rider-reg" onclick="switchRiderTab('register')" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950">New Rider</button>
          <button id="tab-rider-log" onclick="switchRiderTab('login')" class="px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400">Login</button>
        </div>
      </div>

      <form id="rider-form" onsubmit="handleRiderSubmit(event)" class="space-y-4">
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
          Submit Request for Verification
        </button>
      </form>
    </div>
  </section>

  <!-- =========================================================
       VIEW 3: RIDER LIVE DASHBOARD & INCOMING RING ALERT
  ========================================================== -->
  <section id="view-rider-dash" class="hidden relative z-10 max-w-2xl mx-auto px-4 py-8">
    <div class="glass rounded-3xl p-6 sm:p-8 shadow-2xl">
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

      <!-- Pending / Rejected Message -->
      <div id="rd-pending-box" class="mt-6 bg-amber-500/10 border border-amber-500/30 rounded-2xl p-5 text-center">
        <h3 class="text-lg font-bold text-amber-300">Verification Request Submitted</h3>
        <p class="text-sm text-slate-300 mt-1">Admin has been notified to review your Aadhaar & Vehicle details. Once approved, you will automatically enter the First-Joined-First-Served ride queue.</p>
      </div>

      <!-- Verified Active Queue Panel -->
      <div id="rd-active-box" class="hidden mt-6 space-y-5">
        <div class="flex items-center justify-between bg-slate-900/90 p-4 rounded-2xl border border-slate-800">
          <div>
            <p class="text-xs text-slate-400 uppercase font-bold">Queue Sequence Priority</p>
            <p class="text-lg font-black text-emerald-400">Sequence #<span id="rd-seq">1</span> (First-Joined Priority)</p>
          </div>
          <button id="rd-online-btn" onclick="toggleRiderOnline()" class="px-4 py-2 rounded-xl font-bold text-xs bg-emerald-500 text-slate-950">
            ● ONLINE
          </button>
        </div>

        <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-6 text-center">
          <div class="w-12 h-12 rounded-full bg-emerald-500/20 text-emerald-400 flex items-center justify-center mx-auto mb-3 animate-pulse">
            📡
          </div>
          <p class="font-bold text-white">Listening for Customer Rides...</p>
          <p class="text-xs text-slate-400 mt-1">Keep this browser tab open. You will get a 20-second ring alert when it's your turn in the sequence.</p>
        </div>

        <!-- Accepted Ride Customer Details Card -->
        <div id="rd-accepted-card" class="hidden bg-emerald-950/60 border-2 border-emerald-400 rounded-2xl p-5 space-y-3">
          <div class="flex justify-between items-center">
            <span class="text-xs font-black uppercase tracking-wider text-emerald-400">Active Ride Confirmed</span>
            <span id="rd-ride-fare" class="text-xl font-black text-white">₹0</span>
          </div>
          <div class="bg-slate-950/70 p-4 rounded-xl space-y-1">
            <p class="text-xs text-slate-400">Customer Details:</p>
            <p class="text-lg font-black text-white" id="rd-cust-name">-</p>
            <a id="rd-cust-phone" href="#" class="inline-block text-emerald-400 font-bold text-base underline">📞 -</a>
          </div>
          <div class="text-xs space-y-1 text-slate-300">
            <p><strong class="text-emerald-400">Pickup:</strong> <span id="rd-cust-pickup"></span></p>
            <p><strong class="text-blue-400">Drop:</strong> <span id="rd-cust-drop"></span></p>
          </div>
          <a id="rd-gmaps-nav" target="_blank" href="#" class="block text-center w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-500 font-bold text-sm text-white">
            Open Turn-by-Turn Navigation
          </a>
        </div>
      </div>
    </div>
  </section>

  <!-- INCOMING 20-SECOND RING MODAL FOR RIDER -->
  <div id="ring-modal" class="hidden fixed inset-0 z-50 bg-slate-950/90 backdrop-blur-md flex items-center justify-center p-4">
    <div class="glass border-2 border-emerald-400 rounded-3xl max-w-md w-full p-6 text-center ring-pulse">
      <div class="text-xs font-black uppercase tracking-widest text-emerald-400 mb-1">New Ride Request!</div>
      <div class="text-4xl font-black text-white my-2" id="ring-fare">₹0</div>
      <div class="text-xs font-bold text-slate-300 mb-4" id="ring-dist">0 km • 100% Yours (0% Commission)</div>

      <div class="bg-slate-900/90 rounded-2xl p-4 text-left text-xs space-y-2 mb-5 border border-slate-800">
        <div>
          <span class="text-emerald-400 font-bold block">PICKUP LOCATION:</span>
          <span id="ring-pickup" class="text-white font-semibold"></span>
        </div>
        <div class="border-t border-slate-800 pt-2">
          <span class="text-blue-400 font-bold block">DROP LOCATION:</span>
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

  <!-- =========================================================
       VIEW 4: CUSTOMER REGISTRATION & LOGIN
  ========================================================== -->
  <section id="view-customer-auth" class="hidden relative z-10 max-w-md mx-auto px-4 py-8">
    <div class="glass rounded-3xl p-6 sm:p-8 shadow-2xl">
      <div class="flex justify-between items-center mb-6">
        <h2 class="text-2xl font-black text-blue-400">Customer Access</h2>
        <div class="flex bg-slate-900 p-1 rounded-xl border border-slate-800">
          <button id="tab-cust-reg" onclick="switchCustTab('register')" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-blue-500 text-white">Register</button>
          <button id="tab-cust-log" onclick="switchCustTab('login')" class="px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400">Login</button>
        </div>
      </div>

      <form onsubmit="handleCustomerSubmit(event)" class="space-y-4">
        <div id="cust-reg-name-box">
          <label class="block text-xs font-bold text-slate-400 uppercase mb-1">Full Name *</label>
          <input type="text" id="c-name" placeholder="Your full name" class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-white outline-none focus:border-blue-400" />
        </div>

        <div>
          <label class="block text-xs font-bold text-slate-400 uppercase mb-1">Mobile Number *</label>
          <input type="tel" id="c-mobile" maxlength="10" placeholder="10-digit mobile number" required class="w-full bg-slate-900/90 border border-slate-700 rounded-xl px-4 py-3 text-white outline-none focus:border-blue-400" />
        </div>

        <div id="cust-reg-aadhaar-box" class="bg-slate-900/70 p-3 rounded-xl border border-slate-800">
          <label class="block text-xs font-bold text-blue-400 mb-1">Upload Aadhaar Card *</label>
          <input type="file" id="c-aadhaar" accept="image/*" class="text-xs text-slate-400 file:mr-2 file:py-1.5 file:px-3 file:rounded-lg file:border-0 file:bg-blue-500/20 file:text-blue-300" />
        </div>

        <button type="submit" class="w-full py-4 rounded-2xl bg-gradient-to-r from-blue-500 to-indigo-600 font-black text-white text-lg shadow-lg">
          Continue to Map Booking
        </button>
      </form>
    </div>
  </section>

  <!-- =========================================================
       VIEW 5: CUSTOMER MAP BOOKING & LIVE RIDE TRACKER
  ========================================================== -->
  <section id="view-customer-book" class="hidden relative z-10 max-w-4xl mx-auto px-4 py-6">
    <div class="glass rounded-3xl p-5 sm:p-7 shadow-2xl space-y-5">
      <div class="flex flex-wrap justify-between items-center gap-2">
        <div>
          <h2 class="text-xl font-black text-white">Book Pickup & Drop</h2>
          <p class="text-xs text-slate-400">Welcome, <span id="cb-username" class="text-blue-400 font-bold"></span> • Flat ₹12/km (Zero Hidden Fees)</p>
        </div>
        <button onclick="useCurrentGPS()" class="px-3 py-2 rounded-xl bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 text-xs font-bold hover:bg-emerald-500/30">
          📍 Use My Current GPS
        </button>
      </div>

      <!-- Location Search & Pin Controls -->
      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
        <div class="bg-slate-900/90 p-3 rounded-2xl border border-slate-800">
          <label class="text-[11px] font-bold text-emerald-400 uppercase">1. Pickup Location</label>
          <div class="flex gap-2 mt-1">
            <input type="text" id="pickup-input" placeholder="Search pickup or click map..." class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-xs text-white outline-none" />
            <button onclick="searchLocation('pickup')" class="px-3 py-2 bg-emerald-600 rounded-xl text-xs font-bold">Find</button>
          </div>
          <p id="pickup-label" class="text-[11px] text-slate-400 mt-1 truncate">Click on map or search to set pickup</p>
        </div>

        <div class="bg-slate-900/90 p-3 rounded-2xl border border-slate-800">
          <label class="text-[11px] font-bold text-blue-400 uppercase">2. Drop Location</label>
          <div class="flex gap-2 mt-1">
            <input type="text" id="drop-input" placeholder="Search drop address..." class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-xs text-white outline-none" />
            <button onclick="searchLocation('drop')" class="px-3 py-2 bg-blue-600 rounded-xl text-xs font-bold">Find</button>
          </div>
          <p id="drop-label" class="text-[11px] text-slate-400 mt-1 truncate">Search or drag blue marker on map</p>
        </div>
      </div>

      <!-- Interactive OpenStreetMap -->
      <div class="relative">
        <div id="map"></div>
        <div class="mt-2 text-[11px] text-slate-400 flex justify-between">
          <span>Tip: Drag the Green (Pickup) or Blue (Drop) markers directly on the road for pinpoint accuracy.</span>
          <span id="route-status" class="text-emerald-400 font-semibold"></span>
        </div>
      </div>

      <!-- Fare & Confirm Ride Bar -->
      <div id="booking-controls" class="bg-slate-900/95 border border-slate-800 rounded-2xl p-4 flex flex-col sm:flex-row items-center justify-between gap-4">
        <div class="flex items-center gap-6">
          <div>
            <span class="text-xs text-slate-400 block">Road Distance</span>
            <span id="calc-dist" class="text-xl font-black text-white">0.0 km</span>
          </div>
          <div class="h-8 w-px bg-slate-800"></div>
          <div>
            <span class="text-xs text-slate-400 block">Total Fare (₹12/km)</span>
            <span id="calc-fare" class="text-2xl font-black text-emerald-400">₹0</span>
          </div>
        </div>

        <button onclick="confirmRideBooking()" id="confirm-ride-btn" disabled class="w-full sm:w-auto px-8 py-4 rounded-2xl bg-gradient-to-r from-emerald-500 to-blue-600 disabled:opacity-40 font-black text-white text-base shadow-xl transition">
          Confirm the Ride
        </button>
      </div>

      <!-- Live Ride Status Box (Shown after clicking Confirm the Ride) -->
      <div id="live-ride-box" class="hidden bg-slate-900 border-2 border-blue-500/50 rounded-2xl p-5 space-y-4">
        <div id="searching-state" class="flex items-center justify-between">
          <div class="flex items-center gap-3">
            <div class="w-4 h-4 rounded-full bg-blue-500 animate-ping"></div>
            <div>
              <h3 class="font-black text-white text-base">Finding Your Rider...</h3>
              <p id="live-dispatch-msg" class="text-xs text-blue-300">Notifying verified riders in queue sequence...</p>
            </div>
          </div>
          <button onclick="cancelActiveRide()" class="px-3 py-1.5 rounded-lg bg-red-500/20 text-red-400 text-xs font-bold border border-red-500/30">
            Cancel
          </button>
        </div>

        <!-- Rider Accepted State -->
        <div id="accepted-state" class="hidden space-y-3">
          <div class="flex items-center justify-between border-b border-slate-800 pb-3">
            <span class="px-3 py-1 rounded-full bg-emerald-500/20 text-emerald-400 text-xs font-black">✓ RIDER ACCEPTED YOUR RIDE!</span>
            <span id="acc-fare" class="text-lg font-black text-white">₹0</span>
          </div>
          <div class="flex items-center gap-4">
            <img id="acc-rider-img" src="https://via.placeholder.com/60/3b82f6/ffffff?text=R" class="w-14 h-14 rounded-2xl object-cover border border-emerald-400" />
            <div>
              <h4 id="acc-rider-name" class="text-lg font-black text-white">Rider Name</h4>
              <p id="acc-rider-veh" class="text-xs text-slate-300">Vehicle</p>
              <a id="acc-rider-phone" href="#" class="text-sm font-bold text-emerald-400 underline mt-1 inline-block">📞 Call Rider</a>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <!-- =========================================================
       VIEW 6: ADMIN VERIFICATION PANEL
  ========================================================== -->
  <section id="view-admin" class="hidden relative z-10 max-w-5xl mx-auto px-4 py-8">
    <div class="glass rounded-3xl p-6 sm:p-8">
      <div class="flex flex-wrap justify-between items-center gap-4 mb-6">
        <div>
          <h2 class="text-2xl font-black text-white">Admin Verification & Queue Monitor</h2>
          <p class="text-xs text-slate-400">Approve riders to activate them in the sequential dispatch loop</p>
        </div>
        <div class="flex gap-2">
          <input type="password" id="admin-pass-input" placeholder="Admin Password (admin123)" class="bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs text-white" />
          <button onclick="loadAdminData()" class="px-4 py-2 rounded-xl bg-emerald-500 text-slate-950 font-bold text-xs">Unlock / Refresh</button>
        </div>
      </div>

      <div id="admin-riders-list" class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <p class="text-sm text-slate-400">Enter admin password and click Unlock to view pending rider requests.</p>
      </div>
    </div>
  </section>

  <script>
    const socket = io();
    let currentRider = null;
    let currentCustomer = null;
    let activeRideId = null;
    let incomingRingRideId = null;
    let ringCountdownInterval = null;
    let riderAuthMode = 'register';
    let custAuthMode = 'register';

    // Map variables
    let map, pickupMarker, dropMarker, routeLine;
    let pickupCoords = null;
    let dropCoords = null;
    let currentDistanceKm = 0;
    let currentFare = 0;

    // ==========================================
    // 4D TILT INTERACTION
    // ==========================================
    function tilt4D(e, el) {
      const rect = el.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      const centerX = rect.width / 2;
      const centerY = rect.height / 2;
      const rotateX = ((y - centerY) / centerY) * -12;
      const rotateY = ((x - centerX) / centerX) * 12;
      el.style.transform = \`rotateX(\${rotateX}deg) rotateY(\${rotateY}deg) scale(1.03)\`;
    }
    function reset4D(el) {
      el.style.transform = 'rotateX(0deg) rotateY(0deg) scale(1)';
    }

    // ==========================================
    // VIEW NAVIGATION
    // ==========================================
    function showView(viewName) {
      ['landing', 'rider-auth', 'rider-dash', 'customer-auth', 'customer-book', 'admin'].forEach(v => {
        document.getElementById('view-' + v).classList.add('hidden');
      });
      document.getElementById('view-' + viewName).classList.remove('hidden');

      if (viewName === 'customer-book') {
        setTimeout(initMap, 150);
      }
    }

    // Helper: Convert File to Compressed Base64
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

    // Web Audio API Ringtone Generator for Riders
    function playBrowserRing() {
      try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = 'triangle';
        osc.frequency.setValueAtTime(660, ctx.currentTime);
        osc.frequency.setValueAtTime(880, ctx.currentTime + 0.2);
        gain.gain.setValueAtTime(0.3, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.6);
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.start();
        osc.stop(ctx.currentTime + 0.6);
      } catch (e) {}
    }

    // ==========================================
    // RIDER AUTH & DASHBOARD
    // ==========================================
    function switchRiderTab(mode) {
      riderAuthMode = mode;
      const isReg = mode === 'register';
      document.getElementById('rider-reg-fields').classList.toggle('hidden', !isReg);
      document.getElementById('rider-vehicle-fields').classList.toggle('hidden', !isReg);
      document.getElementById('rider-submit-btn').innerText = isReg ? 'Submit Request for Verification' : 'Login to Rider Dashboard';
      document.getElementById('tab-rider-reg').className = isReg ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950' : 'px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400';
      document.getElementById('tab-rider-log').className = !isReg ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950' : 'px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400';
    }

    async function handleRiderSubmit(e) {
      e.preventDefault();
      const btn = document.getElementById('rider-submit-btn');
      btn.disabled = true;
      btn.innerText = 'Processing...';

      try {
        const payload = {
          mode: riderAuthMode,
          mobile: document.getElementById('r-mobile').value.trim()
        };

        if (riderAuthMode === 'register') {
          payload.name = document.getElementById('r-name').value.trim();
          payload.vehicleType = document.getElementById('r-vtype').value;
          payload.vehicleNumber = document.getElementById('r-vnum').value.trim();
          payload.vehicleModel = document.getElementById('r-vmodel').value.trim();
          payload.aadhaarImage = await fileToBase64(document.getElementById('r-aadhaar').files[0]);
          payload.vehicleImage = await fileToBase64(document.getElementById('r-vimg').files[0]);
          payload.profileImage = await fileToBase64(document.getElementById('r-profile').files[0]);
        }

        const res = await fetch('/api/rider/auth', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error);

        currentRider = data.rider;
        renderRiderDashboard(data.queuePosition);
        showView('rider-dash');
      } catch (err) {
        alert(err.message);
      } finally {
        btn.disabled = false;
        btn.innerText = riderAuthMode === 'register' ? 'Submit Request for Verification' : 'Login to Rider Dashboard';
      }
    }

    function renderRiderDashboard(queuePos) {
      document.getElementById('rd-name').innerText = currentRider.name;
      document.getElementById('rd-vehicle').innerText = \`\${currentRider.vehicleType} • \${currentRider.vehicleModel} (\${currentRider.vehicleNumber})\`;
      if (currentRider.profileImage) {
        document.getElementById('rd-avatar').src = currentRider.profileImage;
      }

      socket.emit('register_rider_socket', currentRider._id);

      const badge = document.getElementById('rd-badge');
      if (currentRider.status === 'approved') {
        badge.innerText = '✓ Verified Rider';
        badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30';
        document.getElementById('rd-pending-box').classList.add('hidden');
        document.getElementById('rd-active-box').classList.remove('hidden');
        document.getElementById('rd-seq').innerText = queuePos || 1;
      } else if (currentRider.status === 'rejected') {
        badge.innerText = '✕ Rejected';
        badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-red-500/20 text-red-300 border border-red-500/30';
        document.getElementById('rd-pending-box').innerHTML = '<h3 class="text-red-400 font-bold">Verification Rejected by Admin</h3><p class="text-xs text-slate-300 mt-1">Please contact support with valid documents.</p>';
      }
    }

    async function toggleRiderOnline() {
      const nextState = !currentRider.isOnline;
      const res = await fetch('/api/rider/toggle-online', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ riderId: currentRider._id, isOnline: nextState })
      });
      const data = await res.json();
      currentRider = data.rider;
      const btn = document.getElementById('rd-online-btn');
      btn.innerText = currentRider.isOnline ? '● ONLINE' : '○ OFFLINE';
      btn.className = currentRider.isOnline
        ? 'px-4 py-2 rounded-xl font-bold text-xs bg-emerald-500 text-slate-950'
        : 'px-4 py-2 rounded-xl font-bold text-xs bg-slate-700 text-slate-300';
    }

    // Socket events for Rider
    socket.on('verification_status_changed', ({ status }) => {
      if (currentRider) {
        currentRider.status = status;
        renderRiderDashboard(1);
      }
    });

    socket.on('incoming_ride_ring', (ride) => {
      incomingRingRideId = ride.rideId;
      document.getElementById('ring-fare').innerText = '₹' + ride.fare;
      document.getElementById('ring-dist').innerText = \`\${ride.distanceKm} km • 100% Yours (0% Commission)\`;
      document.getElementById('ring-pickup').innerText = ride.pickupAddress;
      document.getElementById('ring-drop').innerText = ride.dropAddress;
      document.getElementById('ring-modal').classList.remove('hidden');

      let sec = ride.timeoutSec || 20;
      document.getElementById('ring-timer').innerText = sec;
      playBrowserRing();

      clearInterval(ringCountdownInterval);
      ringCountdownInterval = setInterval(() => {
        sec--;
        document.getElementById('ring-timer').innerText = Math.max(0, sec);
        if (sec % 3 === 0) playBrowserRing();
        if (sec <= 0) {
          clearInterval(ringCountdownInterval);
          document.getElementById('ring-modal').classList.add('hidden');
        }
      }, 1000);
    });

    socket.on('ring_timeout', ({ rideId }) => {
      if (incomingRingRideId === rideId) {
        clearInterval(ringCountdownInterval);
        document.getElementById('ring-modal').classList.add('hidden');
        incomingRingRideId = null;
      }
    });

    function acceptIncomingRide() {
      if (!incomingRingRideId || !currentRider) return;
      clearInterval(ringCountdownInterval);
      document.getElementById('ring-modal').classList.add('hidden');
      socket.emit('accept_ride', { rideId: incomingRingRideId, riderId: currentRider._id });
    }

    socket.on('ride_confirmed_for_rider', (details) => {
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
    });

    socket.on('accept_failed', ({ message }) => alert(message));

    // ==========================================
    // CUSTOMER AUTH & MAP NAVIGATION
    // ==========================================
    function switchCustTab(mode) {
      custAuthMode = mode;
      const isReg = mode === 'register';
      document.getElementById('cust-reg-name-box').classList.toggle('hidden', !isReg);
      document.getElementById('cust-reg-aadhaar-box').classList.toggle('hidden', !isReg);
      document.getElementById('tab-cust-reg').className = isReg ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-blue-500 text-white' : 'px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400';
      document.getElementById('tab-cust-log').className = !isReg ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-blue-500 text-white' : 'px-3 py-1.5 rounded-lg text-xs font-bold text-slate-400';
    }

    async function handleCustomerSubmit(e) {
      e.preventDefault();
      try {
        const payload = {
          mode: custAuthMode,
          mobile: document.getElementById('c-mobile').value.trim()
        };
        if (custAuthMode === 'register') {
          payload.name = document.getElementById('c-name').value.trim();
          payload.aadhaarImage = await fileToBase64(document.getElementById('c-aadhaar').files[0]);
        }

        const res = await fetch('/api/customer/auth', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error);

        currentCustomer = data.customer;
        document.getElementById('cb-username').innerText = currentCustomer.name;
        showView('customer-book');
      } catch (err) {
        alert(err.message);
      }
    }

    function initMap() {
      if (map) {
        map.invalidateSize();
        return;
      }
      // Default center: Lucknow (26.8467, 80.9462)
      const defaultCenter = [26.8467, 80.9462];
      map = L.map('map').setView(defaultCenter, 13);

      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap'
      }).addTo(map);

      const greenIcon = new L.Icon({
        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-green.png',
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
        iconSize: [25, 41], iconAnchor: [12, 41]
      });

      const blueIcon = new L.Icon({
        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png',
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
        iconSize: [25, 41], iconAnchor: [12, 41]
      });

      pickupMarker = L.marker(defaultCenter, { draggable: true, icon: greenIcon }).addTo(map).bindPopup('Pickup Location');
      dropMarker = L.marker([26.8567, 80.9562], { draggable: true, icon: blueIcon }).addTo(map).bindPopup('Drop Location');

      pickupMarker.on('dragend', () => {
        const pos = pickupMarker.getLatLng();
        setPoint('pickup', pos.lat, pos.lng);
      });

      dropMarker.on('dragend', () => {
        const pos = dropMarker.getLatLng();
        setPoint('drop', pos.lat, pos.lng);
      });

      map.on('click', (e) => {
        if (!pickupCoords) {
          setPoint('pickup', e.latlng.lat, e.latlng.lng);
        } else {
          setPoint('drop', e.latlng.lat, e.latlng.lng);
        }
      });

      useCurrentGPS();
    }

    function useCurrentGPS() {
      if (!navigator.geolocation) return;
      navigator.geolocation.getCurrentPosition((pos) => {
        const { latitude, longitude } = pos.coords;
        map.setView([latitude, longitude], 15);
        setPoint('pickup', latitude, longitude);
      });
    }

    async function searchLocation(type) {
      const query = document.getElementById(type + '-input').value.trim();
      if (!query) return;
      const res = await fetch(\`https://nominatim.openstreetmap.org/search?format=json&q=\${encodeURIComponent(query)}&limit=1\`);
      const results = await res.json();
      if (results.length > 0) {
        const lat = parseFloat(results[0].lat);
        const lng = parseFloat(results[0].lon);
        setPoint(type, lat, lng, results[0].display_name);
        map.setView([lat, lng], 14);
      } else {
        alert('Location not found. Try adding city name or drag pin on map.');
      }
    }

    async function setPoint(type, lat, lng, knownAddress = null) {
      let address = knownAddress;
      if (!address) {
        try {
          const rev = await fetch(\`https://nominatim.openstreetmap.org/reverse?format=json&lat=\${lat}&lon=\${lng}\`);
          const revData = await rev.json();
          address = revData.display_name || \`\${lat.toFixed(5)}, \${lng.toFixed(5)}\`;
        } catch {
          address = \`\${lat.toFixed(5)}, \${lng.toFixed(5)}\`;
        }
      }

      if (type === 'pickup') {
        pickupCoords = { lat, lng };
        pickupMarker.setLatLng([lat, lng]);
        document.getElementById('pickup-label').innerText = address;
        document.getElementById('pickup-input').value = address;
      } else {
        dropCoords = { lat, lng };
        dropMarker.setLatLng([lat, lng]);
        document.getElementById('drop-label').innerText = address;
        document.getElementById('drop-input').value = address;
      }

      if (pickupCoords && dropCoords) {
        calculateOSRMRoute();
      }
    }

    async function calculateOSRMRoute() {
      document.getElementById('route-status').innerText = 'Calculating exact road route...';
      try {
        const url = \`https://router.project-osrm.org/route/v1/driving/\${pickupCoords.lng},\${pickupCoords.lat};\${dropCoords.lng},\${dropCoords.lat}?overview=full&geometries=geojson\`;
        const res = await fetch(url);
        const data = await res.json();

        if (data.routes && data.routes.length > 0) {
          const route = data.routes[0];
          currentDistanceKm = parseFloat((route.distance / 1000).toFixed(2));
          currentFare = Math.max(12, Math.round(currentDistanceKm * 12)); // ₹12/km

          if (routeLine) map.removeLayer(routeLine);
          const latLngs = route.geometry.coordinates.map(c => [c[1], c[0]]);
          routeLine = L.polyline(latLngs, { color: '#10b981', weight: 5 }).addTo(map);
          map.fitBounds(routeLine.getBounds(), { padding: [40, 40] });

          document.getElementById('calc-dist').innerText = currentDistanceKm + ' km';
          document.getElementById('calc-fare').innerText = '₹' + currentFare;
          document.getElementById('confirm-ride-btn').disabled = false;
          document.getElementById('route-status').innerText = 'Route Locked ✓';
        }
      } catch (err) {
        document.getElementById('route-status').innerText = 'Error calculating route';
      }
    }

    async function confirmRideBooking() {
      if (!currentCustomer || !pickupCoords || !dropCoords) return;

      const btn = document.getElementById('confirm-ride-btn');
      btn.disabled = true;
      btn.innerText = 'Requesting...';

      const res = await fetch('/api/rides/book', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerName: currentCustomer.name,
          customerMobile: currentCustomer.mobile,
          pickupAddress: document.getElementById('pickup-label').innerText,
          pickupCoords,
          dropAddress: document.getElementById('drop-label').innerText,
          dropCoords,
          distanceKm: currentDistanceKm,
          fare: currentFare
        })
      });

      const data = await res.json();
      if (res.ok) {
        activeRideId = data.ride._id;
        socket.emit('join_ride_room', activeRideId);
        document.getElementById('booking-controls').classList.add('hidden');
        document.getElementById('live-ride-box').classList.remove('hidden');
      } else {
        alert(data.error);
        btn.disabled = false;
        btn.innerText = 'Confirm the Ride';
      }
    }

    socket.on('ride_status_update', ({ message }) => {
      document.getElementById('live-dispatch-msg').innerText = message;
    });

    socket.on('ride_accepted_by_rider', (info) => {
      document.getElementById('searching-state').classList.add('hidden');
      document.getElementById('accepted-state').classList.remove('hidden');
      document.getElementById('acc-fare').innerText = '₹' + info.fare;
      document.getElementById('acc-rider-name').innerText = info.riderName;
      document.getElementById('acc-rider-veh').innerText = info.riderVehicle;
      if (info.riderPhoto) document.getElementById('acc-rider-img').src = info.riderPhoto;
      const phone = document.getElementById('acc-rider-phone');
      phone.innerText = '📞 ' + info.riderMobile;
      phone.href = 'tel:' + info.riderMobile;
    });

    function cancelActiveRide() {
      if (activeRideId) {
        socket.emit('cancel_ride', activeRideId);
        activeRideId = null;
      }
      document.getElementById('live-ride-box').classList.add('hidden');
      document.getElementById('booking-controls').classList.remove('hidden');
      document.getElementById('confirm-ride-btn').disabled = false;
      document.getElementById('confirm-ride-btn').innerText = 'Confirm the Ride';
    }

    // ==========================================
    // ADMIN PANEL LOGIC
    // ==========================================
    async function loadAdminData() {
      const password = document.getElementById('admin-pass-input').value;
      socket.emit('join_admin_room', password);

      const res = await fetch('/api/admin/riders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password })
      });
      const data = await res.json();
      if (!res.ok) return alert(data.error);

      const container = document.getElementById('admin-riders-list');
      if (data.riders.length === 0) {
        container.innerHTML = '<p class="text-slate-400 text-sm">No riders registered yet.</p>';
        return;
      }

      container.innerHTML = data.riders.map((r, idx) => \`
        <div class="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 space-y-3">
          <div class="flex justify-between items-start">
            <div>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-slate-800 text-slate-300">Queue #\${idx + 1}</span>
              <h3 class="text-lg font-black text-white mt-1">\${r.name}</h3>
              <p class="text-xs text-slate-400">📞 \${r.mobile} • \${r.vehicleType} (\${r.vehicleNumber})</p>
              <p class="text-xs text-slate-400">Model: \${r.vehicleModel}</p>
            </div>
            <span class="text-xs font-bold px-2.5 py-1 rounded-full \${r.status === 'approved' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-amber-500/20 text-amber-300'}">
              \${r.status.toUpperCase()}
            </span>
          </div>
          <div class="grid grid-cols-2 gap-2">
            <div>
              <p class="text-[10px] text-slate-400 mb-1">Aadhaar Card</p>
              <img src="\${r.aadhaarImage}" class="w-full h-24 object-cover rounded-lg border border-slate-700" />
            </div>
            <div>
              <p class="text-[10px] text-slate-400 mb-1">Vehicle Image</p>
              <img src="\${r.vehicleImage}" class="w-full h-24 object-cover rounded-lg border border-slate-700" />
            </div>
          </div>
          <div class="flex gap-2 pt-2">
            <button onclick="verifyRider('\${r._id}', 'approved')" class="flex-1 py-2 rounded-xl bg-emerald-500 text-slate-950 font-bold text-xs">Approve Rider</button>
            <button onclick="verifyRider('\${r._id}', 'rejected')" class="flex-1 py-2 rounded-xl bg-red-500/20 text-red-400 border border-red-500/30 font-bold text-xs">Reject</button>
          </div>
        </div>
      \`).join('');
    }

    async function verifyRider(riderId, status) {
      const password = document.getElementById('admin-pass-input').value;
      await fetch('/api/admin/verify-rider', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password, riderId, status })
      });
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

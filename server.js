const mqtt = require("mqtt");
const { initializeApp } = require("firebase/app");
const {
  getFirestore,
  collection,
  addDoc,
  serverTimestamp,
} = require("firebase/firestore");
require("dotenv").config();

// Firebase configuration
const firebaseConfig = {
  apiKey: process.env.FIREBASE_API_KEY,
  authDomain: process.env.FIREBASE_AUTH_DOMAIN,
  projectId: process.env.FIREBASE_PROJECT_ID,
  storageBucket: process.env.FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.FIREBASE_APP_ID,
};

// Initialize Firebase
const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);

// Store the latest data
let latestData = null;
let lastSaveTime = 0;

// MQTT client setup
console.log("Connecting to MQTT broker...");
const client = mqtt.connect(
  process.env.MQTT_BROKER_URL || "mqtt://test.mosquitto.org:1883"
);

// Connection event handler
client.on("connect", () => {
  console.log("Successfully connected to MQTT broker");

  // Subscribe to the topic your M5Stack is publishing to
  client.subscribe("Braude/project/R&K");

  console.log(
    'Server is now running and listening for MQTT messages on topic "Braude/project/R&K"...'
  );
});

// Function to validate data with safety checks
function isValidData(data) {
  const requiredFields = [
    "temperature",
    "Light",
    "Soil humidity",
    "humidity",
    "Pressure",
  ];

  for (const field of requiredFields) {
    if (!(field in data)) {
      console.log(`Error: Missing field "${field}" in received data.`);
      return false;
    }
    if (data[field] === null || data[field] === undefined) {
      console.log(`Error: Field "${field}" is null or undefined.`);
      return false;
    }
  }

  return true;
}

// Function to save data to Firestore
async function saveToFirestore(data) {
  try {
    if (!isValidData(data)) {
      console.log("⚠️ Invalid data. Skipping Firestore save.");
      return;
    }

    if (data.temperature > 50) {
      console.log(
        `⚠️ Temperature too high (${data.temperature}°C). Skipping Firestore save.`
      );
      return;
    }

    const dataWithTimestamp = {
      ...data,
      timestamp: serverTimestamp(),
      receivedAt: new Date().toISOString(),
    };

    const docRef = await addDoc(
      collection(db, "sensorData"),
      dataWithTimestamp
    );
    console.log(`✅ Data saved to Firestore with ID: ${docRef.id}`);

    console.log("✅ Sensor Data:");
    console.log(`• Temperature: ${data.temperature} °C`);
    console.log(`• Light: ${data.Light} lux`);
    console.log(`• Soil Humidity: ${data["Soil humidity"]}`);
    console.log(`• Air Humidity: ${data.humidity}%`);
    console.log(`• Air Pressure: ${data.Pressure} hPa`);

    if (data.temperature > 30) {
      console.log(`⚠️ HIGH TEMPERATURE ALERT: ${data.temperature}°C`);
    }

    lastSaveTime = Date.now();
  } catch (error) {
    console.error("❌ Error saving to Firestore:", error);
  }
}

// Message event handler
client.on("message", async (topic, message) => {
  const messageStr = message.toString();
  console.log(
    `[${new Date().toISOString()}] Received message on topic ${topic}: ${messageStr}`
  );

  try {
    const data = JSON.parse(messageStr);

    if (typeof data === "object" && data !== null) {
      console.log(`Received data keys: ${Object.keys(data)}`);
      latestData = data;

      const now = Date.now();
      if (now - lastSaveTime >= 60000) {
        await saveToFirestore(data);
      } else {
        console.log(
          `Waiting to save... Next save in ${Math.round(
            (60000 - (now - lastSaveTime)) / 1000
          )} seconds.`
        );
      }
    } else {
      console.log("⚠️ Received non-object data.");
    }
  } catch (error) {
    console.error("❌ Error processing MQTT message:", error);
  }
});

// Auto-save every 60 seconds
setInterval(async () => {
  const now = Date.now();
  if (latestData && now - lastSaveTime >= 60000) {
    console.log("⏳ 1 minute passed, saving data...");
    try {
      await saveToFirestore(latestData);
    } catch (error) {
      console.error("❌ Error in scheduled save:", error);
    }
  }
}, 1000);

// Error event handlers
client.on("error", (err) => {
  console.error("❌ MQTT error:", err);
});

client.on("reconnect", () => {
  console.log("🔄 Reconnecting to MQTT broker...");
});

client.on("offline", () => {
  console.log("📴 MQTT client is offline.");
});

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("🚦 Shutting down gracefully...");
  client.end();
  process.exit();
});

process.stdin.resume();
console.log("🚀 M5Stack MQTT to Firebase server started.");
